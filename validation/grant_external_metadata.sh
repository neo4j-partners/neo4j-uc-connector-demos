#!/usr/bin/env bash
# Grant CREATE EXTERNAL METADATA privilege on the metastore.
#
# This intentionally runs SQL on the workspace cluster instead of the
# account-level host. The CREATE_EXTERNAL_METADATA grant is metastore-scoped,
# but the Unity Catalog permissions API used for this grant is served from the
# workspace host; accounts.azuredatabricks.net does not expose that endpoint for
# this workflow. A cluster job runs with the submitting user's workspace
# identity, which is the path that successfully issued the grant in
# docs/metadata_synchronization.md.
#
# Usage:
#   ./grant_external_metadata.sh                    # grant to current user
#   ./grant_external_metadata.sh user@example.com   # grant to explicit principal

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load the repo-root .env for the workspace profile, cluster ID, and upload directory.
set -a
source "$SCRIPT_DIR/../.env"
set +a

PROFILE_FLAG=()
if [[ -n "${DATABRICKS_PROFILE:-}" ]]; then
    PROFILE_FLAG=(--profile "$DATABRICKS_PROFILE")
fi

if [[ -z "${DATABRICKS_CLUSTER_ID:-}" ]]; then
    echo "ERROR: DATABRICKS_CLUSTER_ID must be set in ../.env for the grant job." >&2
    exit 1
fi

if [[ -z "${DATABRICKS_WORKSPACE_DIR:-}" ]]; then
    echo "ERROR: DATABRICKS_WORKSPACE_DIR must be set in ../.env." >&2
    exit 1
fi

PRINCIPAL="${1:-}"

if [[ -z "$PRINCIPAL" ]]; then
    echo "--- Discovering current workspace user ---"
    PRINCIPAL=$(databricks api get /api/2.0/preview/scim/v2/Me \
        -o json "${PROFILE_FLAG[@]}" 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(d.get('userName', d.get('emails', [{}])[0].get('value', 'unknown')))
")
fi

echo "  Principal: $PRINCIPAL"
echo "  Cluster: $DATABRICKS_CLUSTER_ID"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
GRANT_FILE="$TMP_DIR/grant_external_metadata.py"
REMOTE_DIR="${DATABRICKS_WORKSPACE_DIR%/}/admin"
REMOTE_FILE="$REMOTE_DIR/grant_external_metadata.py"

cat > "$GRANT_FILE" <<'PY'
"""One-shot cluster task for granting External Metadata registration rights."""

import sys

from pyspark.sql import SparkSession

principal = sys.argv[1]
escaped_principal = principal.replace("`", "``")

spark = SparkSession.builder.getOrCreate()

# The privilege is required by POST /api/2.0/lineage-tracking/external-metadata.
# Keeping the grant in SQL preserves the same authorization path as a notebook
# run by a metastore admin on this workspace.
spark.sql(
    f"GRANT CREATE_EXTERNAL_METADATA ON METASTORE TO `{escaped_principal}`"
)
print(f"Granted CREATE_EXTERNAL_METADATA on metastore to {principal}")
PY

echo ""
echo "--- Uploading grant task ---"
databricks workspace mkdirs "$REMOTE_DIR" "${PROFILE_FLAG[@]}"
# A previous version uploaded this generated helper as a notebook. Delete only
# this known helper path first so the replacement can be uploaded as a FILE.
databricks workspace delete "$REMOTE_FILE" "${PROFILE_FLAG[@]}" 2>/dev/null || true
databricks workspace import "$REMOTE_FILE" \
    --file "$GRANT_FILE" \
    --format AUTO \
    --overwrite \
    "${PROFILE_FLAG[@]}"

JOB_JSON=$(REMOTE_FILE="$REMOTE_FILE" PRINCIPAL="$PRINCIPAL" python3 -c '
import json
import os

print(json.dumps({
    "run_name": "grant_create_external_metadata",
    "tasks": [{
        "task_key": "grant",
        "spark_python_task": {
            "python_file": os.environ["REMOTE_FILE"],
            "parameters": [os.environ["PRINCIPAL"]],
        },
        "existing_cluster_id": os.environ["DATABRICKS_CLUSTER_ID"],
    }],
}))
')

echo ""
echo "--- Submitting grant job ---"
SUBMIT_RESPONSE=$(databricks jobs submit --json "$JOB_JSON" --no-wait -o json "${PROFILE_FLAG[@]}")
RUN_ID=$(printf "%s" "$SUBMIT_RESPONSE" | python3 -c '
import json
import sys

print(json.load(sys.stdin)["run_id"])
')
echo "  Run ID: $RUN_ID"

echo ""
echo "--- Waiting for grant job ---"
for _ in {1..80}; do
    RUN_JSON=$(databricks api get "/api/2.1/jobs/runs/get?run_id=$RUN_ID" \
        -o json "${PROFILE_FLAG[@]}")
    LIFE_CYCLE=$(printf "%s" "$RUN_JSON" | python3 -c '
import json
import sys

state = json.load(sys.stdin).get("state", {})
print(state.get("life_cycle_state", "UNKNOWN"))
')
    RESULT=$(printf "%s" "$RUN_JSON" | python3 -c '
import json
import sys

state = json.load(sys.stdin).get("state", {})
print(state.get("result_state", ""))
')
    echo "  $LIFE_CYCLE ${RESULT:-}"

    if [[ "$LIFE_CYCLE" == "TERMINATED" ]]; then
        if [[ "$RESULT" == "SUCCESS" ]]; then
            echo ""
            echo "Done. $PRINCIPAL can create External Metadata entries."
            exit 0
        fi
        echo "ERROR: grant job finished with result_state=$RESULT" >&2
        exit 1
    fi
    if [[ "$LIFE_CYCLE" == "SKIPPED" || "$LIFE_CYCLE" == "INTERNAL_ERROR" ]]; then
        echo "ERROR: grant job ended with life_cycle_state=$LIFE_CYCLE" >&2
        exit 1
    fi
    sleep 15
done

echo "ERROR: timed out waiting for grant job $RUN_ID" >&2
exit 1
