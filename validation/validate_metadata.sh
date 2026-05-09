#!/usr/bin/env bash
# Run the metadata-only validation suite from validation/.env.
#
# The default path is intentionally end-to-end: load .env, create/update the
# Databricks secret scope, grant External Metadata registration rights, upload
# the scripts, then submit both metadata validations.
#
# Usage:
#   ./validate_metadata.sh
#   ./validate_metadata.sh --skip-secrets --skip-grant --skip-upload
#   ./validate_metadata.sh --compute serverless

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$SCRIPT_DIR/.uv-cache}"

if [[ ! -f .env ]]; then
    echo "ERROR: .env not found. Copy .env.sample to .env and fill in values." >&2
    exit 1
fi

# Source .env here so the orchestration decisions below use the same values
# that databricks-job-runner passes to the remote Spark jobs.
set -a
source .env
set +a

SKIP_SECRETS=""
SKIP_GRANT=""
SKIP_UPLOAD=""
SUBMIT_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-secrets)
            SKIP_SECRETS="true"
            shift
            ;;
        --skip-grant)
            SKIP_GRANT="true"
            shift
            ;;
        --skip-upload)
            SKIP_UPLOAD="true"
            shift
            ;;
        --compute)
            if [[ -z "${2:-}" ]]; then
                echo "ERROR: --compute requires cluster or serverless" >&2
                exit 1
            fi
            SUBMIT_ARGS+=(--compute "$2")
            shift 2
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

echo "============================================"
echo "validation: Metadata Validation"
echo "============================================"
echo ""

echo "--- Step 1: Local runner and syntax checks ---"
uv sync --locked
uv run python -m cli --help >/dev/null
uv run python -m compileall cli scripts tools

for shell_script in create_secrets.sh grant_external_metadata.sh upload.sh submit.sh validate_metadata.sh; do
    bash -n "$shell_script"
done
echo ""

if [[ -z "$SKIP_SECRETS" ]]; then
    echo "--- Step 2: Creating/updating Databricks secrets from .env ---"
    ./create_secrets.sh
    echo ""
else
    echo "--- Step 2: Skipping secrets (--skip-secrets) ---"
    echo ""
fi

if [[ -z "$SKIP_GRANT" ]]; then
    echo "--- Step 3: Granting CREATE_EXTERNAL_METADATA ---"
    if [[ -n "${METADATA_GRANT_PRINCIPAL:-}" ]]; then
        ./grant_external_metadata.sh "$METADATA_GRANT_PRINCIPAL"
    else
        ./grant_external_metadata.sh
    fi
    echo ""
else
    echo "--- Step 3: Skipping grant (--skip-grant) ---"
    echo ""
fi

if [[ -z "$SKIP_UPLOAD" ]]; then
    echo "--- Step 4: Uploading validation scripts ---"
    uv run python -m cli upload --all
    echo ""
else
    echo "--- Step 4: Skipping upload (--skip-upload) ---"
    echo ""
fi

SCRIPTS=(
    "run_03_metadata_sync_tables.py"
    "run_04_metadata_sync_api.py"
)

FAILED=0
for script in "${SCRIPTS[@]}"; do
    echo "--- Step 5: Running: $script ---"
    if [[ ${#SUBMIT_ARGS[@]} -gt 0 ]]; then
        submit_cmd=(uv run python -m cli submit "$script" "${SUBMIT_ARGS[@]}")
    else
        submit_cmd=(uv run python -m cli submit "$script")
    fi
    if "${submit_cmd[@]}"; then
        echo "[OK] $script completed"
    else
        echo "[FAIL] $script failed"
        FAILED=$((FAILED + 1))
    fi
    echo ""
done

echo "============================================"
echo "METADATA VALIDATION SUMMARY"
echo "============================================"
echo "  Total scripts: ${#SCRIPTS[@]}"
echo "  Failed: $FAILED"

if [[ $FAILED -eq 0 ]]; then
    echo "  Result: ALL PASSED"
else
    echo "  Result: $FAILED FAILED"
    exit 1
fi
