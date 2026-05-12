#!/usr/bin/env bash
# Upload aircraft digital twin CSV files to a UC Volume.
#
# Reads UC_CATALOG, UC_SCHEMA, UC_VOLUME, and DATABRICKS_PROFILE from the
# repo-root .env. CSV files are sourced from ./data/aircraft_digital_twin_data/.
#
# Run this once before running notebook 00, or before running validation/.
#
# Usage:
#   ./upload_data.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data/aircraft_digital_twin_data"

# Load repo-root .env
set -a
# shellcheck disable=SC1091
source "$SCRIPT_DIR/../.env"
set +a

VOLUME_PATH="/Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}"

PROFILE_FLAG=()
if [[ -n "${DATABRICKS_PROFILE:-}" ]]; then
    PROFILE_FLAG=(--profile "$DATABRICKS_PROFILE")
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo "Error: $DATA_DIR not found" >&2
    exit 1
fi

echo "Uploading aircraft CSV data to $VOLUME_PATH"
echo ""

echo "Ensuring UC schema and volume exist"
if databricks schemas get "${UC_CATALOG}.${UC_SCHEMA}" "${PROFILE_FLAG[@]}" >/dev/null 2>&1; then
    echo "  Schema exists: ${UC_CATALOG}.${UC_SCHEMA}"
else
    databricks schemas create "$UC_SCHEMA" "$UC_CATALOG" "${PROFILE_FLAG[@]}"
fi

if databricks volumes read "${UC_CATALOG}.${UC_SCHEMA}.${UC_VOLUME}" "${PROFILE_FLAG[@]}" >/dev/null 2>&1; then
    echo "  Volume exists: ${UC_CATALOG}.${UC_SCHEMA}.${UC_VOLUME}"
else
    databricks volumes create "$UC_CATALOG" "$UC_SCHEMA" "$UC_VOLUME" MANAGED "${PROFILE_FLAG[@]}"
fi

echo ""

for f in "$DATA_DIR"/*.csv; do
    [[ -f "$f" ]] || continue
    filename=$(basename "$f")
    echo "  $filename"
    databricks fs cp "${PROFILE_FLAG[@]}" --overwrite "$f" "dbfs:$VOLUME_PATH/$filename"
done

echo ""
echo "Upload complete."
