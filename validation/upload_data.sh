#!/usr/bin/env bash
# Upload aircraft digital twin CSV files to a UC Volume.
#
# Reads UC_CATALOG, UC_SCHEMA, UC_VOLUME, and DATABRICKS_PROFILE from .env.
# CSV files are sourced from ../getting-started/data/aircraft_digital_twin_data/.
#
# Run this once before submitting run_01 or run_02.
#
# Usage:
#   ./upload_data.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../getting-started/data/aircraft_digital_twin_data"

# Load .env
set -a
# shellcheck disable=SC1091
source "$SCRIPT_DIR/.env"
set +a

VOLUME_PATH="/Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}"

PROFILE_FLAG=""
if [[ -n "${DATABRICKS_PROFILE:-}" ]]; then
    PROFILE_FLAG="--profile $DATABRICKS_PROFILE"
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo "Error: $DATA_DIR not found" >&2
    exit 1
fi

echo "Uploading aircraft CSV data to $VOLUME_PATH"
echo ""

for f in "$DATA_DIR"/*.csv; do
    [[ -f "$f" ]] || continue
    filename=$(basename "$f")
    echo "  $filename"
    # shellcheck disable=SC2086
    databricks fs cp $PROFILE_FLAG --overwrite "$f" "dbfs:$VOLUME_PATH/$filename"
done

echo ""
echo "Upload complete."
