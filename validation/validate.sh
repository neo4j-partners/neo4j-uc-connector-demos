#!/usr/bin/env bash
# Run the full federation validation suite.
#
# Usage:
#   ./validate.sh                         # local checks + upload + run all scripts sequentially
#   ./validate.sh --skip-upload
#   ./validate.sh --compute serverless

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$SCRIPT_DIR/.uv-cache}"

SKIP_UPLOAD=""
SUBMIT_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
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
echo "validation: Full Validation Suite"
echo "============================================"
echo ""

echo "--- Step 1: Local runner and syntax checks ---"
uv sync --locked

RUNNER_VERSION="$(
    uv run python -c "from importlib.metadata import version; print(version('databricks-job-runner'))"
)"
if [[ "$RUNNER_VERSION" != "0.4.8" ]]; then
    echo "ERROR: expected databricks-job-runner 0.4.8, got $RUNNER_VERSION" >&2
    exit 1
fi
echo "  databricks-job-runner: $RUNNER_VERSION"

uv run python -c "from databricks_job_runner import Runner; print(Runner)"
uv run python -m cli --help

PYTHON_CHECK_DIRS=(cli scripts)
if [[ -d tools ]]; then
    PYTHON_CHECK_DIRS+=(tools)
fi
uv run python -m compileall "${PYTHON_CHECK_DIRS[@]}"

for shell_script in upload.sh submit.sh validate.sh validate_metadata.sh grant_external_metadata.sh; do
    if [[ -f "$shell_script" ]]; then
        bash -n "$shell_script"
    fi
done
bash -n ../create_secrets.sh
bash -n ../getting-started/upload_data.sh
echo ""

SCRIPTS=(
    "run_01_connection_validation.py"
    "run_02_federated_queries.py"
    "run_03_metadata_sync_tables.py"
    # Requires CREATE_EXTERNAL_METADATA on the metastore. Run
    # ./grant_external_metadata.sh first if this script reports
    # PERMISSION_DENIED from the External Metadata API.
    "run_04_metadata_sync_api.py"
    "run_05_advanced_spark_queries.py"
)

if [[ -z "$SKIP_UPLOAD" ]]; then
    echo "--- Step 2: Uploading all scripts ---"
    uv run python -m cli upload --all
    echo ""
else
    echo "--- Step 2: Skipping upload (--skip-upload) ---"
    echo ""
fi

FAILED=0
for script in "${SCRIPTS[@]}"; do
    echo "--- Step 3: Running: $script ---"
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
echo "VALIDATION SUMMARY"
echo "============================================"
echo "  Total scripts: ${#SCRIPTS[@]}"
echo "  Failed: $FAILED"

if [[ $FAILED -eq 0 ]]; then
    echo "  Result: ALL PASSED"
else
    echo "  Result: $FAILED FAILED"
    exit 1
fi
