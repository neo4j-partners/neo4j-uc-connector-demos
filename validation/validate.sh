#!/usr/bin/env bash
# Run the full federation validation suite.
#
# Usage:
#   ./validate.sh                         # setup + upload + run notebook parity scripts sequentially
#   ./validate.sh --skip-data --skip-secrets --skip-upload
#   ./validate.sh --skip-upload
#   ./validate.sh --compute serverless

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$SCRIPT_DIR/.uv-cache}"

SKIP_UPLOAD=""
SKIP_DATA=""
SKIP_SECRETS=""
INCLUDE_EXTRAS=""
INCLUDE_PERFORMANCE=""
SUBMIT_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-data)
            SKIP_DATA="true"
            shift
            ;;
        --skip-secrets)
            SKIP_SECRETS="true"
            shift
            ;;
        --skip-upload)
            SKIP_UPLOAD="true"
            shift
            ;;
        --include-extras)
            INCLUDE_EXTRAS="true"
            shift
            ;;
        --include-performance)
            INCLUDE_PERFORMANCE="true"
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
echo "validation: Notebook Parity Suite"
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
    "run_00_load_graph.py"
    "run_01_connection_setup.py"
    "run_02_federated_queries.py"
    "run_03_materialized_tables.py"
    # Requires CREATE_EXTERNAL_METADATA on the metastore. Run
    # ./grant_external_metadata.sh first if this script reports
    # PERMISSION_DENIED from the External Metadata API.
    "run_05_metadata_sync_external_api.py"
    "run_06_new_federated_queries.py"
)

EXTRA_SCRIPTS=(
    "run_extra_connection_smoke.py"
    "run_extra_federated_regression.py"
    "run_extra_metadata_sync_tables.py"
)

PERFORMANCE_SCRIPTS=(
    "run_07_performance_diagnostics.py"
)

if [[ -n "$INCLUDE_EXTRAS" ]]; then
    SCRIPTS+=("${EXTRA_SCRIPTS[@]}")
fi

if [[ -n "$INCLUDE_PERFORMANCE" ]]; then
    for performance_script in "${PERFORMANCE_SCRIPTS[@]}"; do
        if [[ -f "scripts/$performance_script" ]]; then
            SCRIPTS+=("$performance_script")
        else
            echo "ERROR: --include-performance requested, but scripts/$performance_script does not exist" >&2
            exit 1
        fi
    done
fi

echo "--- Step 2: Coverage manifest ---"
if [[ -f coverage_manifest.md ]]; then
    sed -n '/^| Notebook /,/^$/p' coverage_manifest.md
else
    echo "  coverage_manifest.md not found"
fi
echo ""

if [[ -z "$SKIP_DATA" ]]; then
    echo "--- Step 3: Uploading sample CSV data ---"
    (cd .. && ./getting-started/upload_data.sh)
    echo ""
else
    echo "--- Step 3: Skipping sample data upload (--skip-data) ---"
    echo ""
fi

if [[ -z "$SKIP_SECRETS" ]]; then
    echo "--- Step 4: Creating/updating Databricks secrets from .env ---"
    (cd .. && ./create_secrets.sh)
    echo ""
else
    echo "--- Step 4: Skipping secrets (--skip-secrets) ---"
    echo ""
fi

if [[ -z "$SKIP_UPLOAD" ]]; then
    echo "--- Step 5: Uploading all scripts ---"
    uv run python -m cli upload --all
    echo ""
else
    echo "--- Step 5: Skipping upload (--skip-upload) ---"
    echo ""
fi

FAILED=0
for script in "${SCRIPTS[@]}"; do
    echo "--- Step 6: Running: $script ---"
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
