#!/usr/bin/env bash
# Upload Python scripts to the Databricks workspace.
#
# Usage:
#   ./upload.sh                          # uploads test_hello.py (default)
#   ./upload.sh run_01_connection_validation.py   # uploads a specific file
#   ./upload.sh --all                    # uploads all scripts/*.py files

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$SCRIPT_DIR/.uv-cache}"

if [[ "${1:-}" == "--all" ]]; then
    uv run python -m cli upload --all
else
    if [[ $# -eq 0 ]]; then
        set -- test_hello.py
    fi
    uv run python -m cli upload "$@"
fi
