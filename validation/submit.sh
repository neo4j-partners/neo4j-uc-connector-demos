#!/usr/bin/env bash
# Submit a Python script as a one-time Databricks job run.
#
# Usage:
#   ./submit.sh                                      # runs test_hello.py (default)
#   ./submit.sh run_01_connection_setup.py           # runs notebook 01 parity
#   ./submit.sh run_01_connection_setup.py --compute serverless

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export UV_CACHE_DIR="${UV_CACHE_DIR:-$SCRIPT_DIR/.uv-cache}"

if [[ $# -eq 0 ]]; then
    set -- test_hello.py
fi

uv run python -m cli submit "$@"
