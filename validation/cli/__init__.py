"""CLI wrapper — wires Runner to the validation project layout."""

from pathlib import Path

from databricks_job_runner import Runner

# Keys whose values live in a Databricks secret scope rather than
# being passed as plaintext job parameters.
SECRET_KEYS = ["NEO4J_USERNAME", "NEO4J_PASSWORD"]

PROJECT_DIR = Path(__file__).resolve().parents[2]

runner = Runner(
    run_name_prefix="validation",
    project_dir=PROJECT_DIR,
    secret_keys=SECRET_KEYS,
    scripts_dir="validation/scripts",
    remote_scripts_dir="scripts",
    cli_command="uv run python -m cli",
)
