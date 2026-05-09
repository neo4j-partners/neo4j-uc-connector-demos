"""CLI wrapper — wires Runner to the validation project layout."""

from databricks_job_runner import Runner

# Keys whose values live in a Databricks secret scope rather than
# being passed as plaintext job parameters.
SECRET_KEYS = ["NEO4J_USERNAME", "NEO4J_PASSWORD"]

runner = Runner(
    run_name_prefix="validation",
    secret_keys=SECRET_KEYS,
    scripts_dir="scripts",
    cli_command="uv run python -m cli",
)
