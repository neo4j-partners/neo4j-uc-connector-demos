"""Bundle wrapper for the Neo4j UC connector validation suites.

Each subcommand reads the repo-root .env, derives the required `--var`
arguments, then shells `databricks bundle deploy && databricks bundle run
<job>`. The DAB jobs live under `resources/`.
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

VALIDATION_DIR = Path(__file__).resolve().parent
PROJECT_DIR = VALIDATION_DIR.parent
ENV_FILE = PROJECT_DIR / ".env"

# Mapping from .env keys to the corresponding bundle variable name. .env keys
# absent from this map are ignored.
ENV_TO_VAR: dict[str, str] = {
    "UC_CATALOG": "catalog",
    "UC_SCHEMA": "schema",
    "UC_VOLUME": "volume",
    "JDBC_JAR_PATH": "jdbc_jar_path",
    "UC_CONNECTION_NAME": "uc_connection_name",
    "DATABRICKS_SECRET_SCOPE": "secret_scope",
    "NEO4J_URI": "neo4j_uri",
    "NEO4J_DATABASE": "neo4j_database",
    "DATABRICKS_CLUSTER_ID": "cluster_id",
    "LAKEHOUSE_SCHEMA": "lakehouse_schema",
    "METADATA_CATALOG": "metadata_catalog",
    "NODES_SCHEMA": "nodes_schema",
    "RELATIONSHIPS_SCHEMA": "relationships_schema",
    "METADATA_GRANT_PRINCIPAL": "metadata_grant_principal",
}

# Bundle variables that must always have a non-empty value supplied at deploy
# time. Other vars carry defaults inside databricks.yml.
REQUIRED_VARS: tuple[str, ...] = ("catalog", "jdbc_jar_path", "neo4j_uri", "cluster_id")

JOBS: dict[str, str] = {
    "run": "notebook_parity",
    "extras": "extras",
    "metadata": "metadata",
}


class CommandError(RuntimeError):
    """Raised for any user-actionable failure in the wrapper."""


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except CommandError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deploy and run the Neo4j UC validation bundle jobs.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_check = sub.add_parser(
        "check",
        help="Lint the validation scripts and validate the bundle definition.",
    )
    p_check.add_argument("--no-sync", action="store_true", help="Skip uv sync --locked")
    add_bundle_options(p_check)
    p_check.set_defaults(func=cmd_check)

    for name, job in JOBS.items():
        p = sub.add_parser(
            name,
            help=f"Deploy the bundle and run the `{job}` job.",
        )
        add_bundle_options(p)
        p.add_argument(
            "--skip-deploy",
            action="store_true",
            help="Skip `databricks bundle deploy` and only run the job.",
        )
        p.set_defaults(func=cmd_run, job=job)

    return parser


def add_bundle_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--target",
        "-t",
        default=None,
        help="Bundle target to deploy/run against (default: bundle default target).",
    )
    parser.add_argument(
        "--profile",
        "-p",
        default=None,
        help="Databricks CLI profile (default: DATABRICKS_PROFILE from .env).",
    )


def cmd_check(args: argparse.Namespace) -> int:
    env = load_env()
    if not args.no_sync:
        run_process(["uv", "sync", "--locked"], cwd=VALIDATION_DIR)
    run_process(["uv", "run", "ruff", "check", "validate.py", "scripts"], cwd=VALIDATION_DIR)
    run_process(
        [
            "databricks",
            "bundle",
            "validate",
            *bundle_option_args(args, env),
            *bundle_var_args(env),
        ],
        cwd=PROJECT_DIR,
    )
    print("check: OK")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    env = load_env()
    option_args = bundle_option_args(args, env)
    var_args = bundle_var_args(env)

    if not args.skip_deploy:
        run_process(
            ["databricks", "bundle", "deploy", *option_args, *var_args],
            cwd=PROJECT_DIR,
        )

    run_process(
        ["databricks", "bundle", "run", args.job, *option_args, *var_args],
        cwd=PROJECT_DIR,
    )
    return 0


def bundle_option_args(args: argparse.Namespace, env: dict[str, str]) -> list[str]:
    """Return Databricks bundle global option args from CLI flags and .env."""
    option_args: list[str] = []
    profile = (args.profile or env.get("DATABRICKS_PROFILE") or "").strip()
    if profile:
        option_args.extend(["--profile", profile])
    if args.target:
        option_args.extend(["--target", args.target])
    return option_args


def bundle_var_args(env: dict[str, str]) -> list[str]:
    """Return [--var k=v, --var k=v, ...] derived from the repo-root .env."""
    if shutil.which("databricks") is None:
        raise CommandError(
            "databricks CLI not on PATH. Install per "
            "https://docs.databricks.com/dev-tools/cli/install.html"
        )
    vars_seen: dict[str, str] = {}
    for env_key, var_name in ENV_TO_VAR.items():
        value = (env.get(env_key) or "").strip()
        if value:
            vars_seen[var_name] = value

    missing = [v for v in REQUIRED_VARS if v not in vars_seen]
    if missing:
        env_keys = [k for k, v in ENV_TO_VAR.items() if v in missing]
        raise CommandError(
            "missing required values in .env: " + ", ".join(env_keys)
        )

    pairs: list[str] = []
    for var_name, value in vars_seen.items():
        pairs.extend(["--var", f"{var_name}={value}"])
    return pairs


def load_env() -> dict[str, str]:
    if not ENV_FILE.is_file():
        raise CommandError(
            f"{ENV_FILE} not found. Copy .env.sample to .env and fill in values."
        )
    return {k: v for k, v in dotenv_values(ENV_FILE).items() if v is not None}


def run_process(command: list[str], *, cwd: Path) -> None:
    pretty = shlex.join(command)
    print(f"  $ {pretty}", flush=True)
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as exc:
        raise CommandError(
            f"command failed with exit code {exc.returncode}: {pretty}"
        ) from exc


if __name__ == "__main__":
    raise SystemExit(main())
