"""Single validation entry point for the Neo4j UC connector demos."""

from __future__ import annotations

import argparse
import compileall
import functools
import os
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import SparkPythonTask, SubmitTask
from databricks.sdk.service.workspace import ImportFormat
from databricks_job_runner import Runner
from databricks_job_runner.errors import RunnerError


EXPECTED_RUNNER_VERSION = "0.4.8"
SECRET_KEYS = ("NEO4J_USERNAME", "NEO4J_PASSWORD")

VALIDATION_DIR = Path(__file__).resolve().parent
PROJECT_DIR = VALIDATION_DIR.parent
ENV_FILE = PROJECT_DIR / ".env"


@dataclass(frozen=True)
class ScriptSpec:
    name: str
    notebook: str | None = None


@dataclass(frozen=True)
class Suite:
    name: str
    scripts: tuple[ScriptSpec, ...]


NOTEBOOK_PARITY = Suite(
    name="notebook-parity",
    scripts=(
        ScriptSpec("run_00_load_graph.py", "getting-started/00-load-graph.ipynb"),
        ScriptSpec(
            "run_01_connection_setup.py",
            "getting-started/01-neo4j-uc-connection-setup.ipynb",
        ),
        ScriptSpec(
            "run_02_federated_queries.py",
            "getting-started/02-federated-queries.ipynb",
        ),
        ScriptSpec(
            "run_03_materialized_tables.py",
            "getting-started/03-materialized-tables.ipynb",
        ),
        ScriptSpec(
            "run_05_metadata_sync_external_api.py",
            "advanced-patterns/05_metadata_sync_external_api.ipynb",
        ),
        ScriptSpec(
            "run_06_new_federated_queries.py",
            "advanced-patterns/06_new_federated_queries.ipynb",
        ),
    ),
)

EXTRAS = Suite(
    name="extra-regression",
    scripts=(
        ScriptSpec("run_extra_connection_smoke.py"),
        ScriptSpec("run_extra_federated_regression.py"),
        ScriptSpec("run_extra_metadata_sync_tables.py"),
    ),
)

METADATA = Suite(
    name="metadata",
    scripts=(
        ScriptSpec("run_extra_metadata_sync_tables.py"),
        ScriptSpec(
            "run_05_metadata_sync_external_api.py",
            "advanced-patterns/05_metadata_sync_external_api.ipynb",
        ),
    ),
)


runner = Runner(
    run_name_prefix="validation",
    project_dir=PROJECT_DIR,
    secret_keys=list(SECRET_KEYS),
    scripts_dir="validation/scripts",
    remote_scripts_dir="scripts",
    cli_command="uv run python validate.py",
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except RunnerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except DatabricksError as exc:
        print(f"ERROR: Databricks API request failed: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Neo4j Unity Catalog connector notebook parity.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Run the notebook-parity validation suite")
    p_run.add_argument(
        "--compute",
        choices=["cluster", "serverless"],
        default=None,
        help="Override compute mode for submitted scripts",
    )
    p_run.add_argument("--skip-upload", action="store_true")
    p_run.add_argument("--include-extras", action="store_true")
    p_run.add_argument("--no-sync", action="store_true", help="Skip uv sync --locked")
    p_run.set_defaults(func=cmd_run)

    p_metadata = sub.add_parser("metadata", help="Run metadata validation")
    p_metadata.add_argument(
        "--compute",
        choices=["cluster", "serverless"],
        default=None,
        help="Override compute mode for submitted scripts",
    )
    p_metadata.add_argument("--skip-grant", action="store_true")
    p_metadata.add_argument("--skip-upload", action="store_true")
    p_metadata.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip uv sync --locked",
    )
    p_metadata.set_defaults(func=cmd_metadata)

    p_check = sub.add_parser("check", help="Run local validation checks")
    p_check.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip uv sync --locked before syntax checks",
    )
    p_check.set_defaults(func=cmd_check)

    p_grant = sub.add_parser(
        "grant",
        help="Grant CREATE_EXTERNAL_METADATA through a one-shot cluster job",
    )
    p_grant.add_argument(
        "principal",
        nargs="?",
        help="Principal to grant. Defaults to METADATA_GRANT_PRINCIPAL or current user.",
    )
    p_grant.set_defaults(func=cmd_grant)

    p_upload = sub.add_parser("upload", help="Upload validation scripts")
    p_upload.add_argument("file", nargs="?", default="test_hello.py")
    p_upload.add_argument("--all", action="store_true", help="Upload all scripts")
    p_upload.set_defaults(func=cmd_upload)

    p_submit = sub.add_parser("submit", help="Submit one validation script")
    p_submit.add_argument("script", nargs="?", default="test_hello.py")
    p_submit.add_argument("--no-wait", action="store_true")
    p_submit.add_argument(
        "--compute",
        choices=["cluster", "serverless"],
        default=None,
        help="Override compute mode for this submission",
    )
    p_submit.add_argument("--upload", action="store_true", help="Upload all first")
    p_submit.set_defaults(func=cmd_submit)

    p_workspace = sub.add_parser(
        "workspace",
        help="Check compute readiness and uploaded workspace files",
    )
    p_workspace.add_argument("file", nargs="?", default=None)
    p_workspace.set_defaults(func=cmd_workspace)

    p_logs = sub.add_parser("logs", help="Print logs for a submitted run")
    p_logs.add_argument("run_id", nargs="?", type=int, default=None)
    p_logs.set_defaults(func=cmd_logs)

    p_list = sub.add_parser("list", help="List validation suites")
    p_list.set_defaults(func=cmd_list)

    return parser


def cmd_run(args: argparse.Namespace) -> int:
    print_header("validation: Notebook Parity Suite")
    run_local_checks(sync=not args.no_sync)
    scripts = list(NOTEBOOK_PARITY.scripts)
    if args.include_extras:
        scripts.extend(EXTRAS.scripts)

    if args.skip_upload:
        print_step("Skipping script upload (--skip-upload)")
    else:
        print_step("Uploading all validation scripts")
        runner.upload_all()

    return run_suite(scripts, compute=args.compute, title="VALIDATION SUMMARY")


def cmd_metadata(args: argparse.Namespace) -> int:
    print_header("validation: Metadata Validation")
    run_local_checks(sync=not args.no_sync)

    if args.skip_grant:
        print_step("Skipping CREATE_EXTERNAL_METADATA grant (--skip-grant)")
    else:
        principal = env_value("METADATA_GRANT_PRINCIPAL") or None
        grant_external_metadata(principal)

    if args.skip_upload:
        print_step("Skipping script upload (--skip-upload)")
    else:
        print_step("Uploading all validation scripts")
        runner.upload_all()

    return run_suite(METADATA.scripts, compute=args.compute, title="METADATA SUMMARY")


def cmd_check(args: argparse.Namespace) -> int:
    run_local_checks(sync=not args.no_sync)
    return 0


def cmd_grant(args: argparse.Namespace) -> int:
    principal = args.principal or env_value("METADATA_GRANT_PRINCIPAL") or None
    grant_external_metadata(principal)
    return 0


def cmd_upload(args: argparse.Namespace) -> int:
    if args.all:
        runner.upload_all()
    else:
        runner.upload_file(args.file)
    return 0


def cmd_submit(args: argparse.Namespace) -> int:
    if args.upload:
        runner.upload_all()
    runner.submit(args.script, no_wait=args.no_wait, compute_mode=args.compute)
    return 0


def cmd_workspace(args: argparse.Namespace) -> int:
    runner.validate(args.file)
    return 0


def cmd_logs(args: argparse.Namespace) -> int:
    runner.logs(args.run_id)
    return 0


def cmd_list(_: argparse.Namespace) -> int:
    for suite in (NOTEBOOK_PARITY, EXTRAS, METADATA):
        print(f"{suite.name}:")
        for spec in suite.scripts:
            notebook = f"  ({spec.notebook})" if spec.notebook else ""
            print(f"  {spec.name}{notebook}")
        print()
    return 0


def run_local_checks(*, sync: bool) -> None:
    print_step("Local runner and syntax checks")
    if sync:
        run_process(["uv", "sync", "--locked"], cwd=VALIDATION_DIR)

    actual_version = version("databricks-job-runner")
    if actual_version != EXPECTED_RUNNER_VERSION:
        raise RunnerError(
            f"expected databricks-job-runner {EXPECTED_RUNNER_VERSION}, "
            f"got {actual_version}"
        )
    print(f"  databricks-job-runner: {actual_version}")

    paths = [VALIDATION_DIR / "validate.py", VALIDATION_DIR / "scripts"]
    tools_dir = VALIDATION_DIR / "tools"
    if tools_dir.is_dir():
        paths.append(tools_dir)
    for path in paths:
        ok = (
            compileall.compile_dir(str(path), force=True, quiet=1)
            if path.is_dir()
            else compileall.compile_file(str(path), force=True, quiet=1)
        )
        if not ok:
            raise RunnerError(f"Python compile check failed for {path}")
    print("  Python syntax checks passed")
    print()


def grant_external_metadata(principal: str | None) -> None:
    print_step("Granting CREATE_EXTERNAL_METADATA")
    require_env_keys("DATABRICKS_CLUSTER_ID", "DATABRICKS_WORKSPACE_DIR")
    principal = principal or current_workspace_user()
    cluster_id = env_value("DATABRICKS_CLUSTER_ID")
    remote_dir = f"{env_value('DATABRICKS_WORKSPACE_DIR').rstrip('/')}/admin"
    remote_file = f"{remote_dir}/grant_external_metadata.py"

    print(f"  Principal: {principal}")
    print(f"  Cluster: {cluster_id}")

    workspace_client().workspace.mkdirs(remote_dir)
    workspace_client().workspace.upload(
        path=remote_file,
        content=GRANT_TASK_SOURCE.encode(),
        format=ImportFormat.AUTO,
        overwrite=True,
    )

    task = SubmitTask(
        task_key="grant",
        spark_python_task=SparkPythonTask(
            python_file=remote_file,
            parameters=[principal],
        ),
        existing_cluster_id=cluster_id,
    )
    waiter = workspace_client().jobs.submit(
        run_name="grant_create_external_metadata",
        tasks=[task],
    )
    run_id = waiter.run_id
    print(f"  Run ID: {run_id}")
    run = waiter.result()
    result_state = (
        run.state.result_state.value
        if run.state and run.state.result_state
        else ""
    )
    print(f"  Result: {result_state or 'UNKNOWN'}")
    if result_state != "SUCCESS":
        raise RunnerError(f"grant job finished with result_state={result_state}")
    print("  Grant complete")
    print()


def current_workspace_user() -> str:
    user_name = workspace_client().current_user.me().user_name
    if not user_name:
        raise RunnerError("could not determine current workspace user")
    return user_name


def run_suite(
    scripts: Sequence[ScriptSpec],
    *,
    compute: str | None,
    title: str,
) -> int:
    failed = 0
    for spec in scripts:
        print_step(f"Running: {spec.name}")
        try:
            runner.submit(spec.name, compute_mode=compute)
            print(f"[OK] {spec.name} completed")
        except RunnerError as exc:
            failed += 1
            print(f"[FAIL] {spec.name} failed: {exc}")
        print()

    print_header(title)
    print(f"  Total scripts: {len(scripts)}")
    print(f"  Failed: {failed}")
    if failed:
        print(f"  Result: {failed} FAILED")
        return 1
    print("  Result: ALL PASSED")
    return 0


def run_process(command: list[str], *, cwd: Path) -> None:
    print(f"  $ {' '.join(command)}")
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as exc:
        raise RunnerError(
            f"command failed with exit code {exc.returncode}: {' '.join(command)}"
        ) from exc


def print_header(title: str) -> None:
    print("=" * 44)
    print(title)
    print("=" * 44)
    print()


def print_step(title: str) -> None:
    print(f"--- {title} ---")


def require_env_keys(*keys: str) -> None:
    missing = [key for key in keys if not env_value(key)]
    if missing:
        raise RunnerError(
            "required variables missing from .env:\n"
            + "".join(f"  - {key}\n" for key in missing)
        )


def env_value(key: str) -> str:
    ensure_env_loaded()
    return os.environ.get(key, "")


@functools.cache
def workspace_client() -> WorkspaceClient:
    profile = env_value("DATABRICKS_PROFILE") or None
    try:
        return WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    except ValueError as exc:
        raise RunnerError(str(exc)) from exc


_ENV_LOADED = False


def ensure_env_loaded() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    if not ENV_FILE.is_file():
        raise RunnerError(
            f"{ENV_FILE} not found. Copy .env.sample to .env and fill in values."
        )
    for key, value in parse_env_file(ENV_FILE).items():
        os.environ.setdefault(key, value)
    _ENV_LOADED = True


def parse_env_file(path: Path) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, value = stripped.partition("=")
        if not sep:
            continue
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        parsed[key] = value
    return parsed


GRANT_TASK_SOURCE = '''"""One-shot cluster task for granting External Metadata registration rights."""

import sys

from pyspark.sql import SparkSession

principal = sys.argv[1]
escaped_principal = principal.replace("`", "``")

spark = SparkSession.builder.getOrCreate()
spark.sql(
    f"GRANT CREATE_EXTERNAL_METADATA ON METASTORE TO `{escaped_principal}`"
)
print(f"Granted CREATE_EXTERNAL_METADATA on metastore to {principal}")
'''


if __name__ == "__main__":
    raise SystemExit(main())
