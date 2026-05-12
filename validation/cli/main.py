"""Argument parsing and command dispatch for validation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from cli.bundle import deploy_bundle, run_bundle_job, validate_bundle
from cli.config import VALIDATION_DIR, load_env, resolved_bundle_vars
from cli.errors import CommandError
from cli.process import run_process
from cli.stages import STAGES, SUITES, stage_names

FORBIDDEN_PATTERNS: tuple[tuple[str, str], ...] = (
    ("DataFrame JDBC read", 'spark.read.format("jdbc"'),
    ("DataFrame JDBC read", "spark.read.format('jdbc'"),
    ("UC JDBC DataFrame option", "databricks.connection"),
    ("removed graph DataSource", "org.neo4j.spark.DataSource"),
    ("removed JDBC helper", "read_neo4j_jdbc"),
    ("removed JDBC materializer", "materialize_neo4j_jdbc"),
)


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
        description="Deploy, run, and debug Neo4j UC validation stages.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_check = sub.add_parser(
        "check",
        help="Run local checks and validate the Databricks bundle.",
    )
    add_check_options(p_check)
    add_bundle_options(p_check)
    add_dry_run_option(p_check)
    p_check.set_defaults(func=cmd_check)

    p_check_local = sub.add_parser(
        "check-local",
        help="Run local lint, compile, and removed-pattern checks.",
    )
    add_check_options(p_check_local)
    p_check_local.set_defaults(func=cmd_check_local)

    p_pattern = sub.add_parser(
        "pattern-check",
        help="Check validation scripts for removed connector patterns.",
    )
    p_pattern.set_defaults(func=cmd_pattern_check)

    p_check_bundle = sub.add_parser(
        "check-bundle",
        help="Validate the Databricks Asset Bundle definition.",
    )
    add_bundle_options(p_check_bundle)
    add_dry_run_option(p_check_bundle)
    p_check_bundle.set_defaults(func=cmd_check_bundle)

    p_deploy = sub.add_parser("deploy", help="Deploy the Databricks bundle.")
    add_bundle_options(p_deploy)
    add_dry_run_option(p_deploy)
    p_deploy.set_defaults(func=cmd_deploy)

    p_env = sub.add_parser(
        "env",
        help="Print resolved non-secret bundle variables from ../.env.",
    )
    add_bundle_options(p_env)
    p_env.set_defaults(func=cmd_env)

    p_stage = sub.add_parser("stage", help="Deploy and run one validation stage.")
    p_stage.add_argument("stage", choices=stage_names(), help="Stage name to run.")
    add_bundle_options(p_stage)
    add_run_options(p_stage)
    p_stage.set_defaults(func=cmd_stage)

    for name, suite in SUITES.items():
        p_suite = sub.add_parser(name, help=f"Deploy and run `{suite.job}`.")
        add_bundle_options(p_suite)
        add_run_options(p_suite)
        p_suite.set_defaults(func=cmd_suite, suite=name)

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


def add_check_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--no-sync", action="store_true", help="Skip uv sync --locked")


def add_dry_run_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without running them.",
    )


def add_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Skip `databricks bundle deploy` and only run the job.",
    )
    add_dry_run_option(parser)


def cmd_check(args: argparse.Namespace) -> int:
    cmd_check_local(args)
    cmd_check_bundle(args)
    print("check: OK")
    return 0


def cmd_check_local(args: argparse.Namespace) -> int:
    if not args.no_sync:
        run_process(
            ["uv", "sync", "--locked"],
            cwd=VALIDATION_DIR,
            dry_run=getattr(args, "dry_run", False),
        )
    run_process(
        [
            "uv",
            "run",
            "ruff",
            "check",
            "validate.py",
            "cli",
            "scripts",
            "tools",
        ],
        cwd=VALIDATION_DIR,
        dry_run=getattr(args, "dry_run", False),
    )
    run_process(
        [sys.executable, "-m", "py_compile", *python_files()],
        cwd=VALIDATION_DIR,
        dry_run=getattr(args, "dry_run", False),
    )
    if not getattr(args, "dry_run", False):
        run_pattern_check()
    print("check-local: OK")
    return 0


def cmd_pattern_check(_args: argparse.Namespace) -> int:
    run_pattern_check()
    print("pattern-check: OK")
    return 0


def cmd_check_bundle(args: argparse.Namespace) -> int:
    env = load_env()
    validate_bundle(
        profile=args.profile,
        target=args.target,
        env=env,
        dry_run=getattr(args, "dry_run", False),
    )
    print("check-bundle: OK")
    return 0


def cmd_deploy(args: argparse.Namespace) -> int:
    env = load_env()
    deploy_bundle(
        profile=args.profile,
        target=args.target,
        env=env,
        dry_run=args.dry_run,
    )
    return 0


def cmd_env(args: argparse.Namespace) -> int:
    env = load_env()
    profile = (args.profile or env.get("DATABRICKS_PROFILE") or "").strip()
    print(f"profile={profile or '<bundle default>'}")
    print(f"target={args.target or '<bundle default>'}")
    for name, value in sorted(resolved_bundle_vars(env).items()):
        print(f"{name}={value}")
    return 0


def cmd_stage(args: argparse.Namespace) -> int:
    env = load_env()
    stage = STAGES[args.stage]
    run_bundle_job(
        stage.job,
        profile=args.profile,
        target=args.target,
        env=env,
        skip_deploy=args.skip_deploy,
        dry_run=args.dry_run,
    )
    return 0


def cmd_suite(args: argparse.Namespace) -> int:
    env = load_env()
    suite = SUITES[args.suite]
    run_bundle_job(
        suite.job,
        profile=args.profile,
        target=args.target,
        env=env,
        skip_deploy=args.skip_deploy,
        dry_run=args.dry_run,
    )
    return 0


def python_files() -> list[str]:
    files: list[Path] = [VALIDATION_DIR / "validate.py"]
    files.extend(sorted((VALIDATION_DIR / "cli").glob("*.py")))
    files.extend(sorted((VALIDATION_DIR / "scripts").glob("*.py")))
    files.extend(sorted((VALIDATION_DIR / "tools").glob("*.py")))
    return [str(path.relative_to(VALIDATION_DIR)) for path in files]


def run_pattern_check() -> None:
    findings: list[str] = []
    for path in sorted((VALIDATION_DIR / "scripts").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for label, needle in FORBIDDEN_PATTERNS:
            if needle in text:
                rel_path = path.relative_to(VALIDATION_DIR)
                findings.append(f"{rel_path}: {label}: {needle}")
    if findings:
        raise CommandError(
            "removed validation patterns found:\n  " + "\n  ".join(findings)
        )
