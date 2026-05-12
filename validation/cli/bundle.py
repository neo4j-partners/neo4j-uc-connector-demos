"""Databricks Asset Bundle command construction and execution."""

from __future__ import annotations

import shutil

from cli.config import PROJECT_DIR, require_bundle_vars
from cli.errors import CommandError
from cli.process import run_process


def bundle_option_args(
    *, profile: str | None, target: str | None, env: dict[str, str]
) -> list[str]:
    """Return Databricks bundle global option args from CLI flags and .env."""
    option_args: list[str] = []
    resolved_profile = (profile or env.get("DATABRICKS_PROFILE") or "").strip()
    if resolved_profile:
        option_args.extend(["--profile", resolved_profile])
    if target:
        option_args.extend(["--target", target])
    return option_args


def bundle_var_args(env: dict[str, str]) -> list[str]:
    """Return [--var k=v, --var k=v, ...] derived from the repo-root .env."""
    pairs: list[str] = []
    for var_name, value in require_bundle_vars(env).items():
        pairs.extend(["--var", f"{var_name}={value}"])
    return pairs


def ensure_databricks_cli() -> None:
    if shutil.which("databricks") is None:
        raise CommandError(
            "databricks CLI not on PATH. Install per "
            "https://docs.databricks.com/dev-tools/cli/install.html"
        )


def bundle_context_args(
    *,
    profile: str | None,
    target: str | None,
    env: dict[str, str],
    require_cli: bool = True,
) -> tuple[list[str], list[str]]:
    if require_cli:
        ensure_databricks_cli()
    return (
        bundle_option_args(profile=profile, target=target, env=env),
        bundle_var_args(env),
    )


def validate_bundle(
    *, profile: str | None, target: str | None, env: dict[str, str], dry_run: bool = False
) -> None:
    option_args, var_args = bundle_context_args(
        profile=profile,
        target=target,
        env=env,
        require_cli=not dry_run,
    )
    run_process(
        ["databricks", "bundle", "validate", *option_args, *var_args],
        cwd=PROJECT_DIR,
        dry_run=dry_run,
    )


def deploy_bundle(
    *, profile: str | None, target: str | None, env: dict[str, str], dry_run: bool = False
) -> None:
    option_args, var_args = bundle_context_args(
        profile=profile,
        target=target,
        env=env,
        require_cli=not dry_run,
    )
    run_process(
        ["databricks", "bundle", "deploy", *option_args, *var_args],
        cwd=PROJECT_DIR,
        dry_run=dry_run,
    )


def run_bundle_job(
    job: str,
    *,
    profile: str | None,
    target: str | None,
    env: dict[str, str],
    skip_deploy: bool = False,
    dry_run: bool = False,
) -> None:
    option_args, var_args = bundle_context_args(
        profile=profile,
        target=target,
        env=env,
        require_cli=not dry_run,
    )
    if not skip_deploy:
        run_process(
            ["databricks", "bundle", "deploy", *option_args, *var_args],
            cwd=PROJECT_DIR,
            dry_run=dry_run,
        )

    run_process(
        ["databricks", "bundle", "run", job, *option_args, *var_args],
        cwd=PROJECT_DIR,
        dry_run=dry_run,
    )
