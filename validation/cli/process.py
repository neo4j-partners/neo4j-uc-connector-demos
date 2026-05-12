"""Subprocess helpers for validation commands."""

from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

from cli.errors import CommandError


def run_process(command: list[str], *, cwd: Path, dry_run: bool = False) -> None:
    pretty = shlex.join(command)
    prefix = "DRY-RUN" if dry_run else "$"
    print(f"  {prefix} {pretty}", flush=True)
    if dry_run:
        return
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as exc:
        raise CommandError(
            f"command failed with exit code {exc.returncode}: {pretty}"
        ) from exc
