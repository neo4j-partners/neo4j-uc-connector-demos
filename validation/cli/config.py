"""Configuration loading and bundle variable resolution."""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values

from cli.errors import CommandError

VALIDATION_DIR = Path(__file__).resolve().parents[1]
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


def load_env() -> dict[str, str]:
    if not ENV_FILE.is_file():
        raise CommandError(
            f"{ENV_FILE} not found. Copy .env.sample to .env and fill in values."
        )
    return {k: v for k, v in dotenv_values(ENV_FILE).items() if v is not None}


def resolved_bundle_vars(env: dict[str, str]) -> dict[str, str]:
    vars_seen: dict[str, str] = {}
    for env_key, var_name in ENV_TO_VAR.items():
        value = (env.get(env_key) or "").strip()
        if value:
            vars_seen[var_name] = value
    return vars_seen


def missing_required_env_keys(bundle_vars: dict[str, str]) -> list[str]:
    missing_vars = [name for name in REQUIRED_VARS if name not in bundle_vars]
    return [env_key for env_key, var_name in ENV_TO_VAR.items() if var_name in missing_vars]


def require_bundle_vars(env: dict[str, str]) -> dict[str, str]:
    bundle_vars = resolved_bundle_vars(env)
    missing = missing_required_env_keys(bundle_vars)
    if missing:
        raise CommandError("missing required values in .env: " + ", ".join(missing))
    return bundle_vars
