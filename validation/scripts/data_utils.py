"""Shared utilities for validation scripts.

Config is loaded from os.environ after inject_params() is called at startup.
The runner passes all .env extras as KEY=VALUE job parameters; inject_params()
parses those into os.environ and fetches Neo4j credentials from the Databricks
secret scope.

Provides:
- inject_params / get_config — parameter injection and config building
- csv_rows — Neo4j import CSV reader (used by run_00_load_graph)
- Neo4j connection helpers
- UC JDBC read helpers (DataFrame API and remote_query)
- PASS/FAIL reporting and summary
"""

import csv
import os
import sys
from dataclasses import dataclass
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Parameter injection (inline — databricks-job-runner not available on cluster)
# ---------------------------------------------------------------------------

def inject_params() -> None:
    """Parse KEY=VALUE parameters from sys.argv into os.environ, then load secrets."""
    remaining = []
    for arg in sys.argv[1:]:
        if "=" in arg and not arg.startswith("-"):
            key, _, value = arg.partition("=")
            os.environ.setdefault(key, value)
        else:
            remaining.append(arg)
    sys.argv[1:] = remaining
    _load_secrets()


def _load_secrets() -> None:
    """Fetch secrets from a Databricks secret scope into os.environ."""
    scope = os.environ.get("DATABRICKS_SECRET_SCOPE")
    raw_keys = os.environ.get("DATABRICKS_SECRET_KEYS")
    if not scope or not raw_keys:
        return
    keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
    if not keys:
        return

    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    for key in keys:
        try:
            value = ws.dbutils.secrets.get(scope=scope, key=key)
            os.environ.setdefault(key, value)
        except Exception as exc:
            print(f"WARNING: failed to load secret '{key}' from scope '{scope}': {exc}")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """Resolved configuration for a validation run."""

    neo4j_host: str
    neo4j_username: str
    neo4j_password: str
    neo4j_database: str
    neo4j_bolt_uri: str
    neo4j_jdbc_url: str
    neo4j_jdbc_url_sql: str
    uc_connection_name: str
    jdbc_jar_path: str
    java_dependencies: str
    lakehouse_catalog: str
    lakehouse_schema: str
    lakehouse_fqn: str
    metadata_catalog: str
    nodes_schema: str
    relationships_schema: str
    uc_catalog: str | None
    uc_schema: str
    uc_volume: str
    volume_path: str | None


def get_config() -> Config:
    """Build Config from environment variables set by inject_params()."""
    neo4j_uri = os.environ.get("NEO4J_URI")
    neo4j_host = os.environ.get("NEO4J_HOST", "")
    if neo4j_uri:
        neo4j_host = _host_from_neo4j_uri(neo4j_uri)
    elif not neo4j_host:
        raise KeyError("NEO4J_HOST or NEO4J_URI")
    elif neo4j_host.startswith(("neo4j+s://", "neo4j+ssc://", "neo4j://", "bolt+s://", "bolt://")):
        neo4j_host = _host_from_neo4j_uri(neo4j_host)
    else:
        neo4j_host = neo4j_host.rstrip("/")

    uc_catalog = os.environ.get("UC_CATALOG")
    uc_schema = os.environ.get("UC_SCHEMA", "neo4j_getting_started")
    uc_volume = os.environ.get("UC_VOLUME", "aircraft_data")
    lakehouse_catalog = os.environ.get("LAKEHOUSE_CATALOG") or uc_catalog
    if not lakehouse_catalog:
        raise KeyError("LAKEHOUSE_CATALOG or UC_CATALOG")
    lakehouse_schema = os.environ.get("LAKEHOUSE_SCHEMA") or "lakehouse"
    volume_path = (
        f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}" if uc_catalog else None
    )
    neo4j_database = os.environ.get("NEO4J_DATABASE", "neo4j")
    neo4j_host_with_port = _with_default_port(neo4j_host, 7687)
    jdbc_jar_path = os.environ["JDBC_JAR_PATH"]

    return Config(
        neo4j_host=neo4j_host,
        neo4j_username=os.environ.get("NEO4J_USERNAME", "neo4j"),
        neo4j_password=os.environ["NEO4J_PASSWORD"],
        neo4j_database=neo4j_database,
        neo4j_bolt_uri=f"neo4j+s://{neo4j_host_with_port}",
        neo4j_jdbc_url=f"jdbc:neo4j+s://{neo4j_host_with_port}/{neo4j_database}",
        neo4j_jdbc_url_sql=f"jdbc:neo4j+s://{neo4j_host_with_port}/{neo4j_database}?enableSQLTranslation=true",
        uc_connection_name=os.environ["UC_CONNECTION_NAME"],
        jdbc_jar_path=jdbc_jar_path,
        java_dependencies=f'["{jdbc_jar_path}"]',
        lakehouse_catalog=lakehouse_catalog,
        lakehouse_schema=lakehouse_schema,
        lakehouse_fqn=f"`{lakehouse_catalog}`.`{lakehouse_schema}`",
        metadata_catalog=os.environ.get("METADATA_CATALOG", "neo4j_metadata"),
        nodes_schema=os.environ.get("NODES_SCHEMA", "nodes"),
        relationships_schema=os.environ.get("RELATIONSHIPS_SCHEMA", "relationships"),
        uc_catalog=uc_catalog,
        uc_schema=uc_schema,
        uc_volume=uc_volume,
        volume_path=volume_path,
    )


def _host_from_neo4j_uri(value: str) -> str:
    """Return the host from a Neo4j/Bolt URI without path or trailing slash."""
    uri = value.strip().removeprefix("jdbc:").rstrip("/")
    parsed = urlparse(uri)
    if parsed.netloc:
        return parsed.netloc
    return uri.split("://", 1)[-1].split("/", 1)[0]


def _with_default_port(host: str, port: int) -> str:
    """Append a default port when a host string does not already include one."""
    if ":" in host:
        return host
    return f"{host}:{port}"


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def csv_rows(path: str) -> list:
    """Read a Neo4j import CSV and normalize column names.

    Converts Neo4j import column prefixes to plain names:
      :ID(Label)       → id
      :START_ID(Label) → start_id
      :END_ID(Label)   → end_id
      :TYPE            → type
    """
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            norm = {}
            for k, v in row.items():
                if k.startswith(":ID("):
                    norm["id"] = v
                elif k.startswith(":START_ID("):
                    norm["start_id"] = v
                elif k.startswith(":END_ID("):
                    norm["end_id"] = v
                elif k == ":TYPE":
                    norm["type"] = v
                else:
                    norm[k] = v
            rows.append(norm)
    return rows


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

def get_neo4j_driver(cfg: Config):
    """Create and return a Neo4j driver from config.

    Uses cfg.neo4j_bolt_uri, which get_config() always builds as
    neo4j+s://host:port (TLS). Non-TLS bolt is not supported by validation —
    the project targets Neo4j Aura where TLS is mandatory.
    """
    from neo4j import GraphDatabase
    return GraphDatabase.driver(cfg.neo4j_bolt_uri, auth=(cfg.neo4j_username, cfg.neo4j_password))


# ---------------------------------------------------------------------------
# UC JDBC helpers
# ---------------------------------------------------------------------------

def read_neo4j_jdbc(spark, cfg: Config, custom_schema: str, query: str):
    """Read from Neo4j through the UC JDBC connection."""
    return (
        spark.read.format("jdbc")
        .option("databricks.connection", cfg.uc_connection_name)
        .option("customSchema", custom_schema)
        .option("query", query)
        .load()
    )


def remote_query(spark, cfg: Config, query: str):
    """Execute a query via remote_query() SQL function."""
    safe_query = query.replace("'", "''")
    return spark.sql(f"""
        SELECT * FROM remote_query('{cfg.uc_connection_name}',
            query => '{safe_query}')
    """)


# ---------------------------------------------------------------------------
# PASS/FAIL reporting
# ---------------------------------------------------------------------------

class ValidationResults:
    """Collects PASS/FAIL results and prints a summary."""

    def __init__(self):
        self.results = []

    def record(self, name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        self.results.append((name, passed, detail))
        msg = f"  [{status}] {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)

    def summary(self) -> bool:
        passed = sum(1 for _, p, _ in self.results if p)
        total = len(self.results)
        print("")
        print("=" * 60)
        print(f"RESULTS: {passed}/{total} passed")
        print("=" * 60)
        for name, p, detail in self.results:
            status = "PASS" if p else "FAIL"
            line = f"  [{status}] {name}"
            if detail:
                line += f" — {detail}"
            print(line)
        print("")
        if passed == total:
            print("STATUS: ALL PASSED")
        else:
            print(f"STATUS: {total - passed} FAILED")
        return passed == total
