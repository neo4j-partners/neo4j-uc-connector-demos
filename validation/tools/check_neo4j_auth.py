"""Local Neo4j auth check for validation.

Usage:
    uv run --with neo4j python tools/check_neo4j_auth.py
    uv run --with neo4j python tools/check_neo4j_auth.py path/to/.env
"""

from __future__ import annotations

import sys
from pathlib import Path


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def main() -> int:
    default_env = Path(__file__).resolve().parents[2] / ".env"
    env_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_env
    if not env_path.exists():
        print(f"ERROR: {env_path} not found", file=sys.stderr)
        return 1

    values = parse_env(env_path)
    uri = values.get("NEO4J_URI", "")
    if not uri:
        print("ERROR: NEO4J_URI is empty", file=sys.stderr)
        return 1
    username = values.get("NEO4J_USERNAME", "neo4j")
    password = values.get("NEO4J_PASSWORD", "")
    database = values.get("NEO4J_DATABASE", "neo4j")

    if not password:
        print("ERROR: NEO4J_PASSWORD is empty", file=sys.stderr)
        return 1

    try:
        from neo4j import GraphDatabase
    except ImportError:
        print(
            "ERROR: neo4j package is not installed. Run with:\n"
            "  uv run --with neo4j python tools/check_neo4j_auth.py",
            file=sys.stderr,
        )
        return 1

    print(f"URI:      {uri}")
    print(f"Database: {database}")
    print(f"Username: {username}")
    print("Password: <redacted>")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    try:
        try:
            driver.verify_connectivity()
            with driver.session(database=database) as session:
                count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
        except Exception as exc:
            print(f"ERROR: {exc.__class__.__name__}: {exc}", file=sys.stderr)
            return 1
    finally:
        driver.close()

    print(f"OK: connected and counted {count:,} nodes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
