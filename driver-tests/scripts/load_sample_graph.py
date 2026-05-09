#!/usr/bin/env python3
"""Load the sample aircraft graph directly into Neo4j.

This is the local-driver equivalent of validation/scripts/run_00_load_graph.py.
It uses the same CSV files and Cypher load statements, but reads the local
repository data files and connects with the Neo4j Python driver instead of
going through Databricks.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "getting-started/data/aircraft_digital_twin_data"
DEFAULT_ENV_FILE = REPO_ROOT / "driver-tests/.env"
DEFAULT_DATABASE = "neo4j"


@dataclass(frozen=True)
class Config:
    uri: str
    username: str
    password: str
    database: str
    data_dir: Path
    clear: bool


@dataclass(frozen=True)
class NodeLoad:
    label: str
    csv_name: str
    query: str
    id_property: str
    expected: int


@dataclass(frozen=True)
class RelationshipLoad:
    rel_type: str
    csv_name: str
    match_create: str


class ValidationResults:
    def __init__(self) -> None:
        self.results: list[tuple[str, bool, str]] = []

    def record(self, name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        self.results.append((name, passed, detail))
        message = f"  [{status}] {name}"
        if detail:
            message += f" - {detail}"
        print(message)

    def summary(self) -> bool:
        passed = sum(1 for _, ok, _ in self.results if ok)
        total = len(self.results)
        print("")
        print("=" * 60)
        print(f"RESULTS: {passed}/{total} passed")
        print("=" * 60)
        for name, ok, detail in self.results:
            status = "PASS" if ok else "FAIL"
            line = f"  [{status}] {name}"
            if detail:
                line += f" - {detail}"
            print(line)
        print("")
        status = (
            "STATUS: ALL PASSED"
            if passed == total
            else f"STATUS: {total - passed} FAILED"
        )
        print(status)
        return passed == total


def main() -> int:
    args = parse_args()
    env = load_env_file(args.env_file)
    cfg = build_config(args, env)

    validate_data_dir(cfg.data_dir)
    graph_database = import_graph_database()

    print("=" * 60)
    print("driver-tests: Direct Python Sample Graph Load")
    print("=" * 60)
    print(f"  Neo4j URI: {cfg.uri}")
    print(f"  Database:  {cfg.database}")
    print(f"  Data dir:  {cfg.data_dir}")
    print("")

    results = ValidationResults()

    with graph_database.driver(cfg.uri, auth=(cfg.username, cfg.password)) as driver:
        driver.verify_connectivity()
        if cfg.clear:
            clear_existing_data(driver, cfg, results)
            if not results.results[-1][1]:
                results.summary()
                return 1

        create_constraints(driver, cfg, results)
        load_nodes(driver, cfg, results)
        load_relationships(driver, cfg, results)
        verify_counts(driver, cfg, results)

    return 0 if results.summary() else 1


def import_graph_database() -> Any:
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise SystemExit(
            "Missing Neo4j Python driver. Run with "
            "`uv run --with neo4j python scripts/load_sample_graph.py` "
            "or install the `neo4j` package."
        ) from exc
    return GraphDatabase


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the direct-JDBC validation sample graph into Neo4j."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="Path to a .env file with NEO4J_* settings.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing the aircraft digital twin CSV files.",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Do not clear existing Neo4j data before loading.",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    with path.open(encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            if not separator:
                continue
            values[key.strip()] = unquote(value.strip())
    return values


def unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def build_config(args: argparse.Namespace, env: dict[str, str]) -> Config:
    uri = setting("NEO4J_URI", env)
    host = setting("NEO4J_HOST", env)
    username = setting("NEO4J_USERNAME", env) or "neo4j"
    password = setting("NEO4J_PASSWORD", env)
    database = setting("NEO4J_DATABASE", env) or DEFAULT_DATABASE

    if not password:
        raise SystemExit(
            "Missing NEO4J_PASSWORD. Set it in driver-tests/.env "
            "or the environment."
        )
    if not uri:
        if not host:
            raise SystemExit(
                "Missing NEO4J_URI or NEO4J_HOST. Set one in "
                "driver-tests/.env or the environment."
            )
        uri = host_to_uri(host)

    return Config(
        uri=normalize_neo4j_uri(uri),
        username=username,
        password=password,
        database=database,
        data_dir=args.data_dir.resolve(),
        clear=not args.no_clear,
    )


def setting(key: str, env: dict[str, str]) -> str | None:
    return os.environ.get(key) or env.get(key)


def host_to_uri(host: str) -> str:
    clean_host = host.strip()
    for prefix in ("neo4j+s://", "neo4j+ssc://", "neo4j://"):
        clean_host = clean_host.removeprefix(prefix)
    return f"neo4j+s://{clean_host.rstrip('/')}"


def normalize_neo4j_uri(value: str) -> str:
    uri = value.strip().rstrip("/")
    if uri.startswith("jdbc:"):
        uri = uri.removeprefix("jdbc:")
    parsed = urlparse(uri)
    allowed_schemes = {"neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc"}
    if parsed.scheme not in allowed_schemes:
        raise SystemExit(
            "NEO4J_URI must start with neo4j://, neo4j+s://, neo4j+ssc://, "
            "bolt://, bolt+s://, bolt+ssc://, or jdbc:neo4j."
        )
    if not parsed.netloc:
        raise SystemExit(f"NEO4J_URI is missing a host: {value}")
    return f"{parsed.scheme}://{parsed.netloc}"


def validate_data_dir(data_dir: Path) -> None:
    missing = [
        csv_name
        for csv_name in required_csv_names()
        if not (data_dir / csv_name).is_file()
    ]
    if missing:
        formatted = "\n  ".join(missing)
        raise SystemExit(f"Missing required CSV files in {data_dir}:\n  {formatted}")


def required_csv_names() -> list[str]:
    return [load.csv_name for load in node_loads()] + [
        load.csv_name for load in relationship_loads()
    ]


def clear_existing_data(driver: Any, cfg: Config, results: ValidationResults) -> None:
    print("--- Section 1: Clear Existing Data ---")
    try:
        with driver.session(database=cfg.database) as session:
            session.run(
                """
                MATCH (n)
                CALL (n) {
                    DETACH DELETE n
                } IN TRANSACTIONS OF 1000 ROWS
                """
            ).consume()
        results.record("Clear existing data", True)
    except Exception as exc:
        results.record("Clear existing data", False, str(exc))


def create_constraints(driver: Any, cfg: Config, results: ValidationResults) -> None:
    print("\n--- Section 2: Create Constraints ---")
    constraints = [unique_constraint(load) for load in node_loads()]
    try:
        with driver.session(database=cfg.database) as session:
            for query in constraints:
                session.run(query).consume()
            session.run("CALL db.awaitIndexes()").consume()
        results.record("Create constraints", True, f"{len(constraints)} constraints")
    except Exception as exc:
        results.record("Create constraints", False, str(exc))


def unique_constraint(load: NodeLoad) -> str:
    constraint_name = camel_to_snake(f"{load.label}_{load.id_property}_unique")
    return (
        f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
        f"FOR (n:{load.label}) REQUIRE n.{load.id_property} IS UNIQUE"
    )


def camel_to_snake(value: str) -> str:
    chars: list[str] = []
    previous = ""
    for char in value:
        if char.isupper() and previous and (previous.islower() or previous.isdigit()):
            chars.append("_")
        chars.append(char.lower())
        previous = char
    return "".join(chars)


def load_nodes(driver: Any, cfg: Config, results: ValidationResults) -> None:
    print("\n--- Section 3: Load Nodes ---")
    try:
        with driver.session(database=cfg.database) as session:
            for load in node_loads():
                rows = csv_rows(cfg.data_dir / load.csv_name)
                session.run(load.query, rows=rows).consume()
                count = query_count(
                    session,
                    f"MATCH (n:{load.label}) RETURN count(n) AS cnt",
                )
                results.record(
                    f"Load {load.label}",
                    count == load.expected,
                    f"{count} nodes",
                )
    except Exception as exc:
        results.record("Load nodes", False, str(exc))


def load_relationships(driver: Any, cfg: Config, results: ValidationResults) -> None:
    print("\n--- Section 4: Load Relationships ---")
    try:
        with driver.session(database=cfg.database) as session:
            for load in relationship_loads():
                rows = csv_rows(cfg.data_dir / load.csv_name)
                query = f"UNWIND $rows AS row {load.match_create}"
                session.run(query, rows=rows).consume()
                count = query_count(
                    session,
                    f"MATCH ()-[r:{load.rel_type}]->() RETURN count(r) AS cnt",
                )
                results.record(
                    f"Load {load.rel_type}",
                    count == len(rows),
                    f"{count} relationships",
                )
    except Exception as exc:
        results.record("Load relationships", False, str(exc))


def verify_counts(driver: Any, cfg: Config, results: ValidationResults) -> None:
    print("\n--- Section 5: Verify Counts ---")
    try:
        with driver.session(database=cfg.database) as session:
            for load in node_loads():
                count = query_count(
                    session,
                    f"MATCH (n:{load.label}) RETURN count(n) AS cnt",
                )
                results.record(
                    f"Verify {load.label}",
                    count == load.expected,
                    f"{count} nodes",
                )
    except Exception as exc:
        results.record("Verify counts", False, str(exc))


def query_count(session: Any, query: str) -> int:
    record = session.run(query).single(strict=True)
    return int(record["cnt"])


def csv_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            normalized = {}
            for key, value in row.items():
                if key.startswith(":ID("):
                    normalized["id"] = value
                elif key.startswith(":START_ID("):
                    normalized["start_id"] = value
                elif key.startswith(":END_ID("):
                    normalized["end_id"] = value
                elif key == ":TYPE":
                    normalized["type"] = value
                else:
                    normalized[key] = value
            rows.append(normalized)
    return rows


def node_loads() -> list[NodeLoad]:
    return [
        NodeLoad(
            "Aircraft",
            "nodes_aircraft.csv",
            """
            UNWIND $rows AS row
            CREATE (:Aircraft {aircraftId: row.id, tail_number: row.tail_number,
                               icao24: row.icao24, model: row.model,
                               manufacturer: row.manufacturer, operator: row.operator})
            """,
            "aircraftId",
            20,
        ),
        NodeLoad(
            "Airport",
            "nodes_airports.csv",
            """
            UNWIND $rows AS row
            CREATE (:Airport {airportId: row.id, name: row.name, city: row.city,
                              country: row.country, iata: row.iata, icao: row.icao,
                              lat: toFloat(row.lat), lon: toFloat(row.lon)})
            """,
            "airportId",
            12,
        ),
        NodeLoad(
            "System",
            "nodes_systems.csv",
            """
            UNWIND $rows AS row
            CREATE (:System {systemId: row.id, aircraftId: row.aircraft_id,
                             type: row.type, name: row.name})
            """,
            "systemId",
            80,
        ),
        NodeLoad(
            "Component",
            "nodes_components.csv",
            """
            UNWIND $rows AS row
            CREATE (:Component {componentId: row.id, systemId: row.system_id,
                                type: row.type, name: row.name})
            """,
            "componentId",
            320,
        ),
        NodeLoad(
            "Sensor",
            "nodes_sensors.csv",
            """
            UNWIND $rows AS row
            CREATE (:Sensor {sensorId: row.id, systemId: row.system_id,
                             type: row.type, name: row.name, unit: row.unit})
            """,
            "sensorId",
            160,
        ),
        NodeLoad(
            "Flight",
            "nodes_flights.csv",
            """
            UNWIND $rows AS row
            CREATE (:Flight {flightId: row.id, flight_number: row.flight_number,
                             aircraftId: row.aircraft_id, operator: row.operator,
                             origin: row.origin, destination: row.destination,
                             scheduled_departure: row.scheduled_departure,
                             scheduled_arrival: row.scheduled_arrival})
            """,
            "flightId",
            800,
        ),
        NodeLoad(
            "MaintenanceEvent",
            "nodes_maintenance.csv",
            """
            UNWIND $rows AS row
            CREATE (:MaintenanceEvent {
                eventId: row.id, componentId: row.component_id,
                systemId: row.system_id, aircraftId: row.aircraft_id,
                fault: row.fault, severity: row.severity,
                reported_at: row.reported_at,
                corrective_action: row.corrective_action
            })
            """,
            "eventId",
            300,
        ),
        NodeLoad(
            "Delay",
            "nodes_delays.csv",
            """
            UNWIND $rows AS row
            CREATE (:Delay {delayId: row.id, flightId: row.flight_id,
                            cause: row.cause, minutes: toInteger(row.minutes)})
            """,
            "delayId",
            514,
        ),
    ]


def relationship_loads() -> list[RelationshipLoad]:
    return [
        RelationshipLoad(
            "HAS_SYSTEM",
            "rels_aircraft_system.csv",
            "MATCH (a:Aircraft {aircraftId: row.start_id}) "
            "MATCH (s:System {systemId: row.end_id}) "
            "CREATE (a)-[:HAS_SYSTEM]->(s)",
        ),
        RelationshipLoad(
            "HAS_COMPONENT",
            "rels_system_component.csv",
            "MATCH (s:System {systemId: row.start_id}) "
            "MATCH (c:Component {componentId: row.end_id}) "
            "CREATE (s)-[:HAS_COMPONENT]->(c)",
        ),
        RelationshipLoad(
            "HAS_SENSOR",
            "rels_system_sensor.csv",
            "MATCH (s:System {systemId: row.start_id}) "
            "MATCH (sn:Sensor {sensorId: row.end_id}) "
            "CREATE (s)-[:HAS_SENSOR]->(sn)",
        ),
        RelationshipLoad(
            "OPERATES_FLIGHT",
            "rels_aircraft_flight.csv",
            "MATCH (a:Aircraft {aircraftId: row.start_id}) "
            "MATCH (f:Flight {flightId: row.end_id}) "
            "CREATE (a)-[:OPERATES_FLIGHT]->(f)",
        ),
        RelationshipLoad(
            "DEPARTS_FROM",
            "rels_flight_departure.csv",
            "MATCH (f:Flight {flightId: row.start_id}) "
            "MATCH (a:Airport {airportId: row.end_id}) "
            "CREATE (f)-[:DEPARTS_FROM]->(a)",
        ),
        RelationshipLoad(
            "ARRIVES_AT",
            "rels_flight_arrival.csv",
            "MATCH (f:Flight {flightId: row.start_id}) "
            "MATCH (a:Airport {airportId: row.end_id}) "
            "CREATE (f)-[:ARRIVES_AT]->(a)",
        ),
        RelationshipLoad(
            "HAS_DELAY",
            "rels_flight_delay.csv",
            "MATCH (f:Flight {flightId: row.start_id}) "
            "MATCH (d:Delay {delayId: row.end_id}) "
            "CREATE (f)-[:HAS_DELAY]->(d)",
        ),
        RelationshipLoad(
            "HAS_EVENT",
            "rels_component_event.csv",
            "MATCH (c:Component {componentId: row.start_id}) "
            "MATCH (m:MaintenanceEvent {eventId: row.end_id}) "
            "CREATE (c)-[:HAS_EVENT]->(m)",
        ),
    ]


if __name__ == "__main__":
    sys.exit(main())
