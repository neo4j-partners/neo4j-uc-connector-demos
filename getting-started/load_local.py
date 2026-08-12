# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "neo4j>=5.0",
#     "python-dotenv>=1.0.0",
# ]
# ///
"""Load the aircraft digital twin graph into Neo4j from local CSV files.

Standalone local counterpart to validation/scripts/run_00_load_graph.py. Reads
Neo4j credentials from the repo-root .env and loads the CSVs in
./data/aircraft_digital_twin_data/ directly via the Neo4j driver. No Databricks,
UC Volume, or Spark required.

Clears all existing data, creates indexes, loads every node and relationship
type, then verifies the node counts.

Usage (from the repo root or getting-started/):
    uv run getting-started/load_local.py

Required .env keys:
    NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DATABASE
"""

import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data" / "aircraft_digital_twin_data"
ENV_PATH = SCRIPT_DIR.parent / ".env"


def csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a Neo4j import CSV and normalize column names.

    Converts Neo4j import column prefixes to plain names:
      :ID(Label)       -> id
      :START_ID(Label) -> start_id
      :END_ID(Label)   -> end_id
      :TYPE            -> type
    """
    rows: list[dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            norm: dict[str, str] = {}
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


NODES = [
    ("Aircraft", "nodes_aircraft.csv", """
        UNWIND $rows AS row
        CREATE (:Aircraft {aircraftId: row.id, tail_number: row.tail_number,
                           icao24: row.icao24, model: row.model,
                           manufacturer: row.manufacturer, operator: row.operator})
    """, 20),
    ("Airport", "nodes_airports.csv", """
        UNWIND $rows AS row
        CREATE (:Airport {airportId: row.id, name: row.name, city: row.city,
                          country: row.country, iata: row.iata, icao: row.icao,
                          lat: toFloat(row.lat), lon: toFloat(row.lon)})
    """, 12),
    ("System", "nodes_systems.csv", """
        UNWIND $rows AS row
        CREATE (:System {systemId: row.id, aircraftId: row.aircraft_id,
                         type: row.type, name: row.name})
    """, 80),
    ("Component", "nodes_components.csv", """
        UNWIND $rows AS row
        CREATE (:Component {componentId: row.id, systemId: row.system_id,
                            type: row.type, name: row.name})
    """, 320),
    ("Sensor", "nodes_sensors.csv", """
        UNWIND $rows AS row
        CREATE (:Sensor {sensorId: row.id, systemId: row.system_id,
                         type: row.type, name: row.name, unit: row.unit})
    """, 160),
    ("Flight", "nodes_flights.csv", """
        UNWIND $rows AS row
        CREATE (:Flight {flightId: row.id, flight_number: row.flight_number,
                         aircraftId: row.aircraft_id, operator: row.operator,
                         origin: row.origin, destination: row.destination,
                         scheduled_departure: row.scheduled_departure,
                         scheduled_arrival: row.scheduled_arrival})
    """, 800),
    ("MaintenanceEvent", "nodes_maintenance.csv", """
        UNWIND $rows AS row
        CREATE (:MaintenanceEvent {eventId: row.id, componentId: row.component_id,
                                   systemId: row.system_id, aircraftId: row.aircraft_id,
                                   fault: row.fault, severity: row.severity,
                                   reported_at: row.reported_at,
                                   corrective_action: row.corrective_action})
    """, 300),
    ("Delay", "nodes_delays.csv", """
        UNWIND $rows AS row
        CREATE (:Delay {delayId: row.id, flightId: row.flight_id,
                        cause: row.cause, minutes: toInteger(row.minutes)})
    """, 514),
]

RELATIONSHIPS = [
    ("HAS_SYSTEM", "rels_aircraft_system.csv",
     "MATCH (a:Aircraft {aircraftId: row.start_id}) MATCH (s:System {systemId: row.end_id}) CREATE (a)-[:HAS_SYSTEM]->(s)"),
    ("HAS_COMPONENT", "rels_system_component.csv",
     "MATCH (s:System {systemId: row.start_id}) MATCH (c:Component {componentId: row.end_id}) CREATE (s)-[:HAS_COMPONENT]->(c)"),
    ("HAS_SENSOR", "rels_system_sensor.csv",
     "MATCH (s:System {systemId: row.start_id}) MATCH (sn:Sensor {sensorId: row.end_id}) CREATE (s)-[:HAS_SENSOR]->(sn)"),
    ("OPERATES_FLIGHT", "rels_aircraft_flight.csv",
     "MATCH (a:Aircraft {aircraftId: row.start_id}) MATCH (f:Flight {flightId: row.end_id}) CREATE (a)-[:OPERATES_FLIGHT]->(f)"),
    ("DEPARTS_FROM", "rels_flight_departure.csv",
     "MATCH (f:Flight {flightId: row.start_id}) MATCH (a:Airport {airportId: row.end_id}) CREATE (f)-[:DEPARTS_FROM]->(a)"),
    ("ARRIVES_AT", "rels_flight_arrival.csv",
     "MATCH (f:Flight {flightId: row.start_id}) MATCH (a:Airport {airportId: row.end_id}) CREATE (f)-[:ARRIVES_AT]->(a)"),
    ("HAS_DELAY", "rels_flight_delay.csv",
     "MATCH (f:Flight {flightId: row.start_id}) MATCH (d:Delay {delayId: row.end_id}) CREATE (f)-[:HAS_DELAY]->(d)"),
    ("HAS_EVENT", "rels_component_event.csv",
     "MATCH (c:Component {componentId: row.start_id}) MATCH (m:MaintenanceEvent {eventId: row.end_id}) CREATE (c)-[:HAS_EVENT]->(m)"),
]

INDEXES = [
    "CREATE INDEX aircraft_id IF NOT EXISTS FOR (n:Aircraft) ON (n.aircraftId)",
    "CREATE INDEX airport_id IF NOT EXISTS FOR (n:Airport) ON (n.airportId)",
    "CREATE INDEX system_id IF NOT EXISTS FOR (n:System) ON (n.systemId)",
    "CREATE INDEX component_id IF NOT EXISTS FOR (n:Component) ON (n.componentId)",
    "CREATE INDEX sensor_id IF NOT EXISTS FOR (n:Sensor) ON (n.sensorId)",
    "CREATE INDEX flight_id IF NOT EXISTS FOR (n:Flight) ON (n.flightId)",
    "CREATE INDEX delay_id IF NOT EXISTS FOR (n:Delay) ON (n.delayId)",
    "CREATE INDEX maint_id IF NOT EXISTS FOR (n:MaintenanceEvent) ON (n.eventId)",
]


def main() -> None:
    load_dotenv(ENV_PATH)

    uri = os.environ.get("NEO4J_URI", "").strip().rstrip("/")
    username = os.environ.get("NEO4J_USERNAME", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    if not uri:
        sys.exit("NEO4J_URI is not set (expected in repo-root .env)")
    if not password:
        sys.exit("NEO4J_PASSWORD is not set (expected in repo-root .env)")
    if not DATA_DIR.is_dir():
        sys.exit(f"Data directory not found: {DATA_DIR}")

    print("=" * 60)
    print("load_local: Aircraft Digital Twin Graph Setup")
    print("=" * 60)
    print(f"  Neo4j URI: {uri}")
    print(f"  Database:  {database}")
    print(f"  Data dir:  {DATA_DIR}")
    print("")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()

    with driver.session(database=database) as session:
        print("--- Clearing existing data ---")
        session.run("MATCH (n) DETACH DELETE n")

        print("--- Creating indexes ---")
        for query in INDEXES:
            session.run(query)
        print(f"  {len(INDEXES)} indexes")

        print("--- Loading nodes ---")
        for label, filename, query, expected in NODES:
            session.run(query, rows=csv_rows(DATA_DIR / filename))
            count = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt").single()["cnt"]
            flag = "OK" if count == expected else "MISMATCH"
            print(f"  [{flag}] {label}: {count} nodes (expected {expected})")

        print("--- Loading relationships ---")
        for rel_type, filename, match_create in RELATIONSHIPS:
            session.run(f"UNWIND $rows AS row {match_create}", rows=csv_rows(DATA_DIR / filename))
            print(f"  [OK] {rel_type}")

    driver.close()
    print("")
    print("Load complete.")


if __name__ == "__main__":
    main()
