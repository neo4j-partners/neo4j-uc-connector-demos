"""Load the full aircraft digital twin graph into Neo4j.

Clears all existing data, creates indexes, loads all node types and
relationship types from CSV files in the UC Volume, and verifies node counts.

This script is a prerequisite for all subsequent validation scripts.
Run it once before running any of the query validation scripts.

Sections:
  1. Clear existing Neo4j data
  2. Create indexes
  3. Load all node types from CSV
  4. Load all relationship types from CSV
  5. Verify node and relationship counts

Usage (via runner):
    uv run python -m cli upload run_00_load_graph.py
    uv run python -m cli submit run_00_load_graph.py
"""

import sys

from data_utils import inject_params, get_config, get_neo4j_driver, csv_rows, ValidationResults


def main():
    inject_params()
    cfg = get_config()

    results = ValidationResults()
    vol = cfg["volume_path"]

    print("=" * 60)
    print("run_00_load_graph: Aircraft Digital Twin Graph Setup")
    print("=" * 60)
    print(f"  Neo4j URI: {cfg['neo4j_uri']}")
    print(f"  Volume:    {vol}")
    print("")

    driver = get_neo4j_driver(cfg)

    # ------------------------------------------------------------------
    # Section 1: Clear existing data
    # ------------------------------------------------------------------
    print("--- Section 1: Clear Existing Data ---")
    try:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        results.record("Clear existing data", True)
    except Exception as e:
        results.record("Clear existing data", False, str(e))
        results.summary()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Section 2: Create indexes
    # ------------------------------------------------------------------
    print("\n--- Section 2: Create Indexes ---")
    indexes = [
        ("Aircraft",         "aircraftId",  "CREATE INDEX aircraft_id IF NOT EXISTS FOR (n:Aircraft) ON (n.aircraftId)"),
        ("Airport",          "airportId",   "CREATE INDEX airport_id IF NOT EXISTS FOR (n:Airport) ON (n.airportId)"),
        ("System",           "systemId",    "CREATE INDEX system_id IF NOT EXISTS FOR (n:System) ON (n.systemId)"),
        ("Component",        "componentId", "CREATE INDEX component_id IF NOT EXISTS FOR (n:Component) ON (n.componentId)"),
        ("Sensor",           "sensorId",    "CREATE INDEX sensor_id IF NOT EXISTS FOR (n:Sensor) ON (n.sensorId)"),
        ("Flight",           "flightId",    "CREATE INDEX flight_id IF NOT EXISTS FOR (n:Flight) ON (n.flightId)"),
        ("Delay",            "delayId",     "CREATE INDEX delay_id IF NOT EXISTS FOR (n:Delay) ON (n.delayId)"),
        ("MaintenanceEvent", "eventId",     "CREATE INDEX maint_id IF NOT EXISTS FOR (n:MaintenanceEvent) ON (n.eventId)"),
    ]
    try:
        with driver.session() as session:
            for label, prop, query in indexes:
                session.run(query)
        results.record("Create indexes", True, f"{len(indexes)} indexes")
    except Exception as e:
        results.record("Create indexes", False, str(e))
        results.summary()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Section 3: Load node types
    # ------------------------------------------------------------------
    print("\n--- Section 3: Load Nodes ---")
    nodes = [
        ("Aircraft", f"{vol}/nodes_aircraft.csv", """
            UNWIND $rows AS row
            CREATE (:Aircraft {aircraftId: row.id, tail_number: row.tail_number,
                               icao24: row.icao24, model: row.model,
                               manufacturer: row.manufacturer, operator: row.operator})
        """, 20),
        ("Airport", f"{vol}/nodes_airports.csv", """
            UNWIND $rows AS row
            CREATE (:Airport {airportId: row.id, name: row.name, city: row.city,
                              country: row.country, iata: row.iata, icao: row.icao,
                              lat: toFloat(row.lat), lon: toFloat(row.lon)})
        """, 12),
        ("System", f"{vol}/nodes_systems.csv", """
            UNWIND $rows AS row
            CREATE (:System {systemId: row.id, aircraftId: row.aircraft_id,
                             type: row.type, name: row.name})
        """, 80),
        ("Component", f"{vol}/nodes_components.csv", """
            UNWIND $rows AS row
            CREATE (:Component {componentId: row.id, systemId: row.system_id,
                                type: row.type, name: row.name})
        """, 320),
        ("Sensor", f"{vol}/nodes_sensors.csv", """
            UNWIND $rows AS row
            CREATE (:Sensor {sensorId: row.id, systemId: row.system_id,
                             type: row.type, name: row.name, unit: row.unit})
        """, 160),
        ("Flight", f"{vol}/nodes_flights.csv", """
            UNWIND $rows AS row
            CREATE (:Flight {flightId: row.id, flight_number: row.flight_number,
                             aircraftId: row.aircraft_id, operator: row.operator,
                             origin: row.origin, destination: row.destination,
                             scheduled_departure: row.scheduled_departure,
                             scheduled_arrival: row.scheduled_arrival})
        """, 800),
        ("MaintenanceEvent", f"{vol}/nodes_maintenance.csv", """
            UNWIND $rows AS row
            CREATE (:MaintenanceEvent {eventId: row.id, componentId: row.component_id,
                                       systemId: row.system_id, aircraftId: row.aircraft_id,
                                       fault: row.fault, severity: row.severity,
                                       reported_at: row.reported_at,
                                       corrective_action: row.corrective_action})
        """, 300),
        ("Delay", f"{vol}/nodes_delays.csv", """
            UNWIND $rows AS row
            CREATE (:Delay {delayId: row.id, flightId: row.flight_id,
                            cause: row.cause, minutes: toInteger(row.minutes)})
        """, 514),
    ]

    try:
        with driver.session() as session:
            for label, csv_path, query, expected in nodes:
                session.run(query, rows=csv_rows(csv_path))
                count = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt").single()["cnt"]
                results.record(f"Load {label}", count == expected, f"{count} nodes")
    except Exception as e:
        results.record("Load nodes", False, str(e))
        results.summary()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Section 4: Load relationship types
    # ------------------------------------------------------------------
    print("\n--- Section 4: Load Relationships ---")
    relationships = [
        ("HAS_SYSTEM",       f"{vol}/rels_aircraft_system.csv",
         "MATCH (a:Aircraft {aircraftId: row.start_id}) MATCH (s:System {systemId: row.end_id}) CREATE (a)-[:HAS_SYSTEM]->(s)"),
        ("HAS_COMPONENT",    f"{vol}/rels_system_component.csv",
         "MATCH (s:System {systemId: row.start_id}) MATCH (c:Component {componentId: row.end_id}) CREATE (s)-[:HAS_COMPONENT]->(c)"),
        ("HAS_SENSOR",       f"{vol}/rels_system_sensor.csv",
         "MATCH (s:System {systemId: row.start_id}) MATCH (sn:Sensor {sensorId: row.end_id}) CREATE (s)-[:HAS_SENSOR]->(sn)"),
        ("OPERATES_FLIGHT",  f"{vol}/rels_aircraft_flight.csv",
         "MATCH (a:Aircraft {aircraftId: row.start_id}) MATCH (f:Flight {flightId: row.end_id}) CREATE (a)-[:OPERATES_FLIGHT]->(f)"),
        ("DEPARTS_FROM",     f"{vol}/rels_flight_departure.csv",
         "MATCH (f:Flight {flightId: row.start_id}) MATCH (a:Airport {airportId: row.end_id}) CREATE (f)-[:DEPARTS_FROM]->(a)"),
        ("ARRIVES_AT",       f"{vol}/rels_flight_arrival.csv",
         "MATCH (f:Flight {flightId: row.start_id}) MATCH (a:Airport {airportId: row.end_id}) CREATE (f)-[:ARRIVES_AT]->(a)"),
        ("HAS_DELAY",        f"{vol}/rels_flight_delay.csv",
         "MATCH (f:Flight {flightId: row.start_id}) MATCH (d:Delay {delayId: row.end_id}) CREATE (f)-[:HAS_DELAY]->(d)"),
        ("HAS_EVENT",        f"{vol}/rels_component_event.csv",
         "MATCH (c:Component {componentId: row.start_id}) MATCH (m:MaintenanceEvent {eventId: row.end_id}) CREATE (c)-[:HAS_EVENT]->(m)"),
    ]

    try:
        with driver.session() as session:
            for rel_type, csv_path, match_create in relationships:
                query = f"UNWIND $rows AS row {match_create}"
                session.run(query, rows=csv_rows(csv_path))
                results.record(f"Load {rel_type}", True)
    except Exception as e:
        results.record("Load relationships", False, str(e))
        results.summary()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Section 5: Verify final counts
    # ------------------------------------------------------------------
    print("\n--- Section 5: Verify Counts ---")
    expected_counts = {
        "Aircraft": 20, "Airport": 12, "System": 80, "Component": 320,
        "Sensor": 160, "Flight": 800, "MaintenanceEvent": 300, "Delay": 514,
    }
    try:
        with driver.session() as session:
            for label, expected in expected_counts.items():
                count = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt").single()["cnt"]
                results.record(f"Verify {label}", count == expected, f"{count} nodes")
    except Exception as e:
        results.record("Verify counts", False, str(e))

    driver.close()

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
