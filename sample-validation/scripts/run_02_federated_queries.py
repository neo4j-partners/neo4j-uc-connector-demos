"""Run federated queries joining Delta sensor data with Neo4j graph data via UC JDBC.

Each query combines:
  - Delta: sensor_readings table (time-series sensor telemetry)
  - Neo4j: graph topology and operational data via UC JDBC

Prerequisites:
  - run_00_load_graph.py must have run (loads Neo4j graph)
  - run_01_connect_test.py must have run (creates UC JDBC connection and sensor_readings table)

Queries:
  1. Sensor health by aircraft — Neo4j topology (Python driver) + Delta sensor stats
  2. Maintenance severity and sensor health — Neo4j JDBC aggregates + Delta sensor averages
  3. Flight delay analysis by operator — Neo4j JDBC aggregates

Usage (via runner):
    uv run python -m cli upload run_02_federated_queries.py
    uv run python -m cli submit run_02_federated_queries.py
"""

import sys

from data_utils import inject_params, get_config, read_neo4j, ValidationResults


def main():
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    results = ValidationResults()
    fqn = cfg["fqn"]

    print("=" * 60)
    print("run_02_federated_queries: Federated Queries")
    print("=" * 60)
    print(f"  Neo4j URI: {cfg['neo4j_uri']}")
    print(f"  Schema:    {fqn}")
    print("")

    # ------------------------------------------------------------------
    # Query 1: Sensor health by aircraft
    # Neo4j JDBC: aircraft → system → sensor topology via NATURAL JOIN
    # Delta: aggregate reading statistics per sensor
    # ------------------------------------------------------------------
    print("--- Query 1: Sensor health by aircraft ---")
    try:
        sensor_topology = read_neo4j(spark, cfg,
            custom_schema="aircraftId STRING, model STRING, systemType STRING, sensorId STRING",
            query="""SELECT a.aircraftId AS aircraftId, a.model AS model,
                            sys.type AS systemType, s.sensorId AS sensorId
                     FROM Aircraft a
                     NATURAL JOIN HAS_SYSTEM r1
                     NATURAL JOIN System sys
                     NATURAL JOIN HAS_SENSOR r2
                     NATURAL JOIN Sensor s""",
        )

        sensor_stats = spark.sql(f"""
            SELECT sensorId,
                   COUNT(*) AS reading_count,
                   ROUND(AVG(value), 2) AS avg_value,
                   ROUND(MIN(value), 2) AS min_value,
                   ROUND(MAX(value), 2) AS max_value
            FROM {fqn}.sensor_readings
            GROUP BY sensorId
        """)

        result = (
            sensor_topology
            .join(sensor_stats, sensor_topology.sensorId == sensor_stats.sensorId, "left")
            .select("aircraftId", "model", "systemType", "sensorId", "reading_count", "avg_value")
        )
        result.orderBy("aircraftId", "systemType").show(10, truncate=False)

        row_count = result.count()
        results.record("Query 1: sensor health by aircraft", row_count == 160,
                       f"{row_count} sensor+aircraft rows")

    except Exception as e:
        results.record("Query 1: sensor health by aircraft", False, str(e))

    # ------------------------------------------------------------------
    # Query 2: Maintenance severity and sensor health
    # Neo4j JDBC: maintenance event counts by severity; counts per aircraft
    # Delta: average sensor readings per aircraft
    # ------------------------------------------------------------------
    print("\n--- Query 2: Maintenance severity and sensor health ---")
    try:
        # Neo4j: maintenance counts by severity
        df_severity = read_neo4j(spark, cfg,
            custom_schema="severity STRING, event_count LONG",
            query="SELECT severity, COUNT(*) AS event_count FROM MaintenanceEvent GROUP BY severity",
        )
        df_severity.orderBy("event_count", ascending=False).show(truncate=False)
        severity_count = df_severity.count()

        # Neo4j: maintenance counts per aircraft
        df_by_aircraft = read_neo4j(spark, cfg,
            custom_schema="aircraftId STRING, maint_count LONG",
            query="SELECT aircraftId, COUNT(*) AS maint_count FROM MaintenanceEvent GROUP BY aircraftId",
        )

        # Delta: average sensor value per aircraft (derived from sensorId prefix)
        aircraft_sensor_health = spark.sql(f"""
            SELECT REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1) AS aircraftId,
                   ROUND(AVG(value), 2) AS avg_sensor_reading
            FROM {fqn}.sensor_readings
            GROUP BY REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1)
        """)

        result = df_by_aircraft.join(aircraft_sensor_health, "aircraftId", "left")
        result.orderBy("maint_count", ascending=False).show(10, truncate=False)

        results.record("Query 2: maintenance severity", severity_count > 0,
                       f"{severity_count} severity levels")
        results.record("Query 2: maintenance + sensor join", result.count() == 20,
                       f"{result.count()} aircraft")

    except Exception as e:
        results.record("Query 2: maintenance + sensor health", False, str(e))

    # ------------------------------------------------------------------
    # Query 3: Flight delay analysis by operator
    # Neo4j JDBC: flight counts per operator; delay counts and average minutes per cause
    # ------------------------------------------------------------------
    print("\n--- Query 3: Flight delay analysis ---")
    try:
        df_flights = read_neo4j(spark, cfg,
            custom_schema="operator STRING, flight_count LONG",
            query="SELECT operator, COUNT(*) AS flight_count FROM Flight GROUP BY operator",
        )
        df_flights.orderBy("flight_count", ascending=False).show(truncate=False)
        operator_count = df_flights.count()

        df_delays = read_neo4j(spark, cfg,
            custom_schema="cause STRING, delay_count LONG, avg_minutes DOUBLE",
            query="SELECT cause, COUNT(*) AS delay_count, AVG(minutes) AS avg_minutes FROM Delay GROUP BY cause",
        )
        df_delays.orderBy("delay_count", ascending=False).show(truncate=False)
        cause_count = df_delays.count()

        results.record("Query 3: flights by operator", operator_count > 0,
                       f"{operator_count} operators")
        results.record("Query 3: delays by cause", cause_count > 0,
                       f"{cause_count} delay causes")

    except Exception as e:
        results.record("Query 3: flight delay analysis", False, str(e))

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
