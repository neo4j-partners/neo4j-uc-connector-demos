"""Materialize Neo4j graph data as Delta tables and run SQL validation queries.

Reads all node types from Neo4j via the Python driver and writes them as
Delta tables in the Unity Catalog schema. Also materializes aircraft-system
relationships via UC JDBC NATURAL JOIN. Then runs SQL validation tests and
federated queries that join the materialized graph tables with the Delta
sensor_readings table loaded by run_02.

Sections:
  1. Verify data sources (Delta sensor_readings, Neo4j node counts via JDBC)
  2. Materialize Neo4j nodes and relationships as Delta tables
  3. SQL validation tests
  4. Federated queries on materialized graph + sensor readings

Prerequisites:
  - run_01_connect_test.py must have run (creates UC JDBC connection)
  - run_02_federated_queries.py must have run (loads Neo4j graph and sensor_readings)

Usage (via runner):
    uv run python -m cli upload run_03_materialized_tables.py
    uv run python -m cli submit run_03_materialized_tables.py
"""

import sys
import time

from data_utils import inject_params, get_config, read_neo4j, ValidationResults


def main():
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType
    spark = SparkSession.builder.getOrCreate()

    results = ValidationResults()
    fqn = cfg["fqn"]

    print("=" * 60)
    print("run_03_materialized_tables: Materialize + SQL Validation")
    print("=" * 60)
    print(f"  Neo4j URI:   {cfg['neo4j_uri']}")
    print(f"  Schema:      {fqn}")
    print("")

    # ------------------------------------------------------------------
    # Section 1: Verify data sources
    # ------------------------------------------------------------------
    print("--- Section 1: Verify Data Sources ---")
    try:
        reading_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}.sensor_readings").collect()[0]["cnt"]
        results.record("Delta: sensor_readings", reading_count == 172800, f"{reading_count:,} rows")

        for label, expected in [
            ("Aircraft", 20), ("Airport", 12), ("System", 80), ("Component", 320),
            ("Sensor", 160), ("Flight", 800), ("MaintenanceEvent", 300), ("Delay", 514),
        ]:
            df = read_neo4j(spark, cfg,
                custom_schema="cnt LONG",
                query=f"SELECT COUNT(*) AS cnt FROM {label}",
            )
            count = df.collect()[0]["cnt"]
            results.record(f"Neo4j JDBC: {label}", count == expected, f"{count} nodes")

    except Exception as e:
        results.record("Verify data sources", False, str(e))
        results.summary()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Section 2: Materialize Neo4j data as Delta tables
    # ------------------------------------------------------------------
    print("\n--- Section 2: Materialize Neo4j Data ---")

    def materialize(table_name: str, query: str, custom_schema: str, expected: int) -> None:
        df = read_neo4j(spark, cfg, custom_schema=custom_schema, query=query)
        # Cast all columns to STRING to avoid JDBC CHAR(0) Delta write errors
        for c in df.columns:
            df = df.withColumn(c, df[c].cast(StringType()))
        t0 = time.time()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.{table_name}")
        ms = (time.time() - t0) * 1000
        cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}.{table_name}").collect()[0]["cnt"]
        results.record(f"Materialized: {table_name}", cnt == expected,
                       f"{cnt} rows, {ms:.0f}ms")

    try:
        materialize("neo4j_aircraft",
            "SELECT aircraftId, tail_number, icao24, model, manufacturer, operator FROM Aircraft",
            "aircraftId STRING, tail_number STRING, icao24 STRING, model STRING, manufacturer STRING, operator STRING",
            20)

        materialize("neo4j_airports",
            "SELECT airportId, name, city, country, iata, icao FROM Airport",
            "airportId STRING, name STRING, city STRING, country STRING, iata STRING, icao STRING",
            12)

        materialize("neo4j_systems",
            "SELECT systemId, aircraftId, type, name FROM System",
            "systemId STRING, aircraftId STRING, type STRING, name STRING",
            80)

        materialize("neo4j_sensors",
            "SELECT sensorId, systemId, type, name, unit FROM Sensor",
            "sensorId STRING, systemId STRING, type STRING, name STRING, unit STRING",
            160)

        materialize("neo4j_components",
            "SELECT componentId, systemId, type, name FROM Component",
            "componentId STRING, systemId STRING, type STRING, name STRING",
            320)

        materialize("neo4j_maintenance_events",
            "SELECT eventId, componentId, systemId, aircraftId, fault, severity, reported_at, corrective_action FROM MaintenanceEvent",
            "eventId STRING, componentId STRING, systemId STRING, aircraftId STRING, fault STRING, severity STRING, reported_at STRING, corrective_action STRING",
            300)

        materialize("neo4j_flights",
            "SELECT flightId, flight_number, aircraftId, operator, origin, destination, scheduled_departure, scheduled_arrival FROM Flight",
            "flightId STRING, flight_number STRING, aircraftId STRING, operator STRING, origin STRING, destination STRING, scheduled_departure STRING, scheduled_arrival STRING",
            800)

        materialize("neo4j_delays",
            "SELECT delayId, flightId, cause, CAST(minutes AS STRING) AS minutes FROM Delay",
            "delayId STRING, flightId STRING, cause STRING, minutes STRING",
            514)

    except Exception as e:
        results.record("Materialize node tables", False, str(e))
        results.summary()
        sys.exit(1)

    # Relationship table via UC JDBC NATURAL JOIN
    try:
        df = read_neo4j(spark, cfg,
            custom_schema="aircraftId STRING, model STRING, systemId STRING, systemType STRING, systemName STRING, cnt LONG",
            query="""SELECT a.aircraftId AS aircraftId, a.model AS model,
                            s.systemId AS systemId, s.type AS systemType, s.name AS systemName,
                            COUNT(*) AS cnt
                     FROM Aircraft a NATURAL JOIN HAS_SYSTEM rel NATURAL JOIN System s
                     GROUP BY a.aircraftId, a.model, s.systemId, s.type, s.name""",
        ).select("aircraftId", "model", "systemId", "systemType", "systemName")

        for c in df.columns:
            df = df.withColumn(c, df[c].cast(StringType()))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fqn}.neo4j_aircraft_systems")
        cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fqn}.neo4j_aircraft_systems").collect()[0]["cnt"]
        results.record("Materialized: neo4j_aircraft_systems", cnt == 80, f"{cnt} rows")

    except Exception as e:
        results.record("Materialized: neo4j_aircraft_systems", False, str(e)[:200])

    # ------------------------------------------------------------------
    # Section 3: SQL Validation Tests
    # ------------------------------------------------------------------
    print("\n--- Section 3: SQL Validation Tests ---")

    # TEST 1: GROUP BY — maintenance events by severity
    print("\n  TEST 1: GROUP BY — maintenance events by severity")
    try:
        df = spark.sql(f"""
            SELECT severity, COUNT(*) AS event_count
            FROM {fqn}.neo4j_maintenance_events
            GROUP BY severity
            ORDER BY event_count DESC
        """)
        df.show(truncate=False)
        row_count = df.count()
        results.record("SQL: GROUP BY severity", row_count > 0, f"{row_count} severity levels")
    except Exception as e:
        results.record("SQL: GROUP BY severity", False, str(e))

    # TEST 2: WHERE + ORDER BY — critical maintenance events
    print("\n  TEST 2: WHERE + ORDER BY — critical maintenance events")
    try:
        df = spark.sql(f"""
            SELECT eventId, aircraftId, fault, reported_at
            FROM {fqn}.neo4j_maintenance_events
            WHERE severity = 'CRITICAL'
            ORDER BY reported_at DESC
        """)
        df.show(10, truncate=False)
        row_count = df.count()
        results.record("SQL: WHERE + ORDER BY", row_count > 0, f"{row_count} critical events")
    except Exception as e:
        results.record("SQL: WHERE + ORDER BY", False, str(e))

    # TEST 3: Aggregations + JOIN — sensor count per aircraft model
    print("\n  TEST 3: Aggregations + JOIN — sensor count per aircraft model")
    try:
        df = spark.sql(f"""
            SELECT a.model, a.manufacturer,
                   COUNT(DISTINCT s.sensorId) AS sensor_count,
                   COUNT(DISTINCT sys.systemId) AS system_count
            FROM {fqn}.neo4j_aircraft a
            JOIN {fqn}.neo4j_systems sys ON a.aircraftId = sys.aircraftId
            JOIN {fqn}.neo4j_sensors s ON sys.systemId = s.systemId
            GROUP BY a.model, a.manufacturer
            ORDER BY sensor_count DESC
        """)
        df.show(truncate=False)
        row_count = df.count()
        results.record("SQL: Aggregations + JOIN", row_count > 0, f"{row_count} aircraft models")
    except Exception as e:
        results.record("SQL: Aggregations + JOIN", False, str(e))

    # TEST 4: DISTINCT — unique aircraft manufacturers
    print("\n  TEST 4: DISTINCT — unique aircraft manufacturers and models")
    try:
        df = spark.sql(f"""
            SELECT DISTINCT manufacturer, model
            FROM {fqn}.neo4j_aircraft
            ORDER BY manufacturer, model
        """)
        df.show(truncate=False)
        count = df.count()
        results.record("SQL: DISTINCT", count > 0, f"{count} manufacturer/model combinations")
    except Exception as e:
        results.record("SQL: DISTINCT", False, str(e))

    # ------------------------------------------------------------------
    # Section 4: Federated Queries
    # ------------------------------------------------------------------
    print("\n--- Section 4: Federated Queries ---")

    # Federated Query 1: Aircraft health overview
    # Joins materialized graph tables with Delta sensor_readings
    print("\n  Federated Query 1: Aircraft health overview")
    try:
        df = spark.sql(f"""
            SELECT a.aircraftId, a.model, a.operator,
                   COUNT(DISTINCT m.eventId) AS maintenance_events,
                   SUM(CASE WHEN m.severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_events,
                   COUNT(DISTINCT s.sensorId) AS sensor_count,
                   ROUND(AVG(r.value), 2) AS avg_sensor_reading
            FROM {fqn}.neo4j_aircraft a
            LEFT JOIN {fqn}.neo4j_maintenance_events m ON a.aircraftId = m.aircraftId
            LEFT JOIN {fqn}.neo4j_sensors s ON s.sensorId LIKE CONCAT(a.aircraftId, '-%')
            LEFT JOIN {fqn}.sensor_readings r ON r.sensorId = s.sensorId
            GROUP BY a.aircraftId, a.model, a.operator
            ORDER BY critical_events DESC, maintenance_events DESC
        """)
        df.show(10, truncate=False)
        row_count = df.count()
        results.record("Federated Q1: aircraft health", row_count == 20, f"{row_count} aircraft")
    except Exception as e:
        results.record("Federated Q1: aircraft health", False, str(e))

    # Federated Query 2: Route analysis — busiest airport pairs
    print("\n  Federated Query 2: Route analysis")
    try:
        df = spark.sql(f"""
            SELECT dep.city AS origin_city, dep.iata AS origin,
                   arr.city AS destination_city, arr.iata AS destination,
                   COUNT(*) AS flight_count
            FROM {fqn}.neo4j_flights f
            JOIN {fqn}.neo4j_airports dep ON f.origin = dep.iata
            JOIN {fqn}.neo4j_airports arr ON f.destination = arr.iata
            GROUP BY dep.city, dep.iata, arr.city, arr.iata
            ORDER BY flight_count DESC
            LIMIT 10
        """)
        df.show(truncate=False)
        row_count = df.count()
        results.record("Federated Q2: route analysis", row_count > 0, f"{row_count} routes")
    except Exception as e:
        results.record("Federated Q2: route analysis", False, str(e))

    # Federated Query 3: Sensor health by system type (Delta + Neo4j)
    print("\n  Federated Query 3: Sensor health by system type")
    try:
        df = spark.sql(f"""
            SELECT sys.type AS system_type,
                   COUNT(DISTINCT s.sensorId) AS sensor_count,
                   COUNT(r.readingId) AS total_readings,
                   ROUND(AVG(r.value), 2) AS avg_reading,
                   ROUND(STDDEV(r.value), 2) AS stddev_reading
            FROM {fqn}.neo4j_sensors s
            JOIN {fqn}.neo4j_systems sys ON s.systemId = sys.systemId
            JOIN {fqn}.sensor_readings r ON r.sensorId = s.sensorId
            GROUP BY sys.type
            ORDER BY total_readings DESC
        """)
        df.show(truncate=False)
        row_count = df.count()
        results.record("Federated Q3: sensor health by system", row_count > 0, f"{row_count} system types")
    except Exception as e:
        results.record("Federated Q3: sensor health by system", False, str(e))

    # Federated Query 4: Delay analysis with airport and operator info
    print("\n  Federated Query 4: Delay analysis by airport and cause")
    try:
        df = spark.sql(f"""
            SELECT dep.city AS departure_city, dep.iata, d.cause,
                   COUNT(*) AS delay_count,
                   ROUND(AVG(CAST(d.minutes AS INT)), 1) AS avg_delay_minutes
            FROM {fqn}.neo4j_delays d
            JOIN {fqn}.neo4j_flights f ON d.flightId = f.flightId
            JOIN {fqn}.neo4j_airports dep ON f.origin = dep.iata
            GROUP BY dep.city, dep.iata, d.cause
            ORDER BY delay_count DESC
            LIMIT 15
        """)
        df.show(truncate=False)
        row_count = df.count()
        results.record("Federated Q4: delay by airport", row_count > 0, f"{row_count} rows")
    except Exception as e:
        results.record("Federated Q4: delay by airport", False, str(e))

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
