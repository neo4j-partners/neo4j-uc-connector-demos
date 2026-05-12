"""Extra federated regression coverage: Neo4j + Databricks Delta lakehouse.

Verifies Delta lakehouse tables and Neo4j counts, then runs federated queries
combining both sources via remote_query() (UC JDBC) and Spark Connector.

Usage:
    uv run python validate.py run --include-extras
    uv run python validate.py submit run_extra_federated_regression.py
"""

import sys
import time

from data_utils import ValidationResults, get_config, inject_params, remote_query


def main() -> None:
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    vr = ValidationResults()

    print("=" * 60)
    print("validation: 02 Federated Query Patterns")
    print("=" * 60)
    print(f"  Lakehouse: {cfg.lakehouse_catalog}.{cfg.lakehouse_schema}")
    print(f"  Neo4j:     {cfg.neo4j_host}")
    print(f"  UC Conn:   {cfg.uc_connection_name}")
    print("")

    try:
        spark.sql(f"USE CATALOG `{cfg.lakehouse_catalog}`")
        spark.sql(f"USE SCHEMA `{cfg.lakehouse_schema}`")
        vr.record(
            "Set lakehouse catalog/schema",
            True,
            f"{cfg.lakehouse_catalog}.{cfg.lakehouse_schema}",
        )
    except Exception as exc:
        vr.record("Set lakehouse catalog/schema", False, str(exc)[:200])
        if not vr.summary():
            sys.exit(1)
        return

    # ============================================================================
    # Section 1: Verify Delta Lakehouse Tables
    # ============================================================================
    print("--- Verify Delta Lakehouse Tables ---")

    expected_tables = {
        "aircraft": 20,
        "systems": 80,
        "sensors": 160,
        "sensor_readings": 172800,
    }

    expected_columns = {
        "aircraft": {"aircraftId", "tail_number", "icao24", "model", "manufacturer", "operator"},
        "systems": {"systemId", "aircraftId", "type", "name"},
        "sensors": {"sensorId", "systemId", "type", "name", "unit"},
        "sensor_readings": {"readingId", "sensorId", "ts", "value"},
    }

    for table, min_rows in expected_tables.items():
        try:
            cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
            vr.record(f"Delta table: {table}", cnt >= min_rows, f"{cnt:,} rows")
        except Exception as e:
            vr.record(f"Delta table: {table}", False, str(e)[:120])

    for table, expected in expected_columns.items():
        try:
            actual = set(spark.table(table).columns)
            missing = sorted(expected - actual)
            vr.record(
                f"Delta schema: {table}",
                not missing,
                "canonical camelCase columns" if not missing else f"missing: {', '.join(missing)}",
            )
        except Exception as e:
            vr.record(f"Delta schema: {table}", False, str(e)[:120])

    # ============================================================================
    # Section 2: Verify Neo4j via UC JDBC (remote_query)
    # ============================================================================
    print("\n--- Verify Neo4j (UC JDBC) ---")

    neo4j_counts = {
        "Aircraft": ("SELECT COUNT(*) AS cnt FROM Aircraft", 1),
        "MaintenanceEvent": ("SELECT COUNT(*) AS cnt FROM MaintenanceEvent", 1),
        "Flight": ("SELECT COUNT(*) AS cnt FROM Flight", 1),
    }

    for label, (query, min_cnt) in neo4j_counts.items():
        try:
            result = remote_query(spark, cfg, query).collect()
            cnt = result[0]["cnt"]
            vr.record(f"Neo4j count: {label}", cnt >= min_cnt, f"{cnt:,} nodes")
        except Exception as e:
            vr.record(f"Neo4j count: {label}", False, str(e)[:120])

    # Graph traversal count
    try:
        result = remote_query(spark, cfg,
            "SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a"
        ).collect()
        cnt = result[0]["cnt"]
        vr.record("Neo4j traversal: Flight→Airport", cnt > 0, f"{cnt:,} connections")
    except Exception as e:
        vr.record("Neo4j traversal: Flight→Airport", False, str(e)[:120])

    # ============================================================================
    # Section 3: Federated Query — Fleet Summary (remote_query only)
    # ============================================================================
    print("\n--- Federated: Fleet Summary ---")
    try:
        t0 = time.time()
        result = spark.sql(f"""
            SELECT
                neo4j.total_maintenance_events,
                neo4j.critical_events,
                neo4j.total_flights,
                neo4j.flight_airport_connections,
                ROUND(sensor.avg_egt, 1) AS avg_egt_celsius,
                ROUND(sensor.avg_vibration, 4) AS avg_vibration_ips,
                ROUND(sensor.avg_fuel_flow, 2) AS avg_fuel_flow_kgs,
                ROUND(sensor.avg_n1_speed, 0) AS avg_n1_speed_rpm,
                sensor.total_readings
            FROM (
                SELECT
                    maint.cnt AS total_maintenance_events,
                    crit.cnt AS critical_events,
                    flights.cnt AS total_flights,
                    deps.cnt AS flight_airport_connections
                FROM
                    remote_query('{cfg.uc_connection_name}',
                        query => 'SELECT COUNT(*) AS cnt FROM MaintenanceEvent') AS maint
                CROSS JOIN
                    remote_query('{cfg.uc_connection_name}',
                        query => 'SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = ''CRITICAL''') AS crit
                CROSS JOIN
                    remote_query('{cfg.uc_connection_name}',
                        query => 'SELECT COUNT(*) AS cnt FROM Flight') AS flights
                CROSS JOIN
                    remote_query('{cfg.uc_connection_name}',
                        query => 'SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a') AS deps
            ) neo4j
            CROSS JOIN (
                SELECT
                    AVG(CASE WHEN sen.type = 'EGT' THEN r.value END) AS avg_egt,
                    AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END) AS avg_vibration,
                    AVG(CASE WHEN sen.type = 'FuelFlow' THEN r.value END) AS avg_fuel_flow,
                    AVG(CASE WHEN sen.type = 'N1Speed' THEN r.value END) AS avg_n1_speed,
                    COUNT(*) AS total_readings
                FROM sensor_readings r
                JOIN sensors sen ON r.sensorId = sen.sensorId
            ) sensor
        """)
        rows = result.collect()
        ms = (time.time() - t0) * 1000
        row = rows[0]
        vr.record("Federated: fleet summary",
                  row["total_maintenance_events"] > 0 and row["total_readings"] > 0,
                  f"maint={row['total_maintenance_events']}, readings={row['total_readings']:,}, {ms:.0f}ms")
        result.show(truncate=False)
    except Exception as e:
        vr.record("Federated: fleet summary", False, str(e)[:200])

    # ============================================================================
    # Section 4: Load Neo4j data via Spark Connector
    # ============================================================================
    print("\n--- Load Neo4j via Spark Connector ---")

    # 4a: Maintenance events
    try:
        t0 = time.time()
        neo4j_maintenance = spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", cfg.neo4j_bolt_uri) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", cfg.neo4j_username) \
            .option("authentication.basic.password", cfg.neo4j_password) \
            .option("labels", "MaintenanceEvent") \
            .load()
        neo4j_maintenance.createOrReplaceTempView("neo4j_maintenance")
        cnt = neo4j_maintenance.count()
        ms = (time.time() - t0) * 1000
        vr.record("Spark Connector: MaintenanceEvent", cnt > 0, f"{cnt} events, {ms:.0f}ms")
    except Exception as e:
        vr.record("Spark Connector: MaintenanceEvent", False, str(e)[:120])

    # 4b: Flights
    try:
        t0 = time.time()
        neo4j_flights = spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", cfg.neo4j_bolt_uri) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", cfg.neo4j_username) \
            .option("authentication.basic.password", cfg.neo4j_password) \
            .option("labels", "Flight") \
            .load()
        neo4j_flights.createOrReplaceTempView("neo4j_flights")
        cnt = neo4j_flights.count()
        ms = (time.time() - t0) * 1000
        vr.record("Spark Connector: Flight", cnt > 0, f"{cnt} flights, {ms:.0f}ms")
    except Exception as e:
        vr.record("Spark Connector: Flight", False, str(e)[:120])

    # ============================================================================
    # Section 5: Federated — Sensor Health + Maintenance Correlation
    # ============================================================================
    print("\n--- Federated: Sensor Health + Maintenance ---")
    try:
        t0 = time.time()
        result = spark.sql("""
            WITH aircraft_ref AS (
                SELECT aircraftId, tail_number, model, manufacturer, operator
                FROM aircraft
            ),
            sensor_health AS (
                SELECT
                    sys.aircraftId,
                    ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
                    ROUND(MAX(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS max_egt,
                    ROUND(AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS avg_vibration,
                    ROUND(MAX(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS max_vibration
                FROM sensor_readings r
                JOIN sensors sen ON r.sensorId = sen.sensorId
                JOIN systems sys ON sen.systemId = sys.systemId
                GROUP BY sys.aircraftId
            ),
            maintenance_summary AS (
                SELECT
                    aircraftId,
                    COUNT(*) AS total_events,
                    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical,
                    SUM(CASE WHEN severity = 'MAJOR' THEN 1 ELSE 0 END) AS major,
                    SUM(CASE WHEN severity = 'MINOR' THEN 1 ELSE 0 END) AS minor
                FROM neo4j_maintenance
                GROUP BY aircraftId
            )
            SELECT
                a.tail_number, a.model, a.operator,
                COALESCE(m.total_events, 0) AS maint_events,
                COALESCE(m.critical, 0) AS critical,
                COALESCE(m.major, 0) AS major,
                COALESCE(m.minor, 0) AS minor,
                s.avg_egt AS avg_egt_c, s.max_egt AS max_egt_c,
                s.avg_vibration AS avg_vib_ips, s.max_vibration AS max_vib_ips
            FROM aircraft_ref a
            LEFT JOIN maintenance_summary m ON a.aircraftId = m.aircraftId
            LEFT JOIN sensor_health s ON a.aircraftId = s.aircraftId
            ORDER BY m.total_events DESC NULLS LAST
        """)
        cnt = result.count()
        ms = (time.time() - t0) * 1000
        vr.record("Federated: sensor+maintenance", cnt > 0, f"{cnt} aircraft, {ms:.0f}ms")
        result.show(10, truncate=False)
    except Exception as e:
        vr.record("Federated: sensor+maintenance", False, str(e)[:200])

    # ============================================================================
    # Section 6: Federated — Flight Operations + Engine Performance
    # ============================================================================
    print("\n--- Federated: Flight Ops + Engine Health ---")
    try:
        t0 = time.time()
        result = spark.sql("""
            WITH aircraft_ref AS (
                SELECT aircraftId, tail_number, model, operator
                FROM aircraft
            ),
            flight_activity AS (
                SELECT
                    aircraftId,
                    COUNT(*) AS total_flights,
                    COUNT(DISTINCT origin) AS unique_origins,
                    COUNT(DISTINCT destination) AS unique_destinations
                FROM neo4j_flights
                GROUP BY aircraftId
            ),
            engine_health AS (
                SELECT
                    sys.aircraftId,
                    ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
                    ROUND(AVG(CASE WHEN sen.type = 'FuelFlow' THEN r.value END), 2) AS avg_fuel_flow,
                    ROUND(AVG(CASE WHEN sen.type = 'N1Speed' THEN r.value END), 0) AS avg_n1_speed
                FROM sensor_readings r
                JOIN sensors sen ON r.sensorId = sen.sensorId
                JOIN systems sys ON sen.systemId = sys.systemId
                WHERE sys.type = 'Engine'
                GROUP BY sys.aircraftId
            )
            SELECT
                a.tail_number, a.model, a.operator,
                f.total_flights, f.unique_origins AS origins, f.unique_destinations AS destinations,
                e.avg_egt AS avg_egt_c, e.avg_fuel_flow AS fuel_kgs, e.avg_n1_speed AS n1_rpm
            FROM aircraft_ref a
            JOIN flight_activity f ON a.aircraftId = f.aircraftId
            JOIN engine_health e ON a.aircraftId = e.aircraftId
            ORDER BY f.total_flights DESC
        """)
        cnt = result.count()
        ms = (time.time() - t0) * 1000
        vr.record("Federated: flights+engine", cnt > 0, f"{cnt} aircraft, {ms:.0f}ms")
        result.show(10, truncate=False)
    except Exception as e:
        vr.record("Federated: flights+engine", False, str(e)[:200])

    # ============================================================================
    # Section 7: Federated — Fleet Health Dashboard (both methods)
    # ============================================================================
    print("\n--- Federated: Fleet Health Dashboard ---")
    try:
        # Graph traversal via remote_query
        departure_count = remote_query(spark, cfg,
            "SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a"
        ).collect()[0]["cnt"]
        print(f"  Graph traversal Flight→Airport: {departure_count:,} connections")

        t0 = time.time()
        result = spark.sql("""
            WITH aircraft_ref AS (
                SELECT aircraftId, tail_number, model, manufacturer, operator
                FROM aircraft
            ),
            sensor_stats AS (
                SELECT
                    sys.aircraftId,
                    ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
                    ROUND(AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS avg_vib,
                    ROUND(AVG(CASE WHEN sen.type = 'FuelFlow' THEN r.value END), 2) AS avg_fuel,
                    COUNT(*) AS reading_count
                FROM sensor_readings r
                JOIN sensors sen ON r.sensorId = sen.sensorId
                JOIN systems sys ON sen.systemId = sys.systemId
                GROUP BY sys.aircraftId
            ),
            maint AS (
                SELECT aircraftId, COUNT(*) AS events,
                       SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical
                FROM neo4j_maintenance
                GROUP BY aircraftId
            ),
            flights AS (
                SELECT aircraftId, COUNT(*) AS flight_count
                FROM neo4j_flights
                GROUP BY aircraftId
            )
            SELECT
                a.tail_number, a.model, a.operator,
                COALESCE(f.flight_count, 0) AS flights,
                COALESCE(m.events, 0) AS maint_events,
                COALESCE(m.critical, 0) AS critical,
                s.avg_egt AS egt_c, s.avg_vib AS vib_ips, s.avg_fuel AS fuel_kgs,
                s.reading_count AS readings
            FROM aircraft_ref a
            LEFT JOIN flights f ON a.aircraftId = f.aircraftId
            LEFT JOIN maint m ON a.aircraftId = m.aircraftId
            LEFT JOIN sensor_stats s ON a.aircraftId = s.aircraftId
            ORDER BY COALESCE(m.critical, 0) DESC, COALESCE(m.events, 0) DESC
        """)
        cnt = result.count()
        ms = (time.time() - t0) * 1000
        vr.record("Federated: fleet health dashboard", cnt > 0, f"{cnt} aircraft, {ms:.0f}ms")
        result.show(10, truncate=False)
    except Exception as e:
        vr.record("Federated: fleet health dashboard", False, str(e)[:200])

    if not vr.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
