"""Notebook parity for getting-started/03-materialized-tables.ipynb.

Materializes the same Neo4j node and traversal queries as managed Delta tables,
then runs the same INFORMATION_SCHEMA, SQL validation, and federated Delta
queries from the notebook.

Usage (via the DAB job):
    uv run python validate.py run                # runs the full notebook_parity job
    databricks bundle run notebook_parity        # equivalent, direct bundle call
"""

import sys
import time

from data_utils import (
    RUNTIME_ERRORS,
    Config,
    ValidationResults,
    get_config,
    inject_params,
    read_neo4j_jdbc,
)


def main() -> None:
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    results = ValidationResults()
    fqn = f"`{cfg.uc_catalog}`.`{cfg.uc_schema}`"

    print("=" * 60)
    print("validation: 03 Materialized Tables")
    print("=" * 60)
    print("Notebook: getting-started/03-materialized-tables.ipynb")
    print(f"  Tables:        {fqn}.*")
    print(f"  UC Connection: {cfg.uc_connection_name}")
    print("")

    print("--- Section 1: Verify Data Sources ---")
    try:
        count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {fqn}.sensor_readings"
        ).collect()[0]["cnt"]
        results.record("Delta sensor_readings", count == 172800, f"{count:,} rows")
    except RUNTIME_ERRORS as exc:
        results.record("Delta sensor_readings", False, str(exc)[:160])

    for label, expected in [
        ("Aircraft", 20),
        ("Airport", 12),
        ("System", 80),
        ("Component", 320),
        ("Sensor", 160),
        ("Flight", 800),
        ("MaintenanceEvent", 300),
        ("Delay", 514),
    ]:
        try:
            cnt = read_neo4j_jdbc(
                spark, cfg, "cnt LONG", f"SELECT COUNT(*) AS cnt FROM {label}"
            ).collect()[0]["cnt"]
            results.record(f"Neo4j {label}", cnt == expected, f"{cnt} nodes")
        except RUNTIME_ERRORS as exc:
            results.record(f"Neo4j {label}", False, str(exc)[:160])

    print("\n--- Section 2: Materialize Neo4j Data ---")
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_aircraft",
        "SELECT aircraftId, tail_number, icao24, model, manufacturer, operator FROM Aircraft",
        "aircraftId STRING, tail_number STRING, icao24 STRING, model STRING, manufacturer STRING, operator STRING",
        20,
    )
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_airports",
        "SELECT airportId, name, city, country, iata, icao FROM Airport",
        "airportId STRING, name STRING, city STRING, country STRING, iata STRING, icao STRING",
        12,
    )
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_systems",
        "SELECT systemId, aircraftId, type, name FROM System",
        "systemId STRING, aircraftId STRING, type STRING, name STRING",
        80,
    )
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_sensors",
        "SELECT sensorId, systemId, type, name, unit FROM Sensor",
        "sensorId STRING, systemId STRING, type STRING, name STRING, unit STRING",
        160,
    )
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_components",
        "SELECT componentId, systemId, type, name FROM Component",
        "componentId STRING, systemId STRING, type STRING, name STRING",
        320,
    )
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_maintenance_events",
        "SELECT eventId, componentId, systemId, aircraftId, fault, severity, reported_at, corrective_action FROM MaintenanceEvent",
        "eventId STRING, componentId STRING, systemId STRING, aircraftId STRING, fault STRING, severity STRING, reported_at STRING, corrective_action STRING",
        300,
    )
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_flights",
        "SELECT flightId, flight_number, aircraftId, operator, origin, destination, scheduled_departure, scheduled_arrival FROM Flight",
        "flightId STRING, flight_number STRING, aircraftId STRING, operator STRING, origin STRING, destination STRING, scheduled_departure STRING, scheduled_arrival STRING",
        800,
    )
    materialize(
        spark,
        cfg,
        results,
        fqn,
        "neo4j_delays",
        "SELECT delayId, flightId, cause, CAST(minutes AS STRING) AS minutes FROM Delay",
        "delayId STRING, flightId STRING, cause STRING, minutes STRING",
        514,
    )
    materialize_aircraft_systems(spark, cfg, results, fqn)

    print("\n--- Section 3: Verify UC Schema Tracking ---")
    expected_neo4j_tables = {
        "neo4j_aircraft",
        "neo4j_aircraft_systems",
        "neo4j_airports",
        "neo4j_components",
        "neo4j_delays",
        "neo4j_flights",
        "neo4j_maintenance_events",
        "neo4j_sensors",
        "neo4j_systems",
    }
    record_sql_result(
        spark,
        results,
        "INFORMATION_SCHEMA neo4j_* tables",
        f"""
        SELECT table_name, table_type
        FROM `{cfg.uc_catalog}`.information_schema.tables
        WHERE table_schema = '{cfg.uc_schema}'
          AND table_name LIKE 'neo4j_%'
        ORDER BY table_name
        """,
        lambda rows: expected_neo4j_tables.issubset({row["table_name"] for row in rows}),
    )
    record_sql_result(
        spark,
        results,
        "INFORMATION_SCHEMA columns for neo4j_aircraft",
        f"""
        SELECT ordinal_position, column_name, data_type, is_nullable
        FROM `{cfg.uc_catalog}`.information_schema.columns
        WHERE table_schema = '{cfg.uc_schema}'
          AND table_name = 'neo4j_aircraft'
        ORDER BY ordinal_position
        """,
        lambda rows: len(rows) >= 6,
    )

    print("\n--- Section 4: SQL Validation Tests ---")
    sql_checks = [
        (
            "TEST 1: GROUP BY maintenance events by severity",
            f"""
            SELECT severity, COUNT(*) AS event_count
            FROM {fqn}.neo4j_maintenance_events
            GROUP BY severity
            ORDER BY event_count DESC
            """,
            lambda rows: len(rows) >= 3,
        ),
        (
            "TEST 2: WHERE + ORDER BY critical events",
            f"""
            SELECT eventId, aircraftId, fault, reported_at
            FROM {fqn}.neo4j_maintenance_events
            WHERE severity = 'CRITICAL'
            ORDER BY reported_at DESC
            """,
            lambda rows: len(rows) > 0,
        ),
        (
            "TEST 3: Aggregations + JOIN sensor count per model",
            f"""
            SELECT a.model, a.manufacturer,
                   COUNT(DISTINCT s.sensorId) AS sensor_count,
                   COUNT(DISTINCT sys.systemId) AS system_count
            FROM {fqn}.neo4j_aircraft a
            JOIN {fqn}.neo4j_systems sys ON a.aircraftId = sys.aircraftId
            JOIN {fqn}.neo4j_sensors s ON sys.systemId = s.systemId
            GROUP BY a.model, a.manufacturer
            ORDER BY sensor_count DESC
            """,
            lambda rows: len(rows) > 0,
        ),
        (
            "TEST 4: DISTINCT manufacturers and models",
            f"""
            SELECT DISTINCT manufacturer, model
            FROM {fqn}.neo4j_aircraft
            ORDER BY manufacturer, model
            """,
            lambda rows: len(rows) > 0,
        ),
    ]
    for name, query, validator in sql_checks:
        record_sql_result(spark, results, name, query, validator)

    print("\n--- Section 5: Federated Queries ---")
    federated_checks = [
        (
            "Federated Query 1: Aircraft Health Overview",
            f"""
            SELECT a.aircraftId, a.model, a.operator,
                   COUNT(DISTINCT m.eventId) AS maintenance_events,
                   COUNT(DISTINCT CASE WHEN m.severity = 'CRITICAL' THEN m.eventId END) AS critical_events,
                   COUNT(DISTINCT s.sensorId) AS sensor_count,
                   ROUND(AVG(r.value), 2) AS avg_sensor_reading
            FROM {fqn}.neo4j_aircraft a
            LEFT JOIN {fqn}.neo4j_maintenance_events m ON a.aircraftId = m.aircraftId
            LEFT JOIN {fqn}.neo4j_sensors s ON s.sensorId LIKE CONCAT(a.aircraftId, '-%')
            LEFT JOIN {fqn}.sensor_readings r ON r.sensorId = s.sensorId
            GROUP BY a.aircraftId, a.model, a.operator
            ORDER BY critical_events DESC, maintenance_events DESC
            """,
            lambda rows: (
                len(rows) == 20
                and all(row["critical_events"] <= row["maintenance_events"] for row in rows)
            ),
        ),
        (
            "Federated Query 2: Route Analysis",
            f"""
            SELECT dep.city AS origin_city, dep.iata AS origin,
                   arr.city AS destination_city, arr.iata AS destination,
                   COUNT(*) AS flight_count
            FROM {fqn}.neo4j_flights f
            JOIN {fqn}.neo4j_airports dep ON f.origin = dep.iata
            JOIN {fqn}.neo4j_airports arr ON f.destination = arr.iata
            GROUP BY dep.city, dep.iata, arr.city, arr.iata
            ORDER BY flight_count DESC
            LIMIT 10
            """,
            lambda rows: len(rows) > 0,
        ),
        (
            "Federated Query 3: Sensor Health by System Type",
            f"""
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
            """,
            lambda rows: len(rows) > 0,
        ),
        (
            "Federated Query 4: Delay Analysis by Airport and Cause",
            f"""
            SELECT dep.city AS departure_city, dep.iata,
                   d.cause, COUNT(*) AS delay_count,
                   ROUND(AVG(CAST(d.minutes AS INT)), 1) AS avg_delay_minutes
            FROM {fqn}.neo4j_delays d
            JOIN {fqn}.neo4j_flights f ON d.flightId = f.flightId
            JOIN {fqn}.neo4j_airports dep ON f.origin = dep.iata
            GROUP BY dep.city, dep.iata, d.cause
            ORDER BY delay_count DESC
            LIMIT 15
            """,
            lambda rows: len(rows) > 0,
        ),
    ]
    for name, query, validator in federated_checks:
        record_sql_result(spark, results, name, query, validator)

    if not results.summary():
        sys.exit(1)


def cast_all_to_string(df):
    """Match the notebook's CHAR(0)-safe Delta write workaround."""
    from pyspark.sql.types import StringType

    for col_name in df.columns:
        df = df.withColumn(col_name, df[col_name].cast(StringType()))
    return df


def materialize(
    spark,
    cfg: Config,
    results: ValidationResults,
    fqn: str,
    table_name: str,
    query: str,
    custom_schema: str,
    expected: int,
) -> None:
    """Read from Neo4j via UC JDBC and write as a managed Delta table."""
    try:
        df = read_neo4j_jdbc(spark, cfg, custom_schema, query)
        df = cast_all_to_string(df)
        start = time.time()
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"{fqn}.{table_name}")
        elapsed = (time.time() - start) * 1000
        cnt = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {fqn}.{table_name}"
        ).collect()[0]["cnt"]
        results.record(
            f"Materialize {table_name}",
            cnt == expected,
            f"{cnt} rows ({elapsed:.0f}ms)",
        )
    except RUNTIME_ERRORS as exc:
        results.record(f"Materialize {table_name}", False, str(exc)[:200])


def materialize_aircraft_systems(
    spark, cfg: Config, results: ValidationResults, fqn: str
) -> None:
    """Materialize the notebook's Aircraft -> HAS_SYSTEM -> System table."""
    try:
        df = read_neo4j_jdbc(
            spark,
            cfg,
            "aircraftId STRING, model STRING, systemId STRING, systemType STRING, systemName STRING, cnt LONG",
            """SELECT a.aircraftId AS aircraftId, a.model AS model,
                      s.systemId AS systemId, s.type AS systemType, s.name AS systemName,
                      COUNT(*) AS cnt
               FROM Aircraft a NATURAL JOIN HAS_SYSTEM rel NATURAL JOIN System s
               GROUP BY a.aircraftId, a.model, s.systemId, s.type, s.name""",
        ).select("aircraftId", "model", "systemId", "systemType", "systemName")

        df = cast_all_to_string(df)
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"{fqn}.neo4j_aircraft_systems")
        cnt = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {fqn}.neo4j_aircraft_systems"
        ).collect()[0]["cnt"]
        results.record("Materialize neo4j_aircraft_systems", cnt == 80, f"{cnt} rows")
    except RUNTIME_ERRORS as exc:
        results.record("Materialize neo4j_aircraft_systems", False, str(exc)[:200])


def record_sql_result(
    spark, results: ValidationResults, name: str, query: str, validator
) -> None:
    """Run a notebook display query and convert it into a validation assertion."""
    try:
        df = spark.sql(query)
        rows = df.collect()
        df.show(50, truncate=False)
        results.record(name, validator(rows), f"{len(rows)} rows")
    except RUNTIME_ERRORS as exc:
        results.record(name, False, str(exc)[:200])


if __name__ == "__main__":
    main()
