"""Notebook parity for getting-started/02-federated-queries.ipynb.

Runs the same three federated query sections from the notebook with the current
validated pattern: live Neo4j reads enter Spark SQL through remote_query(), and
joins are planned inside SQL.

Usage (via the DAB job):
    uv run python validate.py run                # runs the full notebook_parity job
    databricks bundle run notebook_parity        # equivalent, direct bundle call
"""

import sys

from data_utils import (
    RUNTIME_ERRORS,
    ValidationResults,
    get_config,
    inject_params,
    remote_query,
)


def main() -> None:
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    results = ValidationResults()

    fqn = f"`{cfg.uc_catalog}`.`{cfg.uc_schema}`"
    conn = cfg.uc_connection_name

    print("=" * 60)
    print("validation: 02 Federated Queries")
    print("=" * 60)
    print("Notebook: getting-started/02-federated-queries.ipynb")
    print(f"  Tables:        {fqn}.*")
    print(f"  UC Connection: {conn}")
    print("")

    print("--- Setup: Load sensor_readings Delta Table ---")
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}.`{cfg.uc_volume}`")
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {fqn}.sensor_readings AS
            SELECT
                reading_id AS readingId,
                sensor_id AS sensorId,
                ts,
                value
            FROM read_files('{cfg.volume_path}/nodes_readings.csv',
                format => 'csv',
                header => true,
                inferColumnTypes => true)
            """
        )
        count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {fqn}.sensor_readings"
        ).collect()[0]["cnt"]
        results.record("Load sensor_readings", count == 172800, f"{count:,} rows")
        spark.sql(
            f"""
            SELECT sensorId, COUNT(*) AS readings
            FROM {fqn}.sensor_readings
            GROUP BY sensorId
            LIMIT 5
            """
        ).show(truncate=False)
    except RUNTIME_ERRORS as exc:
        results.record("Load sensor_readings", False, str(exc)[:200])

    print("--- Pattern: remote_query() TVF inside Spark SQL ---")

    print("=" * 60)
    print("FEDERATED QUERY 1: Sensor Health by Aircraft")
    print("=" * 60)
    try:
        result = spark.sql(
            f"""
            WITH sensor_topology AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT a.aircraftId AS aircraftId,
                                      a.model AS model,
                                      sys.type AS systemType,
                                      s.sensorId AS sensorId
                               FROM Aircraft a
                               NATURAL JOIN HAS_SYSTEM r1
                               NATURAL JOIN System sys
                               NATURAL JOIN HAS_SENSOR r2
                               NATURAL JOIN Sensor s')
            ),
            sensor_stats AS (
                SELECT sensorId,
                       COUNT(*) AS reading_count,
                       ROUND(AVG(value), 2) AS avg_value,
                       ROUND(MIN(value), 2) AS min_value,
                       ROUND(MAX(value), 2) AS max_value
                FROM {fqn}.sensor_readings
                GROUP BY sensorId
            )
            SELECT t.aircraftId,
                   t.model,
                   t.systemType,
                   t.sensorId,
                   s.reading_count,
                   s.avg_value
            FROM sensor_topology t
            LEFT JOIN sensor_stats s ON t.sensorId = s.sensorId
            ORDER BY t.aircraftId, t.systemType
            """
        ).cache()
        result.show(10, truncate=False)
        row_count = result.count()
        results.record(
            "Federated Query 1: sensor health by aircraft",
            row_count == 160,
            f"{row_count} sensor+aircraft rows",
        )
    except RUNTIME_ERRORS as exc:
        results.record(
            "Federated Query 1: sensor health by aircraft",
            False,
            str(exc)[:200],
        )

    print("=" * 60)
    print("FEDERATED QUERY 2: Maintenance Severity + Sensor Health")
    print("=" * 60)
    try:
        df_severity = remote_query(
            spark,
            cfg,
            """SELECT severity, COUNT(*) AS event_count
               FROM MaintenanceEvent
               GROUP BY severity
               HAVING COUNT(*) > 0""",
        )
        print("\n  Maintenance events by severity:")
        df_severity.orderBy("event_count", ascending=False).show(truncate=False)

        result = spark.sql(
            f"""
            WITH maintenance_by_aircraft AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT aircraftId, COUNT(*) AS maint_count
                              FROM MaintenanceEvent
                              GROUP BY aircraftId
                              HAVING COUNT(*) > 0')
            ),
            aircraft_sensor_health AS (
                SELECT REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1) AS aircraftId,
                       ROUND(AVG(value), 2) AS avg_sensor_reading
                FROM {fqn}.sensor_readings
                GROUP BY REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1)
            )
            SELECT m.aircraftId,
                   m.maint_count,
                   s.avg_sensor_reading
            FROM maintenance_by_aircraft m
            LEFT JOIN aircraft_sensor_health s ON m.aircraftId = s.aircraftId
            ORDER BY m.maint_count DESC
            """
        ).cache()
        print("  Maintenance count + avg sensor reading per aircraft:")
        result.show(10, truncate=False)
        row_count = result.count()
        results.record(
            "Federated Query 2: maintenance severity + sensor health",
            row_count == 20,
            f"{row_count} aircraft",
        )
    except RUNTIME_ERRORS as exc:
        results.record(
            "Federated Query 2: maintenance severity + sensor health",
            False,
            str(exc)[:200],
        )

    print("=" * 60)
    print("FEDERATED QUERY 3: Flight Delay Analysis by Operator")
    print("=" * 60)
    try:
        df_flights = remote_query(
            spark,
            cfg,
            """SELECT operator, COUNT(*) AS flight_count
               FROM Flight
               GROUP BY operator
               HAVING COUNT(*) > 0""",
        )
        print("\n  Flights by operator:")
        flight_rows = df_flights.collect()
        df_flights.orderBy("flight_count", ascending=False).show(truncate=False)

        df_delays = remote_query(
            spark,
            cfg,
            """SELECT cause, COUNT(*) AS delay_count, AVG(minutes) AS avg_minutes
               FROM Delay
               GROUP BY cause
               HAVING COUNT(*) > 0""",
        )
        print("  Delays by cause:")
        delay_rows = df_delays.collect()
        df_delays.orderBy("delay_count", ascending=False).show(truncate=False)
        results.record(
            "Federated Query 3: flight delay analysis",
            len(flight_rows) > 0 and len(delay_rows) > 0,
            f"{len(flight_rows)} operator rows, {len(delay_rows)} delay rows",
        )
    except RUNTIME_ERRORS as exc:
        results.record(
            "Federated Query 3: flight delay analysis",
            False,
            str(exc)[:200],
        )

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
