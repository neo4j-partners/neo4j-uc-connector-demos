"""Notebook parity for getting-started/02-federated-queries.ipynb.

Runs the same three federated query sections from the notebook without running
the notebook itself. Display calls are kept for log readability, and assertions
turn missing or mismatched results into job failures.

Usage:
    uv run python validate.py upload run_02_federated_queries.py
    uv run python validate.py submit run_02_federated_queries.py
"""

import sys

from data_utils import (
    RUNTIME_ERRORS,
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
    print("validation: 02 Federated Queries")
    print("=" * 60)
    print("Notebook: getting-started/02-federated-queries.ipynb")
    print(f"  Tables:        {fqn}.*")
    print(f"  UC Connection: {cfg.uc_connection_name}")
    print("")

    print("--- Helper: read_neo4j_jdbc(custom_schema, query) ---")

    print("=" * 60)
    print("FEDERATED QUERY 1: Sensor Health by Aircraft")
    print("=" * 60)
    try:
        sensor_topology = read_neo4j_jdbc(
            spark,
            cfg,
            "aircraftId STRING, model STRING, systemType STRING, sensorId STRING",
            """SELECT a.aircraftId AS aircraftId, a.model AS model,
                      sys.type AS systemType, s.sensorId AS sensorId
               FROM Aircraft a
               NATURAL JOIN HAS_SYSTEM r1
               NATURAL JOIN System sys
               NATURAL JOIN HAS_SENSOR r2
               NATURAL JOIN Sensor s""",
        )

        sensor_stats = spark.sql(
            f"""
            SELECT sensorId,
                   COUNT(*) AS reading_count,
                   ROUND(AVG(value), 2) AS avg_value,
                   ROUND(MIN(value), 2) AS min_value,
                   ROUND(MAX(value), 2) AS max_value
            FROM {fqn}.sensor_readings
            GROUP BY sensorId
            """
        )

        result = (
            sensor_topology.join(sensor_stats, "sensorId", "left")
            .select(
                "aircraftId",
                "model",
                "systemType",
                "sensorId",
                "reading_count",
                "avg_value",
            )
            .cache()
        )
        result.orderBy("aircraftId", "systemType").show(10, truncate=False)
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
        df_severity = read_neo4j_jdbc(
            spark,
            cfg,
            "severity STRING, event_count LONG",
            "SELECT severity, COUNT(*) AS event_count FROM MaintenanceEvent GROUP BY severity",
        )
        print("\n  Maintenance events by severity:")
        df_severity.orderBy("event_count", ascending=False).show(truncate=False)

        df_by_aircraft = read_neo4j_jdbc(
            spark,
            cfg,
            "aircraftId STRING, maint_count LONG",
            "SELECT aircraftId, COUNT(*) AS maint_count FROM MaintenanceEvent GROUP BY aircraftId",
        )

        aircraft_sensor_health = spark.sql(
            f"""
            SELECT REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1) AS aircraftId,
                   ROUND(AVG(value), 2) AS avg_sensor_reading
            FROM {fqn}.sensor_readings
            GROUP BY REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1)
            """
        )

        result = df_by_aircraft.join(aircraft_sensor_health, "aircraftId", "left").cache()
        print("  Maintenance count + avg sensor reading per aircraft:")
        result.orderBy("maint_count", ascending=False).show(10, truncate=False)
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
        df_flights = read_neo4j_jdbc(
            spark,
            cfg,
            "operator STRING, flight_count LONG",
            "SELECT operator, COUNT(*) AS flight_count FROM Flight GROUP BY operator",
        )
        print("\n  Flights by operator:")
        flight_rows = df_flights.collect()
        df_flights.orderBy("flight_count", ascending=False).show(truncate=False)

        df_delays = read_neo4j_jdbc(
            spark,
            cfg,
            "cause STRING, delay_count LONG, avg_minutes DOUBLE",
            "SELECT cause, COUNT(*) AS delay_count, AVG(minutes) AS avg_minutes FROM Delay GROUP BY cause",
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
