"""Notebook parity for advanced-patterns/06_new_federated_queries.ipynb.

Runs the same advanced remote_query SQL cells and federated Delta joins without
running the notebook itself. Assertions are added around the notebook display
operations.

Usage (via the DAB job):
    uv run python validate.py run                # runs the full notebook_parity job
    databricks bundle run notebook_parity        # equivalent, direct bundle call
"""

import sys
import time

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
    conn = cfg.uc_connection_name
    lakehouse = cfg.lakehouse_fqn

    print("=" * 60)
    print("validation: 06 New Federated Queries")
    print("=" * 60)
    print("Notebook: advanced-patterns/06_new_federated_queries.ipynb")
    print(f"  Lakehouse: {cfg.lakehouse_catalog}.{cfg.lakehouse_schema}")
    print(f"  UC Conn:   {conn}")
    print("")

    try:
        spark.sql(f"USE CATALOG `{cfg.lakehouse_catalog}`")
        spark.sql(f"USE SCHEMA `{cfg.lakehouse_schema}`")
        results.record(
            "Set lakehouse catalog/schema",
            True,
            f"{cfg.lakehouse_catalog}.{cfg.lakehouse_schema}",
        )
    except RUNTIME_ERRORS as exc:
        results.record("Set lakehouse catalog/schema", False, str(exc)[:200])
        if not results.summary():
            sys.exit(1)
        return

    def record_query(name: str, query: str, validator, detail):
        start = time.time()
        try:
            rows = remote_query(spark, cfg, query).collect()
            elapsed = (time.time() - start) * 1000
            passed = validator(rows)
            results.record(name, passed, detail(rows, elapsed))
            return rows
        except RUNTIME_ERRORS as exc:
            results.record(name, False, str(exc)[:200])
            return []

    def any_rows(rows):
        return len(rows) > 0

    def row_count_detail(rows, elapsed):
        return f"{len(rows)} rows, {elapsed:.0f}ms"

    print("--- Advanced remote_query SQL patterns ---")

    record_query(
        "GROUP BY projected key",
        "SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent m GROUP BY severity",
        lambda rows: len(rows) >= 3 and sum(row["cnt"] for row in rows) == 300,
        row_count_detail,
    )

    record_query(
        "GROUP BY non-projected key",
        "SELECT COUNT(*) AS cnt FROM MaintenanceEvent m GROUP BY severity",
        lambda rows: len(rows) >= 3 and sum(row["cnt"] for row in rows) == 300,
        row_count_detail,
    )

    record_query(
        "GROUP BY multiple aggregates",
        """
        SELECT operator, COUNT(*) AS flights,
               COUNT(DISTINCT origin) AS origins,
               COUNT(DISTINCT destination) AS destinations
        FROM Flight f
        GROUP BY operator
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "HAVING simple",
        "SELECT operator, COUNT(*) AS cnt FROM Flight f GROUP BY operator HAVING cnt > 20",
        lambda rows: any_rows(rows) and all(row["cnt"] > 20 for row in rows),
        row_count_detail,
    )

    record_query(
        "HAVING non-projected aggregate",
        "SELECT severity FROM MaintenanceEvent m GROUP BY severity HAVING COUNT(*) > 10",
        any_rows,
        row_count_detail,
    )

    record_query(
        "HAVING compound",
        """
        SELECT operator, COUNT(*) AS cnt
        FROM Flight f
        GROUP BY operator
        HAVING COUNT(*) > 10 AND COUNT(DISTINCT origin) > 2
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "ORDER BY aggregate alias",
        "SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent m GROUP BY severity ORDER BY cnt DESC",
        lambda rows: any_rows(rows) and [row["cnt"] for row in rows] == sorted(
            [row["cnt"] for row in rows], reverse=True
        ),
        row_count_detail,
    )

    record_query(
        "ORDER BY multi-key",
        """
        SELECT operator, COUNT(*) AS cnt, COUNT(DISTINCT origin) AS routes
        FROM Flight f
        GROUP BY operator
        ORDER BY cnt DESC, routes
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "DISTINCT + GROUP BY",
        "SELECT DISTINCT operator, COUNT(*) AS cnt FROM Flight f GROUP BY operator",
        any_rows,
        row_count_detail,
    )

    record_query(
        "LIMIT + OFFSET",
        """
        SELECT operator, COUNT(*) AS cnt
        FROM Flight f
        GROUP BY operator
        ORDER BY cnt DESC
        LIMIT 3 OFFSET 1
        """,
        lambda rows: 0 < len(rows) <= 3,
        row_count_detail,
    )

    record_query(
        "HAVING + ORDER BY + LIMIT + OFFSET",
        """
        SELECT severity, COUNT(*) AS cnt
        FROM MaintenanceEvent m
        GROUP BY severity
        HAVING COUNT(*) > 5
        ORDER BY cnt DESC
        LIMIT 10 OFFSET 0
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "All clauses combined",
        """
        SELECT DISTINCT severity, COUNT(*) AS cnt, MAX(fault) AS last_fault
        FROM MaintenanceEvent m
        WHERE severity IS NOT NULL
        GROUP BY severity
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 10 OFFSET 0
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "JOIN + GROUP BY non-projected",
        """
        SELECT COUNT(*) AS flight_count
        FROM Flight f
        NATURAL JOIN DEPARTS_FROM r
        NATURAL JOIN Airport a
        GROUP BY a.iata
        """,
        lambda rows: (
            len(rows) >= 2
            and sum(row["flight_count"] for row in rows) == 800
        ),
        row_count_detail,
    )

    record_query(
        "JOIN + GROUP BY projected",
        """
        SELECT a.iata, COUNT(*) AS flight_count
        FROM Flight f
        NATURAL JOIN DEPARTS_FROM r
        NATURAL JOIN Airport a
        GROUP BY a.iata
        """,
        lambda rows: (
            len(rows) >= 2
            and sum(row["flight_count"] for row in rows) == 800
        ),
        row_count_detail,
    )

    print("\n--- Federated remote_query + Delta patterns ---")

    start = time.time()
    try:
        result = spark.sql(f"""
            WITH neo4j_maint AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT aircraftId, severity, COUNT(*) AS cnt
                              FROM MaintenanceEvent m
                              GROUP BY aircraftId, severity
                              ORDER BY cnt DESC')
            ),
            sensor_health AS (
                SELECT
                    sys.aircraftId,
                    ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
                    ROUND(AVG(CASE WHEN sen.type = 'Vibration' THEN r.value END), 4) AS avg_vibration
                FROM {lakehouse}.sensor_readings r
                JOIN {lakehouse}.sensors sen ON r.sensorId = sen.sensorId
                JOIN {lakehouse}.systems sys ON sen.systemId = sys.systemId
                GROUP BY sys.aircraftId
            )
            SELECT
                a.tail_number,
                a.model,
                m.severity,
                m.cnt AS maint_count,
                s.avg_egt AS avg_egt_c,
                s.avg_vibration AS avg_vib_ips
            FROM neo4j_maint m
            JOIN {lakehouse}.aircraft a ON m.aircraftId = a.aircraftId
            JOIN sensor_health s ON m.aircraftId = s.aircraftId
            ORDER BY m.cnt DESC
        """)
        rows = result.collect()
        elapsed = (time.time() - start) * 1000
        results.record(
            "Federated: GROUP BY + Delta",
            len(rows) > 0
            and all(row["maint_count"] > 0 for row in rows)
            and all(row["avg_egt_c"] is not None for row in rows)
            and all(row["avg_vib_ips"] is not None for row in rows),
            f"{len(rows)} aircraft/severity rows, {elapsed:.0f}ms",
        )
        result.show(10, truncate=False)
    except RUNTIME_ERRORS as exc:
        results.record("Federated: GROUP BY + Delta", False, str(exc)[:200])

    start = time.time()
    try:
        result = spark.sql(f"""
            WITH active_operators AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT operator,
                                     COUNT(*) AS flight_count,
                                     COUNT(DISTINCT aircraftId) AS aircraft_count
                              FROM Flight f
                              GROUP BY operator
                              HAVING COUNT(*) > 20
                              ORDER BY flight_count DESC')
            ),
            fleet_sensor_avg AS (
                SELECT
                    a.operator,
                    ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
                    ROUND(AVG(CASE WHEN sen.type = 'FuelFlow' THEN r.value END), 2) AS avg_fuel_flow,
                    ROUND(AVG(CASE WHEN sen.type = 'N1Speed' THEN r.value END), 0) AS avg_n1_speed
                FROM {lakehouse}.sensor_readings r
                JOIN {lakehouse}.sensors sen ON r.sensorId = sen.sensorId
                JOIN {lakehouse}.systems sys ON sen.systemId = sys.systemId
                JOIN {lakehouse}.aircraft a ON sys.aircraftId = a.aircraftId
                WHERE sys.type = 'Engine'
                GROUP BY a.operator
            )
            SELECT
                o.operator,
                o.flight_count,
                o.aircraft_count,
                f.avg_egt AS avg_egt_c,
                f.avg_fuel_flow AS fuel_kgs,
                f.avg_n1_speed AS n1_rpm
            FROM active_operators o
            JOIN fleet_sensor_avg f ON o.operator = f.operator
            ORDER BY o.flight_count DESC
        """)
        rows = result.collect()
        elapsed = (time.time() - start) * 1000
        results.record(
            "Federated: HAVING + Delta",
            len(rows) > 0 and all(row["flight_count"] > 20 for row in rows),
            f"{len(rows)} operators, {elapsed:.0f}ms",
        )
        result.show(truncate=False)
    except RUNTIME_ERRORS as exc:
        results.record("Federated: HAVING + Delta", False, str(exc)[:200])

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
