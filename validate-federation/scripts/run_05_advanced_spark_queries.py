"""Advanced Spark and remote_query validation.

This script owns the Databricks/Spark side of the advanced federation matrix.
Driver-only SQL translation regressions live in direct-jdbc-validation.

Coverage:
  1. GROUP BY, with projected and non-projected keys
  2. Multiple aggregates and COUNT(DISTINCT ...)
  3. HAVING, including non-projected and compound predicates
  4. Aggregate ORDER BY, DISTINCT, LIMIT, and OFFSET
  5. Combined post-aggregate clauses
  6. Federated remote_query aggregate results joined to Delta tables
  7. JOIN + GROUP BY traversal queries
  8. Databricks remote_query LIKE literal behavior

Usage:
    uv run python -m cli upload run_05_advanced_spark_queries.py
    uv run python -m cli submit run_05_advanced_spark_queries.py
"""

import sys
import time

from data_utils import ValidationResults, get_config, inject_params


def main():
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{cfg['lakehouse_catalog']}`")
    spark.sql(f"USE SCHEMA `{cfg['lakehouse_schema']}`")

    results = ValidationResults()
    conn = cfg["uc_connection_name"]
    lakehouse = cfg["lakehouse_fqn"]

    print("=" * 60)
    print("validate-federation: 05 Advanced Spark Queries")
    print("=" * 60)
    print(f"  Lakehouse: {cfg['lakehouse_catalog']}.{cfg['lakehouse_schema']}")
    print(f"  UC Conn:   {conn}")
    print("")

    def rq(query: str):
        """Execute SQL through Databricks remote_query()."""
        safe_query = query.replace("'", "''")
        return spark.sql(f"SELECT * FROM remote_query('{conn}', query => '{safe_query}')")

    def record_query(name: str, query: str, validator, detail):
        start = time.time()
        try:
            rows = rq(query).collect()
            elapsed = (time.time() - start) * 1000
            passed = validator(rows)
            results.record(name, passed, detail(rows, elapsed))
            return rows
        except Exception as exc:
            results.record(name, False, str(exc)[:200])
            return []

    def any_rows(rows):
        return len(rows) > 0

    def row_count_detail(rows, elapsed):
        return f"{len(rows)} rows, {elapsed:.0f}ms"

    print("--- Advanced remote_query SQL patterns ---")

    record_query(
        "GROUP BY projected key",
        "SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity",
        lambda rows: len(rows) >= 3 and sum(row["cnt"] for row in rows) == 300,
        row_count_detail,
    )

    record_query(
        "GROUP BY non-projected key",
        "SELECT COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity",
        lambda rows: len(rows) >= 3 and sum(row["cnt"] for row in rows) == 300,
        row_count_detail,
    )

    record_query(
        "GROUP BY multiple aggregates",
        """
        SELECT operator, COUNT(*) AS flights,
               COUNT(DISTINCT origin) AS origins,
               COUNT(DISTINCT destination) AS destinations
        FROM Flight
        GROUP BY operator
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "HAVING simple",
        "SELECT operator, COUNT(*) AS cnt FROM Flight GROUP BY operator HAVING cnt > 20",
        lambda rows: any_rows(rows) and all(row["cnt"] > 20 for row in rows),
        row_count_detail,
    )

    record_query(
        "HAVING non-projected aggregate",
        "SELECT severity FROM MaintenanceEvent GROUP BY severity HAVING COUNT(*) > 10",
        any_rows,
        row_count_detail,
    )

    record_query(
        "HAVING compound",
        """
        SELECT operator, COUNT(*) AS cnt
        FROM Flight
        GROUP BY operator
        HAVING COUNT(*) > 10 AND COUNT(DISTINCT origin) > 2
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "ORDER BY aggregate alias",
        "SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity ORDER BY cnt DESC",
        lambda rows: any_rows(rows) and [row["cnt"] for row in rows] == sorted(
            [row["cnt"] for row in rows], reverse=True
        ),
        row_count_detail,
    )

    record_query(
        "ORDER BY multi-key",
        """
        SELECT operator, COUNT(*) AS cnt, COUNT(DISTINCT origin) AS routes
        FROM Flight
        GROUP BY operator
        ORDER BY cnt DESC, routes
        """,
        any_rows,
        row_count_detail,
    )

    record_query(
        "DISTINCT + GROUP BY",
        "SELECT DISTINCT operator, COUNT(*) AS cnt FROM Flight GROUP BY operator",
        any_rows,
        row_count_detail,
    )

    record_query(
        "LIMIT + OFFSET",
        """
        SELECT operator, COUNT(*) AS cnt
        FROM Flight
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
        FROM MaintenanceEvent
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
        FROM MaintenanceEvent
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
        lambda rows: len(rows) >= 2 and sum(row["flight_count"] for row in rows) == 800,
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
        lambda rows: len(rows) >= 2 and sum(row["flight_count"] for row in rows) == 800,
        row_count_detail,
    )

    print("\n--- Federated remote_query + Delta patterns ---")

    start = time.time()
    try:
        result = spark.sql(f"""
            WITH neo4j_maint AS (
                SELECT
                    COALESCE(aircraftId, aircraft_id) AS aircraftId,
                    SUM(maint_count) AS maint_count
                FROM remote_query('{conn}',
                    query => 'SELECT aircraftId, aircraft_id, COUNT(*) AS maint_count
                              FROM MaintenanceEvent
                              GROUP BY aircraftId, aircraft_id')
                GROUP BY COALESCE(aircraftId, aircraft_id)
            ),
            sensor_health AS (
                SELECT
                    sys.aircraftId,
                    ROUND(AVG(r.value), 2) AS avg_reading
                FROM {lakehouse}.sensor_readings r
                JOIN {lakehouse}.sensors sen ON r.sensorId = sen.sensorId
                JOIN {lakehouse}.systems sys ON sen.systemId = sys.systemId
                GROUP BY sys.aircraftId
            )
            SELECT
                s.aircraftId,
                COALESCE(m.maint_count, 0) AS maint_count,
                s.avg_reading
            FROM sensor_health s
            LEFT JOIN neo4j_maint m ON m.aircraftId = s.aircraftId
            ORDER BY maint_count DESC
        """)
        rows = result.collect()
        elapsed = (time.time() - start) * 1000
        results.record(
            "Federated: GROUP BY + Delta",
            len(rows) == 20 and sum(row["maint_count"] for row in rows) == 300,
            f"{len(rows)} aircraft, {elapsed:.0f}ms",
        )
        result.show(10, truncate=False)
    except Exception as exc:
        results.record("Federated: GROUP BY + Delta", False, str(exc)[:200])

    start = time.time()
    try:
        result = spark.sql(f"""
            WITH active_operators AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT operator, COUNT(*) AS flight_count
                              FROM Flight
                              GROUP BY operator
                              HAVING COUNT(*) > 20')
            ),
            fleet_sensor_avg AS (
                SELECT ROUND(AVG(value), 2) AS avg_reading, COUNT(*) AS reading_count
                FROM {lakehouse}.sensor_readings
            )
            SELECT
                o.operator,
                o.flight_count,
                f.avg_reading AS fleet_avg_sensor_reading,
                f.reading_count
            FROM active_operators o
            CROSS JOIN fleet_sensor_avg f
        """)
        rows = result.collect()
        elapsed = (time.time() - start) * 1000
        results.record(
            "Federated: HAVING + Delta",
            len(rows) > 0 and all(row["flight_count"] > 20 for row in rows),
            f"{len(rows)} operators, {elapsed:.0f}ms",
        )
        result.show(truncate=False)
    except Exception as exc:
        results.record("Federated: HAVING + Delta", False, str(exc)[:200])

    print("\n--- Databricks remote_query LIKE literal behavior ---")
    try:
        rows = rq("""
            SELECT COUNT(*) AS cnt
            FROM MaintenanceEvent
            WHERE aircraftId LIKE 'AC%'
        """).collect()
        passed = len(rows) == 1 and rows[0]["cnt"] == 300
        detail = f"cnt={rows[0]['cnt']}" if rows else "no rows"
        results.record("LIKE literal through remote_query", passed, detail)
    except Exception as exc:
        message = str(exc)
        expected = "LIKE AC%" in message or "Token ')'" in message or "DURING_OUTPUT_SCHEMA_RESOLUTION" in message
        results.record(
            "LIKE literal through remote_query",
            expected,
            "known Databricks quote-stripping behavior" if expected else message[:200],
        )

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
