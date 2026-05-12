"""Optional diagnostics for advanced-patterns/07_performance_diagnostics.ipynb.

This is intentionally excluded from the default validation suite. It executes
the notebook's timing probes without pass/fail thresholds, because the timings
depend on workspace, compute mode, cluster size, and warm-up state.

Usage:
    ./validate.sh --include-performance
    uv run python -m cli submit run_07_performance_diagnostics.py
"""

import sys
import time

from data_utils import ValidationResults, get_config, get_neo4j_driver, inject_params


def main() -> None:
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    results = ValidationResults()
    timings: list[tuple[str, float]] = []

    print("=" * 60)
    print("validation: 07 Performance Diagnostics")
    print("=" * 60)
    print("Notebook: advanced-patterns/07_performance_diagnostics.ipynb")
    print("Mode: optional diagnostics, no thresholds")
    print(f"  Lakehouse: {cfg['lakehouse_catalog']}.{cfg['lakehouse_schema']}")
    print(f"  UC Connection: {cfg['uc_connection_name']}")
    print("")

    try:
        spark.sql(f"USE CATALOG `{cfg['lakehouse_catalog']}`")
        spark.sql(f"USE SCHEMA `{cfg['lakehouse_schema']}`")
        results.record(
            "Set lakehouse catalog/schema",
            True,
            f"{cfg['lakehouse_catalog']}.{cfg['lakehouse_schema']}",
        )
    except Exception as exc:
        results.record("Set lakehouse catalog/schema", False, str(exc)[:200])
        if not results.summary():
            sys.exit(1)
        return

    run_direct_neo4j_timings(cfg, results, timings)
    run_remote_query_timings(spark, cfg, results, timings)
    run_delta_timings(spark, results, timings)
    run_spark_connector_timing(spark, cfg, results, timings)
    run_compound_remote_query_timing(spark, cfg, results, timings)
    run_explain_plans(spark, cfg, results)
    run_warmup_timing(spark, cfg, results, timings)

    print("\n--- Timing Summary ---")
    for name, elapsed_ms in timings:
        print(f"  {name}: {elapsed_ms:.0f}ms")

    if not results.summary():
        sys.exit(1)


def run_direct_neo4j_timings(
    cfg: dict, results: ValidationResults, timings: list[tuple[str, float]]
) -> None:
    print("--- Test 1: Direct Neo4j Python Driver Timing ---")
    queries = {
        "MaintenanceEvent count": "MATCH (m:MaintenanceEvent) RETURN COUNT(m) AS cnt",
        "Critical events": (
            "MATCH (m:MaintenanceEvent {severity: 'CRITICAL'}) "
            "RETURN COUNT(m) AS cnt"
        ),
        "Flight count": "MATCH (f:Flight) RETURN COUNT(f) AS cnt",
        "Flight->Airport traversal": (
            "MATCH (f:Flight)-[:DEPARTS_FROM]->(a:Airport) RETURN COUNT(*) AS cnt"
        ),
        "Aircraft count": "MATCH (a:Aircraft) RETURN COUNT(a) AS cnt",
    }
    try:
        driver = get_neo4j_driver(cfg)
        with driver.session(database=cfg["neo4j_database"]) as session:
            for name, sql in queries.items():
                start = time.time()
                row = session.run(sql).single()
                elapsed = (time.time() - start) * 1000
                timings.append((f"Direct Neo4j: {name}", elapsed))
                count = row["cnt"] if row is not None else None
                results.record(
                    f"Direct Neo4j: {name}",
                    count is not None and count >= 0,
                    f"{count} rows, {elapsed:.0f}ms",
                )
        driver.close()
    except Exception as exc:
        results.record("Direct Neo4j timing", False, str(exc)[:200])


def run_remote_query_timings(
    spark, cfg: dict, results: ValidationResults, timings: list[tuple[str, float]]
) -> None:
    print("\n--- Test 2: remote_query() Timing ---")
    queries = {
        "MaintenanceEvent count": "SELECT COUNT(*) AS cnt FROM MaintenanceEvent",
        "Critical events": "SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = 'CRITICAL'",
        "Flight count": "SELECT COUNT(*) AS cnt FROM Flight",
        "Flight->Airport traversal": (
            "SELECT COUNT(*) AS cnt FROM Flight f "
            "NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a"
        ),
        "Aircraft count": "SELECT COUNT(*) AS cnt FROM Aircraft",
    }
    for name, sql in queries.items():
        try:
            start = time.time()
            rows = remote_query(spark, cfg, sql).collect()
            elapsed = (time.time() - start) * 1000
            timings.append((f"remote_query: {name}", elapsed))
            results.record(
                f"remote_query: {name}",
                len(rows) > 0,
                f"{elapsed:.0f}ms",
            )
        except Exception as exc:
            results.record(f"remote_query: {name}", False, str(exc)[:200])


def run_delta_timings(
    spark, results: ValidationResults, timings: list[tuple[str, float]]
) -> None:
    print("\n--- Test 3: Delta Query Timing ---")
    queries = {
        "sensor_readings count": "SELECT COUNT(*) AS cnt FROM sensor_readings",
        "aircraft count": "SELECT COUNT(*) AS cnt FROM aircraft",
        "sensors count": "SELECT COUNT(*) AS cnt FROM sensors",
        "systems count": "SELECT COUNT(*) AS cnt FROM systems",
        "sensor averages": """
            SELECT sen.type, COUNT(*) AS readings, ROUND(AVG(r.value), 2) AS avg_value
            FROM sensor_readings r
            JOIN sensors sen ON r.sensorId = sen.sensorId
            GROUP BY sen.type
        """,
        "engine health": """
            SELECT sys.aircraftId,
                   ROUND(AVG(CASE WHEN sen.type = 'EGT' THEN r.value END), 1) AS avg_egt,
                   ROUND(AVG(CASE WHEN sen.type = 'FuelFlow' THEN r.value END), 2) AS avg_fuel_flow
            FROM sensor_readings r
            JOIN sensors sen ON r.sensorId = sen.sensorId
            JOIN systems sys ON sen.systemId = sys.systemId
            WHERE sys.type = 'Engine'
            GROUP BY sys.aircraftId
        """,
    }
    for name, sql in queries.items():
        try:
            start = time.time()
            rows = spark.sql(sql).collect()
            elapsed = (time.time() - start) * 1000
            timings.append((f"Delta: {name}", elapsed))
            results.record(f"Delta: {name}", len(rows) > 0, f"{elapsed:.0f}ms")
        except Exception as exc:
            results.record(f"Delta: {name}", False, str(exc)[:200])


def run_spark_connector_timing(
    spark, cfg: dict, results: ValidationResults, timings: list[tuple[str, float]]
) -> None:
    print("\n--- Test 4: Spark Connector Timing ---")
    try:
        start = time.time()
        df = (
            spark.read.format("org.neo4j.spark.DataSource")
            .option("url", cfg["neo4j_bolt_uri"])
            .option("authentication.type", "basic")
            .option("authentication.basic.username", cfg["neo4j_username"])
            .option("authentication.basic.password", cfg["neo4j_password"])
            .option("labels", "MaintenanceEvent")
            .load()
        )
        count = df.count()
        elapsed = (time.time() - start) * 1000
        timings.append(("Spark Connector: MaintenanceEvent", elapsed))
        results.record(
            "Spark Connector: MaintenanceEvent",
            count > 0,
            f"{count} rows, {elapsed:.0f}ms",
        )
    except Exception as exc:
        results.record("Spark Connector timing", False, str(exc)[:200])


def run_compound_remote_query_timing(
    spark, cfg: dict, results: ValidationResults, timings: list[tuple[str, float]]
) -> None:
    print("\n--- Test 5: Compound vs Individual remote_query() ---")
    try:
        start = time.time()
        rows = spark.sql(
            f"""
            SELECT
                maint.cnt AS maintenance_events,
                crit.cnt AS critical_events,
                flights.cnt AS flights,
                deps.cnt AS flight_airport_connections
            FROM
                remote_query('{cfg["uc_connection_name"]}',
                    query => 'SELECT COUNT(*) AS cnt FROM MaintenanceEvent') AS maint
            CROSS JOIN
                remote_query('{cfg["uc_connection_name"]}',
                    query => 'SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = ''CRITICAL''') AS crit
            CROSS JOIN
                remote_query('{cfg["uc_connection_name"]}',
                    query => 'SELECT COUNT(*) AS cnt FROM Flight') AS flights
            CROSS JOIN
                remote_query('{cfg["uc_connection_name"]}',
                    query => 'SELECT COUNT(*) AS cnt FROM Flight f NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a') AS deps
            """
        ).collect()
        elapsed = (time.time() - start) * 1000
        timings.append(("Compound remote_query", elapsed))
        results.record("Compound remote_query", len(rows) == 1, f"{elapsed:.0f}ms")
    except Exception as exc:
        results.record("Compound remote_query", False, str(exc)[:200])


def run_explain_plans(spark, cfg: dict, results: ValidationResults) -> None:
    print("\n--- Test 6: Spark Explain Plans ---")
    try:
        spark.sql(
            f"""
            SELECT * FROM remote_query('{cfg["uc_connection_name"]}',
                query => 'SELECT COUNT(*) AS cnt FROM MaintenanceEvent')
            """
        ).explain(True)
        results.record("EXPLAIN simple remote_query", True)
    except Exception as exc:
        results.record("EXPLAIN simple remote_query", False, str(exc)[:200])


def run_warmup_timing(
    spark, cfg: dict, results: ValidationResults, timings: list[tuple[str, float]]
) -> None:
    print("\n--- Test 7: Warm-up Repeated Query Timing ---")
    sql = "SELECT COUNT(*) AS cnt FROM MaintenanceEvent"
    elapsed_runs = []
    for index in range(3):
        try:
            start = time.time()
            rows = remote_query(spark, cfg, sql).collect()
            elapsed = (time.time() - start) * 1000
            elapsed_runs.append(elapsed)
            timings.append((f"Warm-up run {index + 1}", elapsed))
            results.record(
                f"Warm-up run {index + 1}",
                len(rows) > 0,
                f"{elapsed:.0f}ms",
            )
        except Exception as exc:
            results.record(f"Warm-up run {index + 1}", False, str(exc)[:200])
    if elapsed_runs:
        print(f"  min={min(elapsed_runs):.0f}ms max={max(elapsed_runs):.0f}ms")


def remote_query(spark, cfg: dict, query: str):
    """Execute SQL through Databricks remote_query()."""
    safe_query = query.replace("'", "''")
    return spark.sql(
        f"""
        SELECT * FROM remote_query('{cfg["uc_connection_name"]}',
            query => '{safe_query}')
        """
    )


if __name__ == "__main__":
    main()
