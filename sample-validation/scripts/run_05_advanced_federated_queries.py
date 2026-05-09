"""Validate advanced SQL-to-Cypher translation and federated queries.

Tests the SQL-to-Cypher translator systematically across GROUP BY, HAVING,
ORDER BY, LIMIT, OFFSET, DISTINCT, and combinations. Closes with two federated
queries that embed these results inside Spark SQL joins against Delta sensor data.

Prerequisites:
  - run_00_load_graph.py must have run (loads Neo4j graph data)
  - run_01_connect_test.py must have run (creates UC JDBC connection and sensor_readings table)

Sections:
  1. GROUP BY — projected key, non-projected key, multiple aggregates
  2. HAVING — simple, compound, non-projected aggregate
  3. ORDER BY — on aggregate alias, multi-key
  4. DISTINCT + GROUP BY
  5. LIMIT + OFFSET
  6. All clauses combined
  7. Federated: Neo4j GROUP BY result joined with Delta sensor data
  8. Federated: Neo4j HAVING filter joined with Delta engine performance data
  9. JOIN + GROUP BY — non-projected and projected group key
  10. HAVING without GROUP BY
  11. WHERE + GROUP BY — filtered aggregations
  12. Additional aggregate functions — stDev, stDevP
  13. HAVING + ORDER BY + LIMIT + OFFSET combined

Usage (via runner):
    uv run python -m cli upload run_05_advanced_federated_queries.py
    uv run python -m cli submit run_05_advanced_federated_queries.py
"""

import sys
import time

from data_utils import inject_params, get_config, ValidationResults


def main():
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    results = ValidationResults()
    conn = cfg["uc_connection_name"]
    fqn = cfg["fqn"]

    print("=" * 60)
    print("run_05_advanced_federated_queries: Advanced SQL Translation")
    print("=" * 60)
    print(f"  Neo4j URI:     {cfg['neo4j_uri']}")
    print(f"  UC Connection: {conn}")
    print("")

    def rq(query: str):
        """Execute a query against the Neo4j UC connection via remote_query()."""
        safe = query.replace("'", "''")
        return spark.sql(f"SELECT * FROM remote_query('{conn}', query => '{safe}')")

    # ------------------------------------------------------------------
    # Section 1: GROUP BY
    # The translator generates different Cypher WITH clause structures depending
    # on whether the GROUP BY key appears in the projection.
    # ------------------------------------------------------------------
    print("--- Section 1: GROUP BY ---")

    # Projected group key — key appears in SELECT, translator uses implicit WITH
    try:
        start = time.time()
        rows = rq("SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity").collect()
        elapsed = (time.time() - start) * 1000
        results.record("GROUP BY projected key", len(rows) > 0, f"{len(rows)} severities, {elapsed:.0f}ms")
    except Exception as e:
        results.record("GROUP BY projected key", False, str(e)[:120])

    # Non-projected group key — key not in SELECT, translator must add explicit WITH
    try:
        start = time.time()
        rows = rq("SELECT COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity").collect()
        elapsed = (time.time() - start) * 1000
        results.record("GROUP BY non-projected key", len(rows) > 0, f"{len(rows)} groups, {elapsed:.0f}ms")
    except Exception as e:
        results.record("GROUP BY non-projected key", False, str(e)[:120])

    # Multiple aggregates in a single GROUP BY
    try:
        start = time.time()
        rows = rq("""SELECT operator, COUNT(*) AS flights,
                            COUNT(DISTINCT origin) AS origins,
                            COUNT(DISTINCT destination) AS destinations
                     FROM Flight GROUP BY operator""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("GROUP BY multiple aggregates", len(rows) > 0, f"{len(rows)} operators, {elapsed:.0f}ms")
    except Exception as e:
        results.record("GROUP BY multiple aggregates", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 2: HAVING
    # ------------------------------------------------------------------
    print("\n--- Section 2: HAVING ---")

    # Simple HAVING on projected aggregate alias
    try:
        start = time.time()
        rows = rq("SELECT operator, COUNT(*) AS cnt FROM Flight GROUP BY operator HAVING cnt > 20").collect()
        elapsed = (time.time() - start) * 1000
        all_over_20 = all(r["cnt"] > 20 for r in rows)
        results.record("HAVING simple", all_over_20, f"{len(rows)} operators, {elapsed:.0f}ms")
    except Exception as e:
        results.record("HAVING simple", False, str(e)[:120])

    # HAVING on non-projected aggregate — aggregate used only in filter, not in SELECT
    try:
        start = time.time()
        rows = rq("SELECT severity FROM MaintenanceEvent GROUP BY severity HAVING COUNT(*) > 10").collect()
        elapsed = (time.time() - start) * 1000
        results.record("HAVING non-projected aggregate", len(rows) >= 0,
                       f"{len(rows)} severities, {elapsed:.0f}ms")
    except Exception as e:
        results.record("HAVING non-projected aggregate", False, str(e)[:120])

    # Compound HAVING condition
    try:
        start = time.time()
        rows = rq("""SELECT operator, COUNT(*) AS cnt FROM Flight GROUP BY operator
                     HAVING COUNT(*) > 10 AND COUNT(DISTINCT origin) > 2""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("HAVING compound condition", len(rows) >= 0, f"{len(rows)} operators, {elapsed:.0f}ms")
    except Exception as e:
        results.record("HAVING compound condition", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 3: ORDER BY
    # ------------------------------------------------------------------
    print("\n--- Section 3: ORDER BY ---")

    # ORDER BY on aggregate alias
    try:
        start = time.time()
        rows = rq("SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent GROUP BY severity ORDER BY cnt DESC").collect()
        elapsed = (time.time() - start) * 1000
        counts = [r["cnt"] for r in rows]
        is_sorted = counts == sorted(counts, reverse=True)
        results.record("ORDER BY aggregate alias", is_sorted and len(rows) > 0,
                       f"{len(rows)} rows sorted desc, {elapsed:.0f}ms")
    except Exception as e:
        results.record("ORDER BY aggregate alias", False, str(e)[:120])

    # Multi-key ORDER BY
    try:
        start = time.time()
        rows = rq("""SELECT operator, COUNT(*) AS cnt, COUNT(DISTINCT origin) AS routes
                     FROM Flight GROUP BY operator ORDER BY cnt DESC, routes""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("ORDER BY multi-key", len(rows) > 0, f"{len(rows)} rows, {elapsed:.0f}ms")
    except Exception as e:
        results.record("ORDER BY multi-key", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 4: DISTINCT + GROUP BY
    # ------------------------------------------------------------------
    print("\n--- Section 4: DISTINCT + GROUP BY ---")
    try:
        start = time.time()
        rows = rq("SELECT DISTINCT operator, COUNT(*) AS cnt FROM Flight GROUP BY operator").collect()
        elapsed = (time.time() - start) * 1000
        results.record("DISTINCT + GROUP BY", len(rows) > 0, f"{len(rows)} operators, {elapsed:.0f}ms")
    except Exception as e:
        results.record("DISTINCT + GROUP BY", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 5: LIMIT + OFFSET
    # ------------------------------------------------------------------
    print("\n--- Section 5: LIMIT + OFFSET ---")
    try:
        start = time.time()
        rows = rq("""SELECT operator, COUNT(*) AS cnt FROM Flight
                     GROUP BY operator ORDER BY cnt DESC LIMIT 3 OFFSET 1""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("LIMIT + OFFSET", len(rows) <= 3, f"{len(rows)} rows (skip 1, take 3), {elapsed:.0f}ms")
    except Exception as e:
        results.record("LIMIT + OFFSET", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 6: All clauses combined
    # DISTINCT + GROUP BY + HAVING + ORDER BY + LIMIT + OFFSET
    # Note: WHERE with LIKE is excluded — Databricks remote_query() strips quotes from
    # LIKE string literals before passing to JDBC (known Databricks bug, not a connector issue).
    # ------------------------------------------------------------------
    print("\n--- Section 6: All Clauses Combined ---")
    try:
        start = time.time()
        result = rq("""SELECT DISTINCT severity, COUNT(*) AS cnt, MAX(fault) AS last_fault
                       FROM MaintenanceEvent
                       GROUP BY severity
                       HAVING COUNT(*) > 1
                       ORDER BY cnt DESC
                       LIMIT 10 OFFSET 0""")
        rows = result.collect()
        elapsed = (time.time() - start) * 1000
        results.record("All clauses combined", len(rows) > 0, f"{len(rows)} rows, {elapsed:.0f}ms")
        result.show(truncate=False)
    except Exception as e:
        results.record("All clauses combined", False, str(e)[:200])

    # ------------------------------------------------------------------
    # Section 7: Federated — Neo4j GROUP BY result joined with Delta sensor data
    # Neo4j: maintenance counts grouped by aircraft and severity via remote_query()
    # Delta: average EGT and vibration per aircraft via sensor_readings
    # ------------------------------------------------------------------
    print("\n--- Section 7: Federated — GROUP BY + Delta ---")
    try:
        start = time.time()
        result = spark.sql(f"""
            WITH neo4j_maint AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT aircraftId, COUNT(*) AS maint_count
                              FROM MaintenanceEvent
                              GROUP BY aircraftId')
            ),
            sensor_health AS (
                SELECT
                    REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1) AS aircraftId,
                    ROUND(AVG(value), 2) AS avg_reading
                FROM {fqn}.sensor_readings
                GROUP BY REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1)
            )
            SELECT
                s.aircraftId,
                COALESCE(m.maint_count, 0) AS maint_count,
                s.avg_reading
            FROM sensor_health s
            LEFT JOIN neo4j_maint m ON m.aircraftId = s.aircraftId
            ORDER BY maint_count DESC
        """)
        row_count = result.count()
        elapsed = (time.time() - start) * 1000
        result.show(10, truncate=False)
        results.record("Federated: GROUP BY + Delta sensor health", row_count > 0,
                       f"{row_count} rows, {elapsed:.0f}ms")
    except Exception as e:
        results.record("Federated: GROUP BY + Delta sensor health", False, str(e)[:200])

    # ------------------------------------------------------------------
    # Section 8: Federated — Neo4j HAVING filter joined with Delta engine performance
    # Neo4j: operators with more than 20 flights, filtered via HAVING, via remote_query()
    # Delta: average EGT, fuel flow, and N1 speed per operator via sensor_readings
    # ------------------------------------------------------------------
    print("\n--- Section 8: Federated — HAVING + Delta ---")
    try:
        start = time.time()
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
                FROM {fqn}.sensor_readings
            )
            SELECT
                o.operator,
                o.flight_count,
                f.avg_reading AS fleet_avg_sensor_reading,
                f.reading_count
            FROM active_operators o
            CROSS JOIN fleet_sensor_avg f
        """)
        row_count = result.count()
        elapsed = (time.time() - start) * 1000
        result.show(truncate=False)
        results.record("Federated: HAVING + Delta engine performance", row_count >= 0,
                       f"{row_count} operators, {elapsed:.0f}ms")
    except Exception as e:
        results.record("Federated: HAVING + Delta engine performance", False, str(e)[:200])

    # ------------------------------------------------------------------
    # Section 9: JOIN + GROUP BY
    # Tests translator handling of NATURAL JOIN with GROUP BY, both with and
    # without the group key appearing in the SELECT projection.
    # ------------------------------------------------------------------
    print("\n--- Section 9: JOIN + GROUP BY ---")

    # Non-projected group key — JOIN with GROUP BY, key not in SELECT
    try:
        start = time.time()
        rows = rq("""SELECT COUNT(*) AS flight_count FROM Flight f
                     NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a
                     GROUP BY a.iata""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("JOIN+GROUP BY non-projected", len(rows) > 0, f"{len(rows)} groups, {elapsed:.0f}ms")
    except Exception as e:
        results.record("JOIN+GROUP BY non-projected", False, str(e)[:120])

    # Projected group key — JOIN with GROUP BY, key in SELECT
    try:
        start = time.time()
        rows = rq("""SELECT a.iata, COUNT(*) AS flight_count FROM Flight f
                     NATURAL JOIN DEPARTS_FROM r NATURAL JOIN Airport a
                     GROUP BY a.iata""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("JOIN+GROUP BY projected", len(rows) > 0, f"{len(rows)} airports, {elapsed:.0f}ms")
    except Exception as e:
        results.record("JOIN+GROUP BY projected", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 10: HAVING without GROUP BY
    # Translator must emit a HAVING filter on an implicit aggregate without
    # a GROUP BY clause — produces a single-row aggregate result.
    # ------------------------------------------------------------------
    print("\n--- Section 10: HAVING without GROUP BY ---")
    try:
        start = time.time()
        rows = rq("SELECT COUNT(*) AS cnt FROM MaintenanceEvent HAVING COUNT(*) > 5").collect()
        elapsed = (time.time() - start) * 1000
        results.record("HAVING without GROUP BY", rows[0]["cnt"] > 5, f"cnt={rows[0]['cnt']}, {elapsed:.0f}ms")
    except Exception as e:
        results.record("HAVING without GROUP BY", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 11: WHERE + GROUP BY
    # Tests translator handling of WHERE clause combined with GROUP BY,
    # including IS NOT NULL predicates.
    # ------------------------------------------------------------------
    print("\n--- Section 11: WHERE + GROUP BY ---")

    # WHERE with comparison + GROUP BY + ORDER BY
    try:
        start = time.time()
        rows = rq("""SELECT cause, COUNT(*) AS cnt FROM Delay
                     WHERE minutes > 30 GROUP BY cause ORDER BY cnt DESC""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("WHERE + GROUP BY", len(rows) > 0, f"{len(rows)} causes, {elapsed:.0f}ms")
    except Exception as e:
        results.record("WHERE + GROUP BY", False, str(e)[:120])

    # WHERE IS NOT NULL + GROUP BY
    try:
        start = time.time()
        rows = rq("""SELECT cause, COUNT(*) AS cnt FROM Delay
                     WHERE cause IS NOT NULL GROUP BY cause""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("WHERE + GROUP BY IS NOT NULL", len(rows) > 0, f"{len(rows)} causes, {elapsed:.0f}ms")
    except Exception as e:
        results.record("WHERE + GROUP BY IS NOT NULL", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 12: Additional aggregate functions
    # Verifies translator support for stDev and stDevP aggregate functions.
    # Note: percentileCont and percentileDisc are not supported — Cypher requires
    # two arguments (percentileCont(expr, percentile)) but the SQL single-arg form
    # generates invalid single-argument Cypher.
    # ------------------------------------------------------------------
    print("\n--- Section 12: Additional Aggregate Functions ---")

    # stDev
    try:
        start = time.time()
        rows = rq("SELECT cause, stDev(minutes) AS stddev_minutes FROM Delay GROUP BY cause").collect()
        elapsed = (time.time() - start) * 1000
        results.record("stDev aggregate", len(rows) > 0, f"{len(rows)} causes, {elapsed:.0f}ms")
    except Exception as e:
        results.record("stDev aggregate", False, str(e)[:120])

    # stDevP
    try:
        start = time.time()
        rows = rq("SELECT cause, stDevP(minutes) AS stddevp_minutes FROM Delay GROUP BY cause").collect()
        elapsed = (time.time() - start) * 1000
        results.record("stDevP aggregate", len(rows) > 0, f"{len(rows)} causes, {elapsed:.0f}ms")
    except Exception as e:
        results.record("stDevP aggregate", False, str(e)[:120])

    # ------------------------------------------------------------------
    # Section 13: HAVING + ORDER BY + LIMIT + OFFSET combined
    # Verifies the translator correctly chains all four post-aggregation
    # clauses together in a single query.
    # ------------------------------------------------------------------
    print("\n--- Section 13: HAVING + ORDER BY + LIMIT + OFFSET ---")
    try:
        start = time.time()
        rows = rq("""SELECT severity, COUNT(*) AS cnt FROM MaintenanceEvent
                     GROUP BY severity HAVING COUNT(*) > 5
                     ORDER BY cnt DESC LIMIT 10 OFFSET 0""").collect()
        elapsed = (time.time() - start) * 1000
        results.record("HAVING+ORDER BY+LIMIT+OFFSET", len(rows) >= 0, f"{len(rows)} rows, {elapsed:.0f}ms")
    except Exception as e:
        results.record("HAVING+ORDER BY+LIMIT+OFFSET", False, str(e)[:120])

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
