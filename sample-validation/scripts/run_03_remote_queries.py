"""Validate the remote_query() SQL federation access pattern.

remote_query() is the SQL-native way to call a UC JDBC connection from Spark SQL.
It is what users write in DBSQL notebooks, dashboard queries, and SQL scripting
workflows. These tests confirm the Neo4j JDBC Lakehouse Federation Connector works
from this entry point in addition to the PySpark JDBC DataSource reader (run_01/02).

Prerequisites:
  - run_00_load_graph.py must have run (loads Neo4j graph data)
  - run_01_connect_test.py must have run (creates UC JDBC connection and sensor_readings table)

Sections:
  1. Basic remote_query() — SELECT 1
  2. Aggregate — COUNT(*)
  3. WHERE filter — COUNT with condition
  4. NATURAL JOIN graph traversal — Flight → Airport
  5. Federated SQL — remote_query() embedded in Spark SQL joining Delta tables

Usage (via runner):
    uv run python -m cli upload run_03_remote_queries.py
    uv run python -m cli submit run_03_remote_queries.py
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
    print("run_03_remote_queries: remote_query() Validation")
    print("=" * 60)
    print(f"  Neo4j URI:     {cfg['neo4j_uri']}")
    print(f"  UC Connection: {conn}")
    print("")

    def rq(query: str):
        """Execute a query against the Neo4j UC connection via remote_query()."""
        safe = query.replace("'", "''")
        return spark.sql(f"SELECT * FROM remote_query('{conn}', query => '{safe}')")

    # ------------------------------------------------------------------
    # Section 1: Basic remote_query() — SELECT 1
    # ------------------------------------------------------------------
    print("--- Section 1: Basic remote_query() ---")
    try:
        start = time.time()
        val = rq("SELECT 1 AS test").collect()[0]["test"]
        elapsed = (time.time() - start) * 1000
        results.record("remote_query SELECT 1", val == 1, f"{elapsed:.0f}ms")
    except Exception as e:
        results.record("remote_query SELECT 1", False, str(e))

    # ------------------------------------------------------------------
    # Section 2: Aggregate — COUNT(*)
    # ------------------------------------------------------------------
    print("\n--- Section 2: Aggregate ---")
    try:
        start = time.time()
        cnt = rq("SELECT COUNT(*) AS cnt FROM Aircraft").collect()[0]["cnt"]
        elapsed = (time.time() - start) * 1000
        results.record("remote_query COUNT Aircraft", cnt == 20, f"{cnt} aircraft, {elapsed:.0f}ms")
    except Exception as e:
        results.record("remote_query COUNT Aircraft", False, str(e))

    try:
        start = time.time()
        cnt = rq("SELECT COUNT(*) AS cnt FROM Flight").collect()[0]["cnt"]
        elapsed = (time.time() - start) * 1000
        results.record("remote_query COUNT Flight", cnt == 800, f"{cnt} flights, {elapsed:.0f}ms")
    except Exception as e:
        results.record("remote_query COUNT Flight", False, str(e))

    # ------------------------------------------------------------------
    # Section 3: WHERE filter
    # ------------------------------------------------------------------
    print("\n--- Section 3: WHERE filter ---")
    try:
        start = time.time()
        cnt = rq("SELECT COUNT(*) AS cnt FROM Aircraft WHERE manufacturer = 'Boeing'").collect()[0]["cnt"]
        elapsed = (time.time() - start) * 1000
        results.record("remote_query WHERE manufacturer", cnt >= 0, f"{cnt} Boeing aircraft, {elapsed:.0f}ms")
    except Exception as e:
        results.record("remote_query WHERE manufacturer", False, str(e))

    try:
        start = time.time()
        cnt = rq("SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = 'CRITICAL'").collect()[0]["cnt"]
        elapsed = (time.time() - start) * 1000
        results.record("remote_query WHERE severity=CRITICAL", cnt >= 0, f"{cnt} events, {elapsed:.0f}ms")
    except Exception as e:
        results.record("remote_query WHERE severity=CRITICAL", False, str(e))

    # ------------------------------------------------------------------
    # Section 4: NATURAL JOIN graph traversal
    # ------------------------------------------------------------------
    print("\n--- Section 4: NATURAL JOIN graph traversal ---")
    try:
        start = time.time()
        cnt = rq("""SELECT COUNT(*) AS cnt
                    FROM Flight f
                    NATURAL JOIN DEPARTS_FROM r
                    NATURAL JOIN Airport a""").collect()[0]["cnt"]
        elapsed = (time.time() - start) * 1000
        results.record("remote_query NATURAL JOIN Flight→Airport", cnt == 800,
                       f"{cnt} connections, {elapsed:.0f}ms")
    except Exception as e:
        results.record("remote_query NATURAL JOIN Flight→Airport", False, str(e))

    try:
        start = time.time()
        cnt = rq("""SELECT COUNT(*) AS cnt
                    FROM Aircraft a
                    NATURAL JOIN HAS_SYSTEM rel
                    NATURAL JOIN System s""").collect()[0]["cnt"]
        elapsed = (time.time() - start) * 1000
        results.record("remote_query NATURAL JOIN Aircraft→System", cnt == 80,
                       f"{cnt} connections, {elapsed:.0f}ms")
    except Exception as e:
        results.record("remote_query NATURAL JOIN Aircraft→System", False, str(e))

    # ------------------------------------------------------------------
    # Section 5: Federated SQL — remote_query() inside Spark SQL joining Delta
    # Neo4j: maintenance event counts per aircraft (GROUP BY) via remote_query()
    # Delta: average sensor reading per aircraft via sensor_readings table
    # ------------------------------------------------------------------
    print("\n--- Section 5: Federated SQL ---")
    try:
        start = time.time()
        result = spark.sql(f"""
            WITH neo4j_maintenance AS (
                SELECT *
                FROM remote_query('{conn}',
                    query => 'SELECT aircraftId, COUNT(*) AS maint_count
                              FROM MaintenanceEvent
                              GROUP BY aircraftId')
            ),
            delta_sensor_health AS (
                SELECT REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1) AS aircraftId,
                       ROUND(AVG(value), 2) AS avg_sensor_reading,
                       COUNT(*) AS reading_count
                FROM {fqn}.sensor_readings
                GROUP BY REGEXP_EXTRACT(sensorId, '^(AC[0-9]+)', 1)
            )
            SELECT
                s.aircraftId,
                COALESCE(m.maint_count, 0) AS maint_count,
                s.avg_sensor_reading,
                s.reading_count
            FROM delta_sensor_health s
            LEFT JOIN neo4j_maintenance m ON m.aircraftId = s.aircraftId
            ORDER BY maint_count DESC
        """)
        row_count = result.count()
        elapsed = (time.time() - start) * 1000
        result.show(10, truncate=False)
        results.record("Federated: remote_query() + Delta sensor join", row_count > 0,
                       f"{row_count} aircraft, {elapsed:.0f}ms")
    except Exception as e:
        results.record("Federated: remote_query() + Delta sensor join", False, str(e))

    if not results.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
