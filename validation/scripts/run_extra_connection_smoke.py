"""Extra regression coverage for Neo4j connectivity and UC remote_query().

Covers Python driver connectivity, UC connection creation, and live Neo4j SQL
queries through remote_query().

Usage (via the DAB job):
    uv run python validate.py extras             # runs the full extras job
    databricks bundle run extras                 # equivalent, direct bundle call
"""

import sys
import time

from data_utils import (
    RUNTIME_ERRORS,
    ValidationResults,
    get_config,
    get_neo4j_driver,
    inject_params,
    remote_query,
)


def main() -> None:
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    vr = ValidationResults()

    print("=" * 60)
    print("validation: 01 Connection Validation")
    print("=" * 60)
    print(f"  Neo4j URI:     {cfg.neo4j_uri}")
    print(f"  UC Connection: {cfg.uc_connection_name}")
    print(f"  JDBC JAR:      {cfg.jdbc_jar_path}")
    print("")

    print("--- Environment ---")
    print(f"  Spark: {spark.version}")
    print(f"  Python: {sys.version}")
    try:
        import neo4j

        print(f"  Neo4j Python Driver: {neo4j.__version__}")
    except ImportError:
        print("  Neo4j Python Driver: NOT INSTALLED")

    print("\n--- Neo4j Python Driver ---")
    try:
        t0 = time.time()
        driver = get_neo4j_driver(cfg)
        driver.verify_connectivity()
        ms = (time.time() - t0) * 1000

        with driver.session(database=cfg.neo4j_database) as session:
            val = session.run("RETURN 1 AS test").single()["test"]

        driver.close()
        vr.record("Python driver connectivity", val == 1, f"{ms:.0f}ms")
    except RUNTIME_ERRORS as e:
        vr.record("Python driver connectivity", False, str(e)[:120])

    print("\n--- UC JDBC Connection ---")
    try:
        spark.sql(f"DROP CONNECTION IF EXISTS {cfg.uc_connection_name}")

        create_sql = f"""
        CREATE CONNECTION {cfg.uc_connection_name} TYPE JDBC
        ENVIRONMENT (
          java_dependencies '{cfg.java_dependencies}'
        )
        OPTIONS (
          url '{cfg.neo4j_jdbc_url_sql}',
          user secret('{cfg.secret_scope}', 'NEO4J_USERNAME'),
          password secret('{cfg.secret_scope}', 'NEO4J_PASSWORD'),
          driver 'org.neo4j.jdbc.Neo4jDriver',
          externalOptionsAllowList 'dbtable,query,partitionColumn,lowerBound,upperBound,numPartitions,fetchSize,customSchema'
        )
        """
        t0 = time.time()
        spark.sql(create_sql)
        ms = (time.time() - t0) * 1000
        vr.record("UC connection created", True, f"{cfg.uc_connection_name}, {ms:.0f}ms")
    except RUNTIME_ERRORS as e:
        vr.record("UC connection created", False, str(e)[:120])

    try:
        df = spark.sql(f"DESCRIBE CONNECTION {cfg.uc_connection_name}")
        vr.record("UC connection described", df.count() > 0)
    except RUNTIME_ERRORS as e:
        vr.record("UC connection described", False, str(e)[:120])

    print("\n--- remote_query() Queries ---")
    record_remote_query(
        spark,
        cfg,
        vr,
        "remote_query SELECT 1",
        "SELECT 1 AS test",
        lambda rows: rows[0]["test"] == 1,
        lambda rows, ms: f"{ms:.0f}ms",
    )
    record_remote_query(
        spark,
        cfg,
        vr,
        "remote_query COUNT",
        "SELECT COUNT(*) AS flight_count FROM Flight",
        lambda rows: rows[0]["flight_count"] > 0,
        lambda rows, ms: f"{rows[0]['flight_count']} flights, {ms:.0f}ms",
    )
    record_remote_query(
        spark,
        cfg,
        vr,
        "remote_query JOIN aggregate",
        """
        SELECT COUNT(*) AS relationship_count
        FROM Flight f
        NATURAL JOIN DEPARTS_FROM r
        NATURAL JOIN Airport a
        """,
        lambda rows: rows[0]["relationship_count"] > 0,
        lambda rows, ms: f"{rows[0]['relationship_count']} relationships, {ms:.0f}ms",
    )
    record_remote_query(
        spark,
        cfg,
        vr,
        "remote_query WHERE aggregate",
        "SELECT COUNT(*) AS boeing_count FROM Aircraft WHERE manufacturer = 'Boeing'",
        lambda rows: rows[0]["boeing_count"] >= 0,
        lambda rows, ms: f"{rows[0]['boeing_count']} Boeing aircraft, {ms:.0f}ms",
    )
    record_remote_query(
        spark,
        cfg,
        vr,
        "remote_query multiple aggregates",
        """
        SELECT COUNT(*) AS total,
               MIN(aircraftId) AS first_id,
               MAX(aircraftId) AS last_id
        FROM Aircraft
        """,
        lambda rows: rows[0]["total"] > 0,
        lambda rows, ms: f"total={rows[0]['total']}, {ms:.0f}ms",
    )
    record_remote_query(
        spark,
        cfg,
        vr,
        "remote_query COUNT DISTINCT",
        "SELECT COUNT(DISTINCT manufacturer) AS unique_manufacturers FROM Aircraft",
        lambda rows: rows[0]["unique_manufacturers"] > 0,
        lambda rows, ms: f"{rows[0]['unique_manufacturers']} manufacturers, {ms:.0f}ms",
    )

    if not vr.summary():
        sys.exit(1)


def record_remote_query(spark, cfg, vr, name: str, query: str, validator, detail) -> None:
    try:
        t0 = time.time()
        rows = remote_query(spark, cfg, query).collect()
        ms = (time.time() - t0) * 1000
        vr.record(name, validator(rows), detail(rows, ms))
    except RUNTIME_ERRORS as e:
        vr.record(name, False, str(e)[:120])


if __name__ == "__main__":
    main()
