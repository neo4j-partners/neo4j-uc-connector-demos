"""Extra regression coverage for Neo4j connectivity and UC JDBC connection.

Covers: Python driver, Spark Connector, direct JDBC (dbtable, SQL translation,
aggregate, NATURAL JOIN), UC connection creation, and UC JDBC queries
(basic, remote_query, COUNT, JOIN, WHERE, multiple aggregates, COUNT DISTINCT).

Usage:
    uv run python validate.py run --include-extras
    uv run python validate.py submit run_extra_connection_smoke.py
"""

import sys
import time

from data_utils import (
    ValidationResults,
    get_config,
    get_neo4j_driver,
    inject_params,
    read_neo4j_jdbc,
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
    print(f"  Neo4j Host:    {cfg.neo4j_host}")
    print(f"  Bolt URI:      {cfg.neo4j_bolt_uri}")
    print(f"  UC Connection: {cfg.uc_connection_name}")
    print(f"  JDBC JAR:      {cfg.jdbc_jar_path}")
    print("")

    # ============================================================================
    # Section 1: Environment Information
    # ============================================================================
    print("--- Environment ---")
    print(f"  Spark: {spark.version}")
    print(f"  Python: {sys.version}")
    try:
        import neo4j
        print(f"  Neo4j Python Driver: {neo4j.__version__}")
    except ImportError:
        print("  Neo4j Python Driver: NOT INSTALLED")

    # ============================================================================
    # Section 2: Neo4j Python Driver
    # ============================================================================
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
    except Exception as e:
        vr.record("Python driver connectivity", False, str(e)[:120])

    # ============================================================================
    # Section 3: Neo4j Spark Connector
    # ============================================================================
    print("\n--- Neo4j Spark Connector ---")
    try:
        t0 = time.time()
        df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", cfg.neo4j_bolt_uri) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", cfg.neo4j_username) \
            .option("authentication.basic.password", cfg.neo4j_password) \
            .option("query", "RETURN 'ok' AS message, 1 AS value") \
            .load()
        rows = df.collect()
        ms = (time.time() - t0) * 1000
        vr.record("Spark Connector", len(rows) == 1 and rows[0]["value"] == 1, f"{ms:.0f}ms")
    except Exception as e:
        vr.record("Spark Connector", False, str(e)[:120])

    # ============================================================================
    # Section 4: Direct JDBC Tests
    # ============================================================================
    print("\n--- Direct JDBC ---")

    # 4a: dbtable — read Aircraft label
    AIRCRAFT_SCHEMA = "`v$id` STRING, aircraftId STRING, tail_number STRING, icao24 STRING, model STRING, operator STRING, manufacturer STRING"
    try:
        t0 = time.time()
        df = spark.read.format("jdbc") \
            .option("url", cfg.neo4j_jdbc_url_sql) \
            .option("driver", "org.neo4j.jdbc.Neo4jDriver") \
            .option("user", cfg.neo4j_username) \
            .option("password", cfg.neo4j_password) \
            .option("dbtable", "Aircraft") \
            .option("customSchema", AIRCRAFT_SCHEMA) \
            .load()
        rows = df.take(5)
        ms = (time.time() - t0) * 1000
        vr.record("Direct JDBC dbtable (Aircraft)", len(rows) > 0, f"{len(rows)} rows, {ms:.0f}ms")
    except Exception as e:
        vr.record("Direct JDBC dbtable (Aircraft)", False, str(e)[:120])

    # 4b: SQL translation — SELECT 1
    try:
        t0 = time.time()
        df = spark.read.format("jdbc") \
            .option("url", cfg.neo4j_jdbc_url_sql) \
            .option("driver", "org.neo4j.jdbc.Neo4jDriver") \
            .option("user", cfg.neo4j_username) \
            .option("password", cfg.neo4j_password) \
            .option("query", "SELECT 1 AS value") \
            .option("customSchema", "value INT") \
            .load()
        val = df.collect()[0]["value"]
        ms = (time.time() - t0) * 1000
        vr.record("Direct JDBC SQL translation", val == 1, f"{ms:.0f}ms")
    except Exception as e:
        vr.record("Direct JDBC SQL translation", False, str(e)[:120])

    # 4c: SQL aggregate — COUNT(*)
    try:
        t0 = time.time()
        df = spark.read.format("jdbc") \
            .option("url", cfg.neo4j_jdbc_url_sql) \
            .option("driver", "org.neo4j.jdbc.Neo4jDriver") \
            .option("user", cfg.neo4j_username) \
            .option("password", cfg.neo4j_password) \
            .option("query", "SELECT COUNT(*) AS flight_count FROM Flight") \
            .option("customSchema", "flight_count LONG") \
            .load()
        cnt = df.collect()[0]["flight_count"]
        ms = (time.time() - t0) * 1000
        vr.record("Direct JDBC COUNT aggregate", cnt > 0, f"{cnt} flights, {ms:.0f}ms")
    except Exception as e:
        vr.record("Direct JDBC COUNT aggregate", False, str(e)[:120])

    # 4d: SQL JOIN — NATURAL JOIN (graph traversal)
    try:
        t0 = time.time()
        df = spark.read.format("jdbc") \
            .option("url", cfg.neo4j_jdbc_url_sql) \
            .option("driver", "org.neo4j.jdbc.Neo4jDriver") \
            .option("user", cfg.neo4j_username) \
            .option("password", cfg.neo4j_password) \
            .option("query", """SELECT COUNT(*) AS cnt
                               FROM Flight f
                               NATURAL JOIN DEPARTS_FROM r
                               NATURAL JOIN Airport a""") \
            .option("customSchema", "cnt LONG") \
            .load()
        cnt = df.collect()[0]["cnt"]
        ms = (time.time() - t0) * 1000
        vr.record("Direct JDBC NATURAL JOIN", cnt > 0, f"{cnt} relationships, {ms:.0f}ms")
    except Exception as e:
        vr.record("Direct JDBC NATURAL JOIN", False, str(e)[:120])

    # ============================================================================
    # Section 5: UC Connection Creation
    # ============================================================================
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
          user '{cfg.neo4j_username}',
          password '{cfg.neo4j_password}',
          driver 'org.neo4j.jdbc.Neo4jDriver',
          externalOptionsAllowList 'dbtable,query,partitionColumn,lowerBound,upperBound,numPartitions,fetchSize,customSchema'
        )
        """
        t0 = time.time()
        spark.sql(create_sql)
        ms = (time.time() - t0) * 1000
        vr.record("UC connection created", True, f"{cfg.uc_connection_name}, {ms:.0f}ms")
    except Exception as e:
        vr.record("UC connection created", False, str(e)[:120])

    # Verify connection
    try:
        df = spark.sql(f"DESCRIBE CONNECTION {cfg.uc_connection_name}")
        vr.record("UC connection described", df.count() > 0)
    except Exception as e:
        vr.record("UC connection described", False, str(e)[:120])

    # ============================================================================
    # Section 6: UC JDBC Queries
    # ============================================================================
    print("\n--- UC JDBC Queries ---")

    # 6a: Basic query — SELECT 1
    try:
        t0 = time.time()
        df = read_neo4j_jdbc(spark, cfg, "test INT", query="SELECT 1 AS test")
        val = df.collect()[0]["test"]
        ms = (time.time() - t0) * 1000
        vr.record("UC JDBC basic query", val == 1, f"{ms:.0f}ms")
    except Exception as e:
        vr.record("UC JDBC basic query", False, str(e)[:120])

    # 6b: remote_query() — SELECT 1
    try:
        t0 = time.time()
        df = spark.sql(f"""
            SELECT * FROM remote_query('{cfg.uc_connection_name}',
                query => 'SELECT 1 AS test')
        """)
        val = df.collect()[0]["test"]
        ms = (time.time() - t0) * 1000
        vr.record("UC remote_query()", val == 1, f"{ms:.0f}ms")
    except Exception as e:
        vr.record("UC remote_query()", False, str(e)[:120])

    # 6c: COUNT aggregate
    try:
        t0 = time.time()
        df = read_neo4j_jdbc(spark, cfg, "flight_count LONG",
                             query="SELECT COUNT(*) AS flight_count FROM Flight")
        cnt = df.collect()[0]["flight_count"]
        ms = (time.time() - t0) * 1000
        vr.record("UC JDBC COUNT", cnt > 0, f"{cnt} flights, {ms:.0f}ms")
    except Exception as e:
        vr.record("UC JDBC COUNT", False, str(e)[:120])

    # 6d: JOIN with aggregate
    try:
        t0 = time.time()
        df = read_neo4j_jdbc(spark, cfg, "relationship_count LONG",
                             query="""SELECT COUNT(*) AS relationship_count
                                      FROM Flight f
                                      NATURAL JOIN DEPARTS_FROM r
                                      NATURAL JOIN Airport a""")
        cnt = df.collect()[0]["relationship_count"]
        ms = (time.time() - t0) * 1000
        vr.record("UC JDBC JOIN aggregate", cnt > 0, f"{cnt} relationships, {ms:.0f}ms")
    except Exception as e:
        vr.record("UC JDBC JOIN aggregate", False, str(e)[:120])

    # 6e: Aggregate with WHERE
    try:
        t0 = time.time()
        df = read_neo4j_jdbc(spark, cfg, "boeing_count LONG",
                             query="SELECT COUNT(*) AS boeing_count FROM Aircraft WHERE manufacturer = 'Boeing'")
        cnt = df.collect()[0]["boeing_count"]
        ms = (time.time() - t0) * 1000
        vr.record("UC JDBC WHERE aggregate", cnt >= 0, f"{cnt} Boeing aircraft, {ms:.0f}ms")
    except Exception as e:
        vr.record("UC JDBC WHERE aggregate", False, str(e)[:120])

    # 6f: Multiple aggregates
    try:
        t0 = time.time()
        df = read_neo4j_jdbc(spark, cfg, "total LONG, first_id STRING, last_id STRING",
                             query="""SELECT COUNT(*) AS total,
                                             MIN(aircraftId) AS first_id,
                                             MAX(aircraftId) AS last_id
                                      FROM Aircraft""")
        row = df.collect()[0]
        ms = (time.time() - t0) * 1000
        vr.record("UC JDBC multiple aggregates", row["total"] > 0,
                  f"total={row['total']}, {ms:.0f}ms")
    except Exception as e:
        vr.record("UC JDBC multiple aggregates", False, str(e)[:120])

    # 6g: COUNT DISTINCT
    try:
        t0 = time.time()
        df = read_neo4j_jdbc(spark, cfg, "unique_manufacturers LONG",
                             query="SELECT COUNT(DISTINCT manufacturer) AS unique_manufacturers FROM Aircraft")
        cnt = df.collect()[0]["unique_manufacturers"]
        ms = (time.time() - t0) * 1000
        vr.record("UC JDBC COUNT DISTINCT", cnt > 0, f"{cnt} manufacturers, {ms:.0f}ms")
    except Exception as e:
        vr.record("UC JDBC COUNT DISTINCT", False, str(e)[:120])

    if not vr.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
