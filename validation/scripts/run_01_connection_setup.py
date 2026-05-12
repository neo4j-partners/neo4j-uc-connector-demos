"""Connection setup and validation for getting-started notebooks.

Creates the UC JDBC connection and validates the same remote_query() reads as
getting-started/01-neo4j-uc-connection-setup.ipynb. It also prepares the Delta
tables needed by downstream validation tasks.

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
    remote_query,
)


def main() -> None:
    inject_params()
    cfg = get_config()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    results = ValidationResults()

    fqn = f"`{cfg.uc_catalog}`.`{cfg.uc_schema}`"
    volume_path = cfg.volume_path

    print("=" * 60)
    print("validation: 01 Connection Setup")
    print("=" * 60)
    print("Notebook: getting-started/01-neo4j-uc-connection-setup.ipynb")
    print(f"  Tables:        {fqn}.*")
    print(f"  Volume:        {volume_path}")
    print(f"  UC Connection: {cfg.uc_connection_name}")
    print("")

    print("--- Validation Setup: Load sensor_readings Delta Table ---")
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}.`{cfg.uc_volume}`")
        results.record("Create tutorial schema and volume", True, fqn)
    except RUNTIME_ERRORS as exc:
        results.record("Create tutorial schema and volume", False, str(exc)[:160])

    try:
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {fqn}.sensor_readings AS
            SELECT
                reading_id AS readingId,
                sensor_id AS sensorId,
                ts,
                value
            FROM read_files('{volume_path}/nodes_readings.csv',
                format => 'csv',
                header => true,
                inferColumnTypes => true)
            """
        )
        count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {fqn}.sensor_readings"
        ).collect()[0]["cnt"]
        results.record(
            "Load sensor_readings",
            count == 172800,
            f"{count:,} rows",
        )
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

    create_lakehouse_helper_tables(spark, cfg, results)

    print("\n--- Section 2: Create UC JDBC Connection ---")
    connection_created = False
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
        start = time.time()
        spark.sql(create_sql)
        elapsed = (time.time() - start) * 1000
        connection_created = True
        results.record(
            "Create UC JDBC connection",
            True,
            f"{cfg.uc_connection_name}, {elapsed:.0f}ms",
        )
    except RUNTIME_ERRORS as exc:
        results.record("Create UC JDBC connection", False, str(exc)[:200])

    if connection_created:
        try:
            test_val = remote_query(spark, cfg, "SELECT 1 AS test").collect()[0]["test"]
            results.record("Validate remote_query SELECT 1", test_val == 1, str(test_val))
        except RUNTIME_ERRORS as exc:
            results.record(
                "Validate remote_query SELECT 1",
                False,
                f"connection created but query failed: {str(exc)[:160]}",
            )

    print("\n--- Section 3: Query via remote_query() ---")
    record_query(
        spark, cfg, results, connection_created,
        "Aircraft count",
        "SELECT COUNT(*) AS aircraft_count FROM Aircraft",
        lambda rows: rows[0]["aircraft_count"] == 20,
        lambda rows: str(rows[0]["aircraft_count"]),
    )
    record_query(
        spark, cfg, results, connection_created,
        "Airport count",
        "SELECT COUNT(*) AS airport_count FROM Airport",
        lambda rows: rows[0]["airport_count"] == 12,
        lambda rows: str(rows[0]["airport_count"]),
    )
    try:
        df = (
            remote_query(
                spark,
                cfg,
                """SELECT operator, COUNT(*) AS flight_count
                   FROM Flight
                   GROUP BY operator
                   HAVING COUNT(*) > 0
                   ORDER BY flight_count DESC""",
            )
            if connection_created
            else None
        )
        if df is None:
            results.record(
                "Flights by operator",
                False,
                "skipped: connection not created",
            )
        else:
            rows = df.collect()
            results.record("Flights by operator", len(rows) > 0, f"{len(rows)} rows")
            df.show(truncate=False)
    except RUNTIME_ERRORS as exc:
        results.record(
            "Flights by operator",
            False,
            f"connection created but query failed: {str(exc)[:140]}",
        )

    if not results.summary():
        sys.exit(1)


def record_query(
    spark,
    cfg: Config,
    results: ValidationResults,
    connection_created: bool,
    name: str,
    query: str,
    validator,
    detail,
) -> None:
    """Run a notebook query and tag failures so connection-creation issues are distinguishable."""
    if not connection_created:
        results.record(name, False, "skipped: connection not created")
        return
    try:
        rows = remote_query(spark, cfg, query).collect()
        results.record(name, validator(rows), detail(rows))
    except RUNTIME_ERRORS as exc:
        results.record(
            name,
            False,
            f"connection created but query failed: {str(exc)[:140]}",
        )


def create_lakehouse_helper_tables(spark, cfg: Config, results: ValidationResults) -> None:
    """Create Delta helper tables required by advanced-patterns/06.

    The getting-started notebooks use the tutorial schema. The advanced
    notebook uses UC_CATALOG.LAKEHOUSE_SCHEMA for normalized Delta
    tables, so validation creates those from the same uploaded CSV data instead
    of assuming they already exist.
    """
    lakehouse_fqn = cfg.lakehouse_fqn
    volume_path = cfg.volume_path

    print("\n--- Validation Setup: Lakehouse Helper Tables ---")
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {lakehouse_fqn}")
        results.record("Create lakehouse schema", True, lakehouse_fqn)
    except RUNTIME_ERRORS as exc:
        results.record("Create lakehouse schema", False, str(exc)[:160])
        return

    table_specs = [
        (
            "aircraft",
            "nodes_aircraft.csv",
            """
            SELECT
                `:ID(Aircraft)` AS aircraftId,
                tail_number,
                icao24,
                model,
                manufacturer,
                operator
            FROM source
            """,
            20,
        ),
        (
            "systems",
            "nodes_systems.csv",
            """
            SELECT
                `:ID(System)` AS systemId,
                aircraft_id AS aircraftId,
                type,
                name
            FROM source
            """,
            80,
        ),
        (
            "sensors",
            "nodes_sensors.csv",
            """
            SELECT
                `:ID(Sensor)` AS sensorId,
                system_id AS systemId,
                type,
                name,
                unit
            FROM source
            """,
            160,
        ),
        (
            "sensor_readings",
            "nodes_readings.csv",
            """
            SELECT
                reading_id AS readingId,
                sensor_id AS sensorId,
                ts,
                value
            FROM source
            """,
            172800,
        ),
    ]

    for table_name, file_name, projection_sql, expected_rows in table_specs:
        try:
            source = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(f"{volume_path}/{file_name}")
            )
            source.createOrReplaceTempView("source")
            df = spark.sql(projection_sql)
            full_table = f"{lakehouse_fqn}.`{table_name}`"
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(full_table)
            count = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {full_table}"
            ).collect()[0]["cnt"]
            results.record(
                f"Lakehouse helper table: {table_name}",
                count == expected_rows,
                f"{count:,} rows",
            )
        except RUNTIME_ERRORS as exc:
            results.record(
                f"Lakehouse helper table: {table_name}",
                False,
                str(exc)[:200],
            )


if __name__ == "__main__":
    main()
