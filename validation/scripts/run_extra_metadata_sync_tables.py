"""Extra metadata regression coverage: materialize Neo4j schema as Delta tables.

Discovers all node labels and relationship types from Neo4j, then materializes
each as a managed Delta table in a target catalog via the UC JDBC connection.

Node labels → {catalog}.nodes.{label}
Relationship types → {catalog}.relationships.{rel_type}

Usage (via the DAB job):
    uv run python validate.py extras             # runs as part of the extras job
    uv run python validate.py metadata           # runs as part of the metadata job
    databricks bundle run extras                 # equivalent, direct bundle call
"""

import re
import sys
import time
from collections import defaultdict

from data_utils import (
    RUNTIME_ERRORS,
    Config,
    ValidationResults,
    get_config,
    get_neo4j_driver,
    inject_params,
    remote_query,
)

GETTING_STARTED_SCHEMA = "getting_started"


def cast_all_to_string(df):
    """Match the notebook's CHAR(0)-safe Delta write workaround."""
    from pyspark.sql.types import StringType
    for col_name in df.columns:
        df = df.withColumn(col_name, df[col_name].cast(StringType()))
    return df


def materialize_remote_query_table(
    spark,
    cfg: Config,
    vr: ValidationResults,
    target_catalog: str,
    schema: str,
    results_log: list,
    table_name: str,
    query: str,
    expected_rows: int,
    select_columns: list | None = None,
) -> None:
    """Read from Neo4j via remote_query() and write as a managed Delta table."""
    full_tbl = f"`{target_catalog}`.`{schema}`.`{table_name}`"
    t0 = time.time()
    try:
        df = remote_query(spark, cfg, query)
        if select_columns:
            df = df.select(*select_columns)
        df = cast_all_to_string(df)
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(full_tbl)
        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_tbl}").collect()[0]["cnt"]
        ms = (time.time() - t0) * 1000
        results_log.append({"table": table_name, "rows": row_count})
        vr.record(
            f"Getting Started materialized: {table_name}",
            row_count == expected_rows,
            f"{row_count} rows, {ms:.0f}ms",
        )
    except RUNTIME_ERRORS as e:
        vr.record(f"Getting Started materialized: {table_name}", False, str(e)[:160])


def id_property_for_label(label: str, discovered_labels) -> str | None:
    """Best-effort identifier property for relationship table materialization."""
    props = {prop["name"] for prop in discovered_labels.get(label, [])}
    preferred = [
        f"{label[:1].lower()}{label[1:]}Id",
        "id",
        "eventId",
    ]
    for name in preferred:
        if name in props:
            return name
    id_like = sorted(name for name in props if name.lower().endswith("id"))
    return id_like[0] if id_like else None


def main() -> None:
    inject_params()
    cfg = get_config()

    target_catalog = cfg.metadata_catalog
    nodes_schema = cfg.nodes_schema
    rels_schema = cfg.relationships_schema

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    vr = ValidationResults()

    print("=" * 60)
    print("validation: 03 Metadata Sync (Delta Tables)")
    print("=" * 60)
    print(f"  Neo4j:    {cfg.neo4j_uri}")
    print(f"  Target:   {target_catalog}")
    print(f"  Nodes:    {target_catalog}.{nodes_schema}")
    print(f"  Rels:     {target_catalog}.{rels_schema}")
    print(f"  Tutorial: {target_catalog}.{GETTING_STARTED_SCHEMA}")
    print("")

    # ============================================================================
    # Section 1: Verify Neo4j Connectivity
    # ============================================================================
    print("--- Verify Neo4j ---")
    try:
        driver = get_neo4j_driver(cfg)
        driver.verify_connectivity()
        with driver.session(database=cfg.neo4j_database) as session:
            val = session.run("RETURN 1 AS test").single()["test"]
        driver.close()
        vr.record("Neo4j connectivity", val == 1)
    except RUNTIME_ERRORS as e:
        vr.record("Neo4j connectivity", False, str(e)[:120])

    # ============================================================================
    # Section 2: Discover Neo4j Schema
    # ============================================================================
    print("\n--- Discover Schema ---")
    discovered_labels = defaultdict(list)
    discovered_relationships = defaultdict(list)
    relationship_patterns = []
    relationship_counts = {}
    multi_label_skipped = 0

    try:
        driver = get_neo4j_driver(cfg)
        with driver.session(database=cfg.neo4j_database) as session:
            # Node label properties
            result = session.run("CALL db.schema.nodeTypeProperties()")
            for record in result:
                if record["propertyName"] is None:
                    continue
                labels = record["nodeLabels"]
                if len(labels) == 1:
                    discovered_labels[labels[0]].append({
                        "name": record["propertyName"],
                        "types": record["propertyTypes"],
                        "mandatory": record["mandatory"]
                    })
                else:
                    multi_label_skipped += 1

            # Relationship type properties
            result = session.run("CALL db.schema.relTypeProperties()")
            for record in result:
                if record["propertyName"] is None:
                    continue
                raw = record["relType"]
                rel_type = re.sub(r'^:`|`$', '', raw)
                discovered_relationships[rel_type].append({
                    "name": record["propertyName"],
                    "types": record["propertyTypes"],
                    "mandatory": record["mandatory"]
                })

            result = session.run("""
                MATCH (src)-[r]->(tgt)
                WITH type(r) AS relType,
                     labels(src)[0] AS sourceLabel,
                     labels(tgt)[0] AS targetLabel,
                     count(r) AS relCount
                RETURN relType, sourceLabel, targetLabel, relCount
                ORDER BY relType
            """)
            for record in result:
                rel_type = record["relType"]
                discovered_relationships.setdefault(rel_type, [])
                relationship_patterns.append({
                    "type": rel_type,
                    "source": record["sourceLabel"],
                    "target": record["targetLabel"],
                    "count": record["relCount"],
                })
                relationship_counts[rel_type] = (
                    relationship_counts.get(rel_type, 0) + record["relCount"]
                )

        driver.close()
        vr.record("Schema discovery: labels", len(discovered_labels) > 0,
                  f"{len(discovered_labels)} labels")
        vr.record("Schema discovery: relationships", len(relationship_counts) > 0,
                  f"{len(relationship_counts)} types")

        for label, props in sorted(discovered_labels.items()):
            print(f"    :{label}, {len(props)} properties")
        for rel_type, props in sorted(discovered_relationships.items()):
            print(f"    [:{rel_type}], {len(props)} properties")
        if multi_label_skipped > 0:
            print(f"    (skipped {multi_label_skipped} multi-label entries)")

    except RUNTIME_ERRORS as e:
        vr.record("Schema discovery", False, str(e)[:120])

    # ============================================================================
    # Section 3: Create Target Schemas
    # ============================================================================
    print("\n--- Create Target Schemas ---")
    try:
        spark.sql(f"USE CATALOG `{target_catalog}`")
        vr.record(f"Catalog: {target_catalog}", True)
    except RUNTIME_ERRORS as e:
        vr.record(f"Catalog: {target_catalog}", False, str(e)[:120])

    for schema_name in [nodes_schema, rels_schema, GETTING_STARTED_SCHEMA]:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{schema_name}`")
            vr.record(f"Schema: {schema_name}", True)
        except RUNTIME_ERRORS as e:
            vr.record(f"Schema: {schema_name}", False, str(e)[:120])

    # ============================================================================
    # Section 4: Getting Started Materialized Tables Pattern
    # ============================================================================
    print("\n--- Getting Started Pattern: remote_query() Materialized Tables ---")
    getting_started_results = []

    def materialize_getting_started(table_name, query, expected_rows, select_columns=None):
        materialize_remote_query_table(
            spark, cfg, vr, target_catalog, GETTING_STARTED_SCHEMA,
            getting_started_results, table_name, query,
            expected_rows, select_columns=select_columns,
        )

    materialize_getting_started(
        "neo4j_aircraft",
        "SELECT aircraftId, tail_number, icao24, model, manufacturer, operator FROM Aircraft",
        20,
    )
    materialize_getting_started(
        "neo4j_airports",
        "SELECT airportId, name, city, country, iata, icao FROM Airport",
        12,
    )
    materialize_getting_started(
        "neo4j_systems",
        "SELECT systemId, aircraftId, type, name FROM System",
        80,
    )
    materialize_getting_started(
        "neo4j_sensors",
        "SELECT sensorId, systemId, type, name, unit FROM Sensor",
        160,
    )
    materialize_getting_started(
        "neo4j_components",
        "SELECT componentId, systemId, type, name FROM Component",
        320,
    )
    materialize_getting_started(
        "neo4j_maintenance_events",
        "SELECT eventId, componentId, systemId, aircraftId, fault, severity, reported_at, corrective_action FROM MaintenanceEvent",
        300,
    )
    materialize_getting_started(
        "neo4j_flights",
        "SELECT flightId, flight_number, aircraftId, operator, origin, destination, scheduled_departure, scheduled_arrival FROM Flight",
        800,
    )
    materialize_getting_started(
        "neo4j_delays",
        "SELECT delayId, flightId, cause, CAST(minutes AS STRING) AS minutes FROM Delay",
        514,
    )
    materialize_getting_started(
        "neo4j_aircraft_systems",
        """SELECT a.aircraftId AS aircraftId, a.model AS model,
                  s.systemId AS systemId, s.type AS systemType, s.name AS systemName,
                  COUNT(*) AS cnt
           FROM Aircraft a NATURAL JOIN HAS_SYSTEM rel NATURAL JOIN System s
           GROUP BY a.aircraftId, a.model, s.systemId, s.type, s.name
           HAVING COUNT(*) > 0""",
        80,
        select_columns=["aircraftId", "model", "systemId", "systemType", "systemName"],
    )

    # ============================================================================
    # Section 5: Materialize Node Labels
    # ============================================================================
    print(f"\n--- Materialize Node Labels ({len(discovered_labels)}) ---")
    label_results = []

    for label in sorted(discovered_labels.keys()):
        tbl_name = label.lower()
        full_tbl = f"`{target_catalog}`.`{nodes_schema}`.`{tbl_name}`"
        props = sorted({prop["name"] for prop in discovered_labels[label]})
        select_list = ", ".join(props)
        t0 = time.time()
        try:
            if not props:
                vr.record(f"Label: {label}", False, "no properties discovered")
                continue

            df = remote_query(
                spark,
                cfg,
                f"SELECT {select_list} FROM {label}",
            )

            col_count = len(df.columns)
            df = cast_all_to_string(df)
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_tbl)

            row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_tbl}").collect()[0]["cnt"]
            ms = (time.time() - t0) * 1000

            label_results.append({"label": label, "rows": row_count, "cols": col_count})
            vr.record(f"Label: {label}", row_count > 0,
                      f"{row_count} rows, {col_count} cols, {ms:.0f}ms")
        except RUNTIME_ERRORS as e:
            vr.record(f"Label: {label}", False, str(e)[:120])

    # ============================================================================
    # Section 6: Materialize Relationship Types
    # ============================================================================
    print("\n--- Materialize Relationship Types ---")

    vr.record(
        "Relationship pattern discovery",
        len(relationship_patterns) > 0,
        f"{len(relationship_patterns)} patterns",
    )

    rel_results = []
    patterns_by_type = defaultdict(list)
    for pattern in relationship_patterns:
        patterns_by_type[pattern["type"]].append(pattern)

    for rel_type, patterns in sorted(patterns_by_type.items()):
        tbl_name = rel_type.lower()
        full_tbl = f"`{target_catalog}`.`{rels_schema}`.`{tbl_name}`"

        t0 = time.time()
        try:
            dfs = []
            for index, pattern in enumerate(patterns):
                source_id = id_property_for_label(pattern["source"], discovered_labels)
                target_id = id_property_for_label(pattern["target"], discovered_labels)
                if not source_id or not target_id:
                    raise ValueError(
                        "missing id property for "
                        f"{pattern['source']} -[:{rel_type}]-> {pattern['target']}"
                    )
                dfs.append(
                    remote_query(
                        spark,
                        cfg,
                        f"""SELECT s.{source_id} AS source_id,
                                  t.{target_id} AS target_id,
                                  '{pattern["source"]}' AS source_label,
                                  '{pattern["target"]}' AS target_label,
                                  '{index}' AS pattern_index
                           FROM {pattern["source"]} s
                           NATURAL JOIN {rel_type} r
                           NATURAL JOIN {pattern["target"]} t""",
                    )
                )

            df = dfs[0]
            for extra_df in dfs[1:]:
                df = df.unionByName(extra_df)

            col_count = len(df.columns)
            df = cast_all_to_string(df)
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_tbl)

            row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_tbl}").collect()[0]["cnt"]
            ms = (time.time() - t0) * 1000
            expected_rows = relationship_counts.get(rel_type)

            rel_results.append({"type": rel_type, "rows": row_count, "cols": col_count})
            vr.record(f"Rel: {rel_type}", row_count == expected_rows,
                      f"{row_count}/{expected_rows} rows, {col_count} cols, {ms:.0f}ms")
        except RUNTIME_ERRORS as e:
            vr.record(f"Rel: {rel_type}", False, str(e)[:120])

    materialized_rel_types = {r["type"] for r in rel_results}
    missing_rel_types = sorted(set(relationship_counts) - materialized_rel_types)
    vr.record(
        "Relationship type materialization coverage",
        not missing_rel_types,
        "all discovered relationship types"
        if not missing_rel_types
        else f"missing: {', '.join(missing_rel_types)}",
    )

    # ============================================================================
    # Section 7: Verify via INFORMATION_SCHEMA
    # ============================================================================
    print("\n--- Verify INFORMATION_SCHEMA ---")
    try:
        tables_df = spark.sql(f"""
            SELECT table_schema, table_name, table_type
            FROM `{target_catalog}`.information_schema.tables
            WHERE table_schema IN ('{nodes_schema}', '{rels_schema}', '{GETTING_STARTED_SCHEMA}')
            ORDER BY table_schema, table_name
        """)
        table_count = tables_df.count()
        rows = tables_df.collect()
        table_names = {
            (row["table_schema"], row["table_name"])
            for row in rows
        }
        expected_node_tables = {
            (nodes_schema, r["label"].lower())
            for r in label_results
        }
        expected_rel_tables = {
            (rels_schema, rel_type.lower())
            for rel_type in relationship_counts
        }
        expected_getting_started_tables = {
            (GETTING_STARTED_SCHEMA, r["table"])
            for r in getting_started_results
        }
        expected = (
            expected_node_tables
            | expected_rel_tables
            | expected_getting_started_tables
        )
        missing_tables = sorted(expected - table_names)
        tables_df.show(50, truncate=False)
        vr.record("INFORMATION_SCHEMA tables", not missing_tables,
                  f"{table_count} tables found" if not missing_tables else f"missing: {missing_tables}")
    except RUNTIME_ERRORS as e:
        vr.record("INFORMATION_SCHEMA tables", False, str(e)[:120])

    total_rows = (sum(r["rows"] for r in label_results) +
                  sum(r["rows"] for r in rel_results))
    print(f"\n  Total data rows materialized: {total_rows:,}")

    if not vr.summary():
        sys.exit(1)


if __name__ == "__main__":
    main()
