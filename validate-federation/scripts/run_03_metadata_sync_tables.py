"""Metadata sync: materialize Neo4j schema as Delta tables.

Discovers all node labels and relationship types from Neo4j, then materializes
each as a managed Delta table in a target catalog via the Spark Connector.

Node labels → {catalog}.nodes.{label}
Relationship types → {catalog}.relationships.{rel_type}

Usage:
    uv run python -m cli upload run_03_metadata_sync_tables.py
    uv run python -m cli submit run_03_metadata_sync_tables.py
"""

import re
import sys
import time
from collections import defaultdict

from data_utils import inject_params, get_config, get_neo4j_driver, ValidationResults

inject_params()
cfg = get_config()

TARGET_CATALOG = cfg["metadata_catalog"]
NODES_SCHEMA = cfg["nodes_schema"]
RELS_SCHEMA = cfg["relationships_schema"]

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Set Neo4j credentials at session level (avoids repeating per-query)
spark.conf.set("neo4j.url", cfg["neo4j_bolt_uri"])
spark.conf.set("neo4j.authentication.type", "basic")
spark.conf.set("neo4j.authentication.basic.username", cfg["neo4j_username"])
spark.conf.set("neo4j.authentication.basic.password", cfg["neo4j_password"])
spark.conf.set("neo4j.database", cfg["neo4j_database"])

vr = ValidationResults()

print("=" * 60)
print("validate-federation: 03 Metadata Sync (Delta Tables)")
print("=" * 60)
print(f"  Neo4j:    {cfg['neo4j_host']}")
print(f"  Target:   {TARGET_CATALOG}")
print(f"  Nodes:    {TARGET_CATALOG}.{NODES_SCHEMA}")
print(f"  Rels:     {TARGET_CATALOG}.{RELS_SCHEMA}")
print("")

# ============================================================================
# Section 1: Verify Neo4j Connectivity
# ============================================================================
print("--- Verify Neo4j ---")
try:
    driver = get_neo4j_driver(cfg)
    driver.verify_connectivity()
    with driver.session(database=cfg["neo4j_database"]) as session:
        val = session.run("RETURN 1 AS test").single()["test"]
    driver.close()
    vr.record("Neo4j connectivity", val == 1)
except Exception as e:
    vr.record("Neo4j connectivity", False, str(e)[:120])

# ============================================================================
# Section 2: Discover Neo4j Schema
# ============================================================================
print("\n--- Discover Schema ---")
discovered_labels = defaultdict(list)
discovered_relationships = defaultdict(list)
multi_label_skipped = 0

try:
    driver = get_neo4j_driver(cfg)
    with driver.session(database=cfg["neo4j_database"]) as session:
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

    driver.close()
    vr.record("Schema discovery: labels", len(discovered_labels) > 0,
              f"{len(discovered_labels)} labels")
    vr.record("Schema discovery: relationships", len(discovered_relationships) >= 0,
              f"{len(discovered_relationships)} types")

    for label, props in sorted(discovered_labels.items()):
        print(f"    :{label} — {len(props)} properties")
    for rel_type, props in sorted(discovered_relationships.items()):
        print(f"    [:{rel_type}] — {len(props)} properties")
    if multi_label_skipped > 0:
        print(f"    (skipped {multi_label_skipped} multi-label entries)")

except Exception as e:
    vr.record("Schema discovery", False, str(e)[:120])

# ============================================================================
# Section 3: Create Target Schemas
# ============================================================================
print("\n--- Create Target Schemas ---")
try:
    spark.sql(f"USE CATALOG `{TARGET_CATALOG}`")
    vr.record(f"Catalog: {TARGET_CATALOG}", True)
except Exception as e:
    vr.record(f"Catalog: {TARGET_CATALOG}", False, str(e)[:120])

for schema_name in [NODES_SCHEMA, RELS_SCHEMA]:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{TARGET_CATALOG}`.`{schema_name}`")
        vr.record(f"Schema: {schema_name}", True)
    except Exception as e:
        vr.record(f"Schema: {schema_name}", False, str(e)[:120])

# ============================================================================
# Section 4: Materialize Node Labels
# ============================================================================
print(f"\n--- Materialize Node Labels ({len(discovered_labels)}) ---")
label_results = []

for label in sorted(discovered_labels.keys()):
    tbl_name = label.lower()
    full_tbl = f"`{TARGET_CATALOG}`.`{NODES_SCHEMA}`.`{tbl_name}`"

    t0 = time.time()
    try:
        df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("labels", f":{label}") \
            .load()

        col_count = len(df.columns)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_tbl)

        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_tbl}").collect()[0]["cnt"]
        ms = (time.time() - t0) * 1000

        label_results.append({"label": label, "rows": row_count, "cols": col_count})
        vr.record(f"Label: {label}", row_count > 0,
                  f"{row_count} rows, {col_count} cols, {ms:.0f}ms")
    except Exception as e:
        vr.record(f"Label: {label}", False, str(e)[:120])

# ============================================================================
# Section 5: Materialize Relationship Types
# ============================================================================
print(f"\n--- Materialize Relationship Types ---")

# Discover relationship patterns (source → type → target)
rel_patterns = []
try:
    driver = get_neo4j_driver(cfg)
    with driver.session(database=cfg["neo4j_database"]) as session:
        result = session.run("""
            MATCH (src)-[r]->(tgt)
            WITH type(r) AS relType, labels(src) AS srcLabels, labels(tgt) AS tgtLabels
            RETURN DISTINCT relType, srcLabels[0] AS sourceLabel, tgtLabels[0] AS targetLabel
            ORDER BY relType
        """)
        for record in result:
            rel_patterns.append({
                "type": record["relType"],
                "source": record["sourceLabel"],
                "target": record["targetLabel"]
            })
    driver.close()
    print(f"  Found {len(rel_patterns)} relationship patterns")
except Exception as e:
    print(f"  [WARN] Could not discover patterns: {str(e)[:120]}")

rel_results = []

for pattern in rel_patterns:
    rel_type = pattern["type"]
    tbl_name = rel_type.lower()
    full_tbl = f"`{TARGET_CATALOG}`.`{RELS_SCHEMA}`.`{tbl_name}`"

    t0 = time.time()
    try:
        df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("relationship", rel_type) \
            .option("relationship.source.labels", f":{pattern['source']}") \
            .option("relationship.target.labels", f":{pattern['target']}") \
            .load()

        col_count = len(df.columns)
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_tbl)

        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_tbl}").collect()[0]["cnt"]
        ms = (time.time() - t0) * 1000

        rel_results.append({"type": rel_type, "rows": row_count, "cols": col_count})
        vr.record(f"Rel: {rel_type}", row_count > 0,
                  f"{row_count} rows, {col_count} cols, {ms:.0f}ms")
    except Exception as e:
        vr.record(f"Rel: {rel_type}", False, str(e)[:120])

# ============================================================================
# Section 6: Verify via INFORMATION_SCHEMA
# ============================================================================
print("\n--- Verify INFORMATION_SCHEMA ---")
try:
    tables_df = spark.sql(f"""
        SELECT table_schema, table_name, table_type
        FROM `{TARGET_CATALOG}`.information_schema.tables
        WHERE table_schema IN ('{NODES_SCHEMA}', '{RELS_SCHEMA}')
        ORDER BY table_schema, table_name
    """)
    table_count = tables_df.count()
    expected = len(label_results) + len(rel_results)
    tables_df.show(50, truncate=False)
    vr.record("INFORMATION_SCHEMA tables", table_count >= expected,
              f"{table_count} tables found")
except Exception as e:
    vr.record("INFORMATION_SCHEMA tables", False, str(e)[:120])

# ============================================================================
# Summary
# ============================================================================
total_rows = (sum(r["rows"] for r in label_results) +
              sum(r["rows"] for r in rel_results))
print(f"\n  Total data rows materialized: {total_rows:,}")

all_passed = vr.summary()
if not all_passed:
    sys.exit(1)
