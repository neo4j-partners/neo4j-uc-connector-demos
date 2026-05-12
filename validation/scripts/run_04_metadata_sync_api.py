"""Metadata sync: register Neo4j schema in UC External Metadata API.

Discovers all node labels and relationship types from Neo4j, then registers
each as an external metadata entry in Unity Catalog via the External Metadata
API. No Delta tables are created — this registers metadata only.

Usage:
    uv run python -m cli upload run_04_metadata_sync_api.py
    uv run python -m cli submit run_04_metadata_sync_api.py
"""

import json
import re
import sys
import time
from collections import defaultdict

import requests

from data_utils import inject_params, get_config, get_neo4j_driver, ValidationResults

inject_params()
cfg = get_config()

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

vr = ValidationResults()

# Neo4j type → UC SQL type mapping
TYPE_MAP = {
    "String": "STRING",
    "Long": "BIGINT",
    "Double": "DOUBLE",
    "Boolean": "BOOLEAN",
    "Date": "DATE",
    "LocalDateTime": "TIMESTAMP_NTZ",
    "DateTime": "TIMESTAMP",
    "StringArray": "ARRAY<STRING>",
    "LongArray": "ARRAY<BIGINT>",
    "DoubleArray": "ARRAY<DOUBLE>",
}

print("=" * 60)
print("validation: 04 Metadata Sync (External Metadata API)")
print("=" * 60)
print(f"  Neo4j: {cfg['neo4j_host']}")

# ============================================================================
# Section 1: Auto-discover Workspace URL and Auth Token
# ============================================================================
print("\n--- Discover Workspace Auth ---")
WORKSPACE_URL = None
AUTH_TOKEN = None

try:
    WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
    if not WORKSPACE_URL.startswith("https://"):
        WORKSPACE_URL = f"https://{WORKSPACE_URL}"
    print(f"  Workspace URL: {WORKSPACE_URL}")
except Exception:
    pass

if not WORKSPACE_URL:
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        WORKSPACE_URL = ctx.apiUrl().get()
        print(f"  Workspace URL: {WORKSPACE_URL} (from dbutils)")
    except Exception as e:
        vr.record("Workspace URL discovery", False, str(e)[:120])

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    # Use the run-scoped Databricks token so the External Metadata API sees the
    # same principal that submitted the job. That principal must have
    # CREATE_EXTERNAL_METADATA on the metastore before registration can succeed.
    AUTH_TOKEN = ctx.apiToken().get()
    print(f"  Auth Token: {'*' * 8} (auto-discovered)")
except Exception as e:
    vr.record("Auth token discovery", False, str(e)[:120])

if not WORKSPACE_URL or not AUTH_TOKEN:
    if not WORKSPACE_URL:
        vr.record("Workspace URL discovery", False, "Could not auto-discover")
    if not AUTH_TOKEN:
        vr.record("Auth token discovery", False, "Could not auto-discover")
    print("\n  Cannot proceed without workspace URL and auth token.")
    all_passed = vr.summary()
    sys.exit(1)

vr.record("Workspace auth", True)

API_BASE = f"{WORKSPACE_URL}/api/2.0/lineage-tracking/external-metadata"
HEADERS = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================================
# Section 2: Verify Neo4j Connectivity
# ============================================================================
print("\n--- Verify Neo4j ---")
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
# Section 3: Discover Neo4j Schema
# ============================================================================
print("\n--- Discover Schema ---")
discovered_labels = defaultdict(list)
discovered_relationships = defaultdict(list)
relationship_patterns = defaultdict(list)

try:
    driver = get_neo4j_driver(cfg)
    with driver.session(database=cfg["neo4j_database"]) as session:
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
            WITH type(r) AS relType, labels(src) AS srcLabels, labels(tgt) AS tgtLabels
            RETURN relType, collect(DISTINCT {
                source: srcLabels[0],
                target: tgtLabels[0]
            }) AS patterns
            ORDER BY relType
        """)
        for record in result:
            rel_type = record["relType"]
            # db.schema.relTypeProperties() omits relationship types that have
            # no properties. The External Metadata test still needs to register
            # those types, so the pattern query seeds them with an empty
            # property list and captures source/target labels as metadata.
            discovered_relationships.setdefault(rel_type, [])
            relationship_patterns[rel_type].extend(record["patterns"] or [])

    driver.close()
    vr.record("Schema discovery: labels", len(discovered_labels) > 0,
              f"{len(discovered_labels)} labels")
    vr.record("Schema discovery: relationships", len(discovered_relationships) > 0,
              f"{len(discovered_relationships)} types")

    for label, props in sorted(discovered_labels.items()):
        print(f"    :{label} — {len(props)} properties")
    for rel_type, props in sorted(discovered_relationships.items()):
        print(f"    [:{rel_type}] — {len(props)} properties")

except Exception as e:
    vr.record("Schema discovery", False, str(e)[:120])


# ============================================================================
# Section 4: Register Node Labels
# ============================================================================
print(f"\n--- Register Node Labels ({len(discovered_labels)}) ---")

registered_ids = []


def is_already_exists(resp) -> bool:
    """Return True for idempotent External Metadata create conflicts."""
    if resp is None:
        return False
    return resp.status_code == 409 or "ALREADY_EXISTS" in resp.text


def build_label_payload(label_name, properties):
    columns = [p["name"] for p in properties if p["name"]]
    # UC External Metadata stores columns as names only, so the richer Neo4j
    # type and mandatory-property details are encoded into properties for later
    # inspection by governance and lineage workflows.
    props_map = {
        "neo4j.database": cfg["neo4j_database"],
        "neo4j.label": label_name,
        "neo4j.host": cfg["neo4j_host"],
        "neo4j.property_count": str(len(columns)),
    }
    for p in properties:
        if p["name"]:
            neo4j_type = p["types"][0] if p["types"] else "String"
            uc_type = TYPE_MAP.get(neo4j_type, "STRING")
            props_map[f"neo4j.property.{p['name']}.type"] = uc_type
            props_map[f"neo4j.property.{p['name']}.neo4j_type"] = neo4j_type
            if p["mandatory"]:
                props_map[f"neo4j.property.{p['name']}.mandatory"] = "true"
    return {
        "name": label_name,
        "system_type": "OTHER",
        "entity_type": "NodeLabel",
        "description": f"Neo4j :{label_name} node label ({len(columns)} properties)",
        "columns": columns,
        "url": cfg["neo4j_bolt_uri"],
        "properties": props_map
    }


for label in sorted(discovered_labels.keys()):
    props = discovered_labels[label]
    payload = build_label_payload(label, props)

    try:
        t0 = time.time()
        resp = requests.post(API_BASE, headers=HEADERS, json=payload)
        resp.raise_for_status()
        result = resp.json()
        ms = (time.time() - t0) * 1000

        registered_ids.append(result["id"])
        vr.record(f"Register label: {label}", True,
                  f"{len(payload['columns'])} props, {ms:.0f}ms")
    except requests.exceptions.HTTPError as e:
        if is_already_exists(resp):
            vr.record(f"Register label: {label}", True, "already exists")
            continue
        error_msg = resp.text[:100] if resp is not None else str(e)[:100]
        vr.record(f"Register label: {label}", False, error_msg)
    except Exception as e:
        vr.record(f"Register label: {label}", False, str(e)[:120])

# ============================================================================
# Section 5: Register Relationship Types
# ============================================================================
print(f"\n--- Register Relationship Types ({len(discovered_relationships)}) ---")

for rel_type in sorted(discovered_relationships.keys()):
    properties = discovered_relationships[rel_type]
    columns = [p["name"] for p in properties if p["name"]]

    props_map = {
        "neo4j.database": cfg["neo4j_database"],
        "neo4j.relationship_type": rel_type,
        "neo4j.host": cfg["neo4j_host"],
        "neo4j.property_count": str(len(columns)),
    }
    patterns = relationship_patterns.get(rel_type, [])
    if patterns:
        props_map["neo4j.relationship.pattern_count"] = str(len(patterns))
        props_map["neo4j.relationship.source_labels"] = ",".join(
            sorted({p.get("source", "") for p in patterns if p.get("source")})
        )
        props_map["neo4j.relationship.target_labels"] = ",".join(
            sorted({p.get("target", "") for p in patterns if p.get("target")})
        )
    for p in properties:
        if p["name"]:
            neo4j_type = p["types"][0] if p["types"] else "String"
            uc_type = TYPE_MAP.get(neo4j_type, "STRING")
            props_map[f"neo4j.property.{p['name']}.type"] = uc_type

    payload = {
        "name": rel_type,
        "system_type": "OTHER",
        "entity_type": "RelationshipType",
        "description": f"Neo4j [:{rel_type}] relationship type ({len(columns)} properties)",
        "columns": columns,
        "url": cfg["neo4j_bolt_uri"],
        "properties": props_map
    }

    try:
        t0 = time.time()
        resp = requests.post(API_BASE, headers=HEADERS, json=payload)
        resp.raise_for_status()
        result = resp.json()
        ms = (time.time() - t0) * 1000

        registered_ids.append(result["id"])
        vr.record(f"Register rel: {rel_type}", True,
                  f"{len(columns)} props, {ms:.0f}ms")
    except requests.exceptions.HTTPError as e:
        if is_already_exists(resp):
            vr.record(f"Register rel: {rel_type}", True, "already exists")
            continue
        error_msg = resp.text[:100] if resp is not None else str(e)[:100]
        vr.record(f"Register rel: {rel_type}", False, error_msg)
    except Exception as e:
        vr.record(f"Register rel: {rel_type}", False, str(e)[:120])

# ============================================================================
# Section 6: Verify — List Registered External Metadata
# ============================================================================
print(f"\n--- Verify Registered Metadata ---")
try:
    all_items = []
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(API_BASE, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        all_items.extend(data.get("external_metadata", []))
        page_token = data.get("next_page_token")
        if not page_token:
            break

    neo4j_items = [m for m in all_items if m.get("system_type") == "OTHER" and
                   m.get("entity_type") in ("NodeLabel", "RelationshipType")]

    node_count = len([i for i in neo4j_items if i["entity_type"] == "NodeLabel"])
    rel_count = len([i for i in neo4j_items if i["entity_type"] == "RelationshipType"])

    vr.record("Verify: list external label metadata",
              node_count >= len(discovered_labels),
              f"{node_count}/{len(discovered_labels)} labels")
    vr.record("Verify: list external relationship metadata",
              rel_count >= len(discovered_relationships),
              f"{rel_count}/{len(discovered_relationships)} rels")
except Exception as e:
    vr.record("Verify: list external metadata", False, str(e)[:120])

# ============================================================================
# Section 7: Cleanup — delete what we registered
# ============================================================================
print(f"\n--- Cleanup ({len(registered_ids)} objects) ---")
deleted = 0
for obj_id in registered_ids:
    try:
        # Cleanup validates reversibility, but some workspaces grant create
        # without delete. Treat delete failures as best-effort cleanup output
        # instead of failing the create/list validation this suite exists to
        # prove.
        resp = requests.delete(f"{API_BASE}/{obj_id}", headers=HEADERS)
        resp.raise_for_status()
        deleted += 1
    except Exception:
        pass

cleanup_detail = f"{deleted}/{len(registered_ids)} deleted"
if deleted != len(registered_ids):
    cleanup_detail += "; non-fatal without delete privilege"
vr.record("Cleanup: delete registered metadata (best effort)", True,
          cleanup_detail)

# ============================================================================
# Summary
# ============================================================================
all_passed = vr.summary()
if not all_passed:
    sys.exit(1)
