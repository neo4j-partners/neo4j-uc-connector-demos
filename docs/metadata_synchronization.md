# Unity Catalog Metadata Synchronization for Neo4j

## Why Add Neo4j Metadata to Unity Catalog?

Adding Neo4j metadata to Unity Catalog gives UC visibility into graph assets
that are not stored as Delta tables. It makes Neo4j labels, relationship types,
property names, and source references discoverable in the same governed catalog
plane as lakehouse assets.

This provides:

- **Discoverability:** Unity Catalog can show that Neo4j labels such as
  `Aircraft`, `Flight`, and `Sensor`, and relationship types such as
  `HAS_SENSOR` and `OPERATES_FLIGHT`, exist.
- **Governance surface:** External graph entities can carry ownership,
  descriptions, schema shape, property names, and source URLs.
- **Lineage readiness:** Downstream Databricks assets can reference or relate to
  external graph entities in Unity Catalog's governance model.
- **No data copy:** External Metadata registration records schema-level facts
  only. It does not materialize, duplicate, or move Neo4j data.
- **Common catalog plane:** Teams using Unity Catalog as their governed
  inventory can see lakehouse tables and external graph entities in one place.

## What This Proves

The metadata synchronization validation proves that:

- Databricks jobs can discover live Neo4j schema from the graph.
- Neo4j node labels and relationship types can be represented in Unity Catalog
  External Metadata.
- `CREATE_EXTERNAL_METADATA` on the metastore is sufficient for registration.
- The workflow can be automated from `.env`: secrets, grant, upload, and
  validation jobs.
- Unity Catalog can list and verify registered external graph metadata after
  creation.

It does **not** prove query federation by itself. Query federation is validated
separately by the JDBC and `remote_query()` validation scripts. Metadata
synchronization proves the governance and catalog registration side of the Neo4j
and Unity Catalog integration.

## What Is Metadata Synchronization?

When Databricks refers to "Unity Catalog metadata synchronization," they mean making the schema structure of an external data source (Neo4j) visible as first-class objects within Unity Catalog's three-level namespace (`catalog.schema.table`). This enables:

- **Discoverability:** Users browse Neo4j node labels and relationship types in Catalog Explorer alongside Delta tables
- **Governance:** Fine-grained access control (USE_CATALOG, USE_SCHEMA, SELECT) on Neo4j objects
- **Lineage tracking:** Data lineage from Neo4j through downstream Spark jobs and dashboards
- **Schema documentation:** Column types, descriptions, and tags managed centrally in UC

This is distinct from what the existing JDBC connection provides. The JDBC connection (`CREATE CONNECTION ... TYPE JDBC`) registers credentials and a driver; it does not expose Neo4j's schema as browsable UC objects.

---

## How This Differs from the Existing JDBC Integration

| Aspect | JDBC Connection (Current) | Metadata Sync (Needed) |
|--------|--------------------------|----------------------|
| What's registered in UC | A `CONNECTION` object only | Catalog + schemas + tables + columns as UC objects |
| Schema browsing in Catalog Explorer | No | Yes |
| Access control granularity | Connection-level only | Table-level and column-level |
| Lineage tracking | No | Yes |
| Query model | `spark.read.format('jdbc').option('databricks.connection', ...)` | Could also support `SELECT * FROM neo4j_catalog.graph.persons` |

---

## Why Neo4j Requires a Custom Solution

Databricks provides automatic metadata sync for [Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/) sources, but only for these databases: MySQL, PostgreSQL, Oracle, Teradata, Redshift, SQL Server, Azure Synapse, BigQuery, Snowflake, Salesforce Data Cloud, and Databricks-to-Databricks.

For federated sources, Unity Catalog syncs the remote schema on every interaction:

> "Foreign catalog metadata is synced into Unity Catalog on each interaction with the catalog."

Neo4j is **not** on the supported list. Additionally, the JDBC connection type explicitly does not support foreign catalogs:

> "Foreign catalogs are not supported with JDBC connections."

This means we must build a custom metadata sync pipeline.

---

## Neo4j Schema Introspection

Neo4j provides several methods to extract schema metadata. Here are the recommended approaches, ordered by richness:

### Primary: `apoc.meta.schema()` (APOC Core, Richest Single Call)

```cypher
CALL apoc.meta.schema({sample: -1})
YIELD value
```

Returns a nested map containing:
- Node labels with counts
- Property names and types (uses Cypher 5 canonical names: `STRING`, `INTEGER`, `FLOAT`)
- Indexed/unique/existence flags per property
- Relationship types with direction, connectivity, and counts
- Relationship properties

Example output structure:
```json
{
  "Aircraft": {
    "type": "node",
    "count": 60,
    "properties": {
      "aircraftId": {"type": "STRING", "indexed": true, "unique": true, "existence": false},
      "manufacturer": {"type": "STRING", "indexed": false, "unique": false, "existence": false},
      "model": {"type": "STRING", "indexed": false, "unique": false, "existence": false},
      "icao24": {"type": "STRING", "indexed": false, "unique": false, "existence": false}
    },
    "relationships": {
      "DEPARTS_FROM": {
        "direction": "out",
        "count": 2400,
        "labels": ["Airport"],
        "properties": {}
      }
    }
  }
}
```

**Requires:** APOC Core plugin installed on the Neo4j instance.

### Fallback: Built-in Procedures (No APOC Required)

```cypher
-- Node label properties
CALL db.schema.nodeTypeProperties()
-- Returns: nodeType, nodeLabels, propertyName, propertyTypes, mandatory

-- Relationship type properties
CALL db.schema.relTypeProperties()
-- Returns: relType, propertyName, propertyTypes, mandatory
```

These use older internal type names (`String`, `Long`, `Double`) instead of Cypher 5 canonical names.

> **Note:** Some documentation references `sourceNodeLabels` and `targetNodeLabels` as
> output columns for `db.schema.relTypeProperties()`, but these are **not available** on
> Neo4j 5.x Aura (verified on 5.27-aura). To discover relationship source/target patterns,
> use `apoc.meta.schema()` or `db.schema.visualization()` instead.

### Constraints and Indexes

```cypher
-- Constraints (uniqueness, existence, type, key)
SHOW CONSTRAINTS YIELD *

-- Indexes (range, fulltext, text, point, vector)
SHOW INDEXES YIELD *
```

### Recommended Combination

1. `CALL apoc.meta.schema({sample: -1})`: property names, types, indexed/unique flags, counts
2. `SHOW CONSTRAINTS YIELD *`: precise constraint types and mandatory enforcement
3. `SHOW INDEXES YIELD *`: standalone (non-constraint) indexes

---

## Neo4j to Unity Catalog Type Mapping

| Neo4j Type (Internal) | Neo4j Cypher 5 Type | UC/Spark SQL Type | Notes |
|----------------------|--------------------|--------------------|-------|
| `String` | `STRING` | `STRING` | No length limit in Neo4j |
| `Long` | `INTEGER` | `BIGINT` | 64-bit signed |
| `Double` | `FLOAT` | `DOUBLE` | 64-bit IEEE 754 |
| `Boolean` | `BOOLEAN` | `BOOLEAN` | |
| `Date` | `DATE` | `DATE` | |
| `LocalDateTime` | `LOCAL DATETIME` | `TIMESTAMP_NTZ` | |
| `DateTime` | `ZONED DATETIME` | `TIMESTAMP` | |
| `LocalTime` | `LOCAL TIME` | `STRING` | Serialize as ISO string |
| `Time` | `ZONED TIME` | `STRING` | No direct SQL equivalent |
| `Duration` | `DURATION` | `STRING` | Serialize as ISO 8601 duration |
| `Point` | `POINT` | `STRING` | Serialize as WKT or JSON |
| `StringArray` | `LIST<STRING>` | `ARRAY<STRING>` | |
| `LongArray` | `LIST<INTEGER>` | `ARRAY<BIGINT>` | |
| `DoubleArray` | `LIST<FLOAT>` | `ARRAY<DOUBLE>` | |
| `BooleanArray` | `LIST<BOOLEAN>` | `ARRAY<BOOLEAN>` | |
| `ByteArray` | (byte array) | `BINARY` | |

---

## Graph-to-Relational Mapping Strategy

Neo4j's graph model (labels, relationships, properties) must be mapped to UC's relational model (catalogs, schemas, tables, columns).

### Proposed Mapping

```
Unity Catalog                  Neo4j
─────────────                  ─────
Catalog: neo4j_catalog    →    Neo4j database
  Schema: nodes           →    Node labels namespace
    Table: aircraft        →    :Aircraft label
      Column: aircraftId   →    aircraftId property (STRING)
      Column: manufacturer →    manufacturer property (STRING)
    Table: flight          →    :Flight label
    Table: airport         →    :Airport label
  Schema: relationships   →    Relationship types namespace
    Table: departs_from    →    :DEPARTS_FROM type
      Column: source_id    →    Start node identifier
      Column: target_id    →    End node identifier
      Column: ...          →    Relationship properties
```

### Design Decisions

1. **One table per node label:** Each Neo4j label maps to a UC table in the `nodes` schema
2. **One table per relationship type:** Each relationship type maps to a UC table in the `relationships` schema, with `source_id` and `target_id` columns plus any relationship properties
3. **Properties become columns:** Neo4j properties map to UC columns with types from the mapping table above
4. **Multi-label nodes:** Nodes with multiple labels (e.g., `:Person:Actor`) appear in tables for each label
5. **Naming convention:** UC identifiers are lowercase; Neo4j labels/types are converted accordingly (`DEPARTS_FROM` → `departs_from`)

---

## Prerequisites: Granting CREATE_EXTERNAL_METADATA

Approach 2 (External Metadata API) requires the `CREATE_EXTERNAL_METADATA` privilege on the metastore. Without it, every registration call to `/api/2.0/lineage-tracking/external-metadata` fails with:

```
PERMISSION_DENIED: User does not have CREATE EXTERNAL METADATA on Metastore
```

### Why the grant requires a cluster job

`CREATE_EXTERNAL_METADATA` is a metastore-level grant, but it must be issued via SQL on a workspace host. The account-level host (`accounts.azuredatabricks.net`) does not serve the Unity Catalog permissions endpoint needed for this grant. The reliable path is a one-shot PySpark job on an all-purpose cluster, which runs with the submitting user's workspace identity:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("GRANT CREATE_EXTERNAL_METADATA ON METASTORE TO `user@example.com`")
```

This works when the submitting user is a metastore admin or holds sufficient inherited privileges.

### Using the metadata DAB job

The `metadata` Databricks Asset Bundle job automates this. Its first task (`grant_external_metadata`) runs `validation/scripts/grant_external_metadata.py` on the configured cluster, then the materialization and External Metadata API tasks run in parallel:

```bash
cd validation
uv run python validate.py metadata
```

By default the grant target is the current workspace user. To grant to a specific principal, set `METADATA_GRANT_PRINCIPAL` in the repo-root `.env`:

```bash
METADATA_GRANT_PRINCIPAL=other.user@example.com
```

The job reads `DATABRICKS_CLUSTER_ID` from `.env` (via the bundle `cluster_id` variable). It must point to an all-purpose cluster, since the privilege is issued through SQL on a workspace host.

For repeat runs where the bundle is already deployed, skip the deploy step:

```bash
uv run python validate.py metadata --skip-deploy
```

---

## Implementation Approaches

There are three approaches to register Neo4j metadata in Unity Catalog, each with different tradeoffs.

### Approach 1: UC Tables API (Register as External Tables)

Register Neo4j labels/relationships as `EXTERNAL` or `FOREIGN` table entries in UC using the Tables REST API.

**API Endpoint:** `POST /api/2.1/unity-catalog/tables`

```json
{
  "name": "aircraft",
  "catalog_name": "neo4j_catalog",
  "schema_name": "nodes",
  "table_type": "EXTERNAL",
  "data_source_format": "JSON",
  "columns": [
    {"name": "aircraftId", "type_name": "STRING", "nullable": false, "position": 0, "comment": "Unique aircraft identifier"},
    {"name": "manufacturer", "type_name": "STRING", "nullable": true, "position": 1},
    {"name": "model", "type_name": "STRING", "nullable": true, "position": 2},
    {"name": "icao24", "type_name": "STRING", "nullable": true, "position": 3}
  ],
  "properties": {
    "neo4j.label": "Aircraft",
    "neo4j.database": "neo4j",
    "neo4j.node_count": "60",
    "neo4j.connection": "neo4j_connection"
  },
  "comment": "Neo4j :Aircraft node label (60 nodes)"
}
```

**Pros:**
- Tables appear in Catalog Explorer and `INFORMATION_SCHEMA.TABLES` / `INFORMATION_SCHEMA.COLUMNS`
- Supports fine-grained access control (GRANT SELECT ON TABLE)
- Column types, nullability, and comments are fully preserved
- Can be queried via SQL `INFORMATION_SCHEMA`

**Cons:**
- `EXTERNAL` tables require a `storage_location`, which would need a workaround or placeholder
- May not support actual query routing back to Neo4j without additional plumbing
- Table metadata becomes stale if Neo4j schema changes (requires re-sync)

**Prerequisite steps:**
```bash
# 1. Create catalog
POST /api/2.1/unity-catalog/catalogs
{"name": "neo4j_catalog", "comment": "Neo4j graph database metadata"}

# 2. Create schemas
POST /api/2.1/unity-catalog/schemas
{"name": "nodes", "catalog_name": "neo4j_catalog", "comment": "Neo4j node labels"}

POST /api/2.1/unity-catalog/schemas
{"name": "relationships", "catalog_name": "neo4j_catalog", "comment": "Neo4j relationship types"}

# 3. Register tables (one per label/relationship type)
POST /api/2.1/unity-catalog/tables
{...}  # As shown above
```

### Approach 2: External Metadata API (Public Preview)

Register Neo4j objects as external metadata entries, designed for systems outside the standard relational model.

**API Endpoint:** `POST /api/2.0/lineage-tracking/external-metadata`

```json
{
  "name": "Aircraft",
  "system_type": "OTHER",
  "entity_type": "NodeLabel",
  "description": "Aircraft node label in Neo4j (60 nodes, 4 properties)",
  "columns": ["aircraftId", "manufacturer", "model", "icao24"],
  "url": "neo4j+s://your-host:7687",
  "properties": {
    "neo4j.database": "neo4j",
    "neo4j.label": "Aircraft",
    "neo4j.node_count": "60",
    "neo4j.property.aircraftId.type": "STRING",
    "neo4j.property.aircraftId.indexed": "true",
    "neo4j.property.aircraftId.unique": "true",
    "neo4j.property.manufacturer.type": "STRING",
    "neo4j.property.model.type": "STRING",
    "neo4j.property.icao24.type": "STRING"
  }
}
```

**Pros:**
- Designed specifically for external systems that don't fit the relational model
- Supports arbitrary `entity_type` values (e.g., `NodeLabel`, `RelationshipType`)
- Free-form `properties` map for rich metadata
- Supports lineage tracking via the External Lineage API

**Cons:**
- `columns` is `Array<string>`, with no type info per column (types must go in `properties`)
- Does **not** appear in `INFORMATION_SCHEMA.TABLES`; only queryable via REST API
- No `NEO4J` enum for `system_type`; must use `OTHER`
- Public Preview; API may change
- No direct query routing to Neo4j

### Approach 3: Materialized Delta Tables (Data Copy)

Read data through the UC JDBC connection and write it as managed Delta tables in UC. Metadata is registered automatically when `saveAsTable()` runs.

```python
# Read from Neo4j through the UC JDBC connection
df = (spark.read.format("jdbc")
      .option("databricks.connection", "sample_neo4j_jdbc_connection")
      .option("customSchema", "aircraftId STRING, model STRING, manufacturer STRING")
      .option("query", "SELECT aircraftId, model, manufacturer FROM Aircraft")
      .load())

# Write to UC as a managed Delta table; schema is registered automatically
df.write.format("delta").mode("overwrite").saveAsTable("neo4j_catalog.nodes.aircraft")
```

**Pros:**
- Full UC integration: tables, columns, types, statistics, lineage all work automatically
- Data is queryable via standard SQL (`SELECT * FROM neo4j_catalog.nodes.aircraft`)
- Delta features (time travel, ACID transactions, optimization) apply
- No custom sync code for metadata; schema is inferred from the DataFrame
- Uses the same UC JDBC connection as federation, so the metadata sync follows the same connector path as the query notebooks

**Cons:**
- Copies data; not a live view of Neo4j
- Requires scheduled refresh jobs to keep data current
- Storage cost for duplicated data
- `customSchema` is needed per query to avoid `NullType` inference on JDBC reads
- Loses graph structure; traversals require SQL JOINs

---

## Recommended Architecture

A hybrid approach combining metadata registration with query routing:

```
┌──────────────────────────────────────────────────────┐
│                    Unity Catalog                      │
│                                                       │
│  neo4j_catalog                                        │
│  ├── nodes (schema)                                   │
│  │   ├── aircraft  ← metadata registered via API      │
│  │   ├── flight    ← metadata registered via API      │
│  │   └── airport   ← metadata registered via API      │
│  └── relationships (schema)                           │
│      ├── departs_from  ← metadata registered via API  │
│      └── arrives_at    ← metadata registered via API  │
│                                                       │
│  neo4j_connection  ← JDBC connection (existing)       │
└──────────────┬───────────────────────────────────────┘
               │
    ┌──────────┴──────────┐
    │   Sync Pipeline      │
    │   (Databricks Job)   │
    │                      │
    │  1. Introspect Neo4j │
    │  2. Map to UC schema │
    │  3. Register via API │
    │  4. Optionally       │
    │     materialize data │
    └──────────┬──────────┘
               │
    ┌──────────┴──────────┐
    │      Neo4j           │
    │                      │
    │  :Aircraft           │
    │  :Flight             │
    │  :Airport            │
    │  [:DEPARTS_FROM]     │
    │  [:ARRIVES_AT]       │
    └─────────────────────┘
```

### Sync Pipeline Steps

1. **Introspect Neo4j schema**: Call `db.schema.nodeTypeProperties()` and `db.schema.relTypeProperties()` through the Neo4j UC connector/JDBC path
2. **Map to UC model**: Convert labels → tables, properties → columns, Neo4j types → UC types
3. **Register metadata in UC**: Use Tables REST API or External Metadata API
4. **Optionally materialize data**: Write high-value labels as Delta tables for SQL querying
5. **Schedule periodic sync**: Run as a Databricks job to detect schema changes

### Sync Pipeline Implementation Sketch

```python
import requests
import json

# --- Step 1: Introspect Neo4j through UC JDBC ---

def run_remote_query(spark, connection_name, query):
    """Run a Neo4j-side metadata query through Databricks remote_query()."""
    escaped = query.replace("'", "''")
    return spark.sql(f"""
        SELECT * FROM remote_query('{connection_name}',
            query => '{escaped}')
    """)

def get_neo4j_schema(spark, connection_name):
    """Extract label and relationship property metadata through UC JDBC."""
    node_props = run_remote_query(
        spark,
        connection_name,
        """/*+ NEO4J FORCE_CYPHER */
        CALL db.schema.nodeTypeProperties()
        YIELD nodeLabels, propertyName, propertyTypes, mandatory
        RETURN nodeLabels, propertyName, propertyTypes, mandatory"""
    ).collect()
    rel_props = run_remote_query(
        spark,
        connection_name,
        """/*+ NEO4J FORCE_CYPHER */
        CALL db.schema.relTypeProperties()
        YIELD relType, propertyName, propertyTypes, mandatory
        RETURN relType, propertyName, propertyTypes, mandatory"""
    ).collect()
    return {"nodes": node_props, "relationships": rel_props}

# --- Step 2: Map to UC model ---

NEO4J_TO_UC_TYPE = {
    "STRING": "STRING",
    "INTEGER": "BIGINT",
    "FLOAT": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "LOCAL DATETIME": "TIMESTAMP_NTZ",
    "ZONED DATETIME": "TIMESTAMP",
    "LOCAL TIME": "STRING",
    "ZONED TIME": "STRING",
    "DURATION": "STRING",
    "POINT": "STRING",
    "LIST": "ARRAY<STRING>",
}

def map_neo4j_type(neo4j_type):
    """Map Neo4j property type to UC column type."""
    return NEO4J_TO_UC_TYPE.get(neo4j_type, "STRING")

def schema_to_uc_tables(neo4j_schema, constraints):
    """Convert Neo4j schema to UC table definitions."""
    tables = []
    for name, meta in neo4j_schema.items():
        if meta.get("type") != "node":
            continue
        columns = []
        for i, (prop_name, prop_meta) in enumerate(meta.get("properties", {}).items()):
            columns.append({
                "name": prop_name,
                "type_name": map_neo4j_type(prop_meta.get("type", "STRING")),
                "nullable": not prop_meta.get("existence", False),
                "position": i,
            })
        tables.append({
            "name": name.lower(),
            "table_type": "EXTERNAL",
            "columns": columns,
            "properties": {
                "neo4j.label": name,
                "neo4j.node_count": str(meta.get("count", 0)),
            },
            "comment": f"Neo4j :{name} node label ({meta.get('count', 0)} nodes)",
        })
    return tables

# --- Step 3: Register in UC ---

def register_table(workspace_url, token, catalog, schema, table_def):
    """Register a table in Unity Catalog via REST API."""
    url = f"{workspace_url}/api/2.1/unity-catalog/tables"
    payload = {
        "catalog_name": catalog,
        "schema_name": schema,
        **table_def,
    }
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    return resp.json()
```

---

## Verifying Synced Metadata

After registration, verify metadata visibility:

```sql
-- Check registered tables
SELECT table_catalog, table_schema, table_name, table_type, comment
FROM system.information_schema.tables
WHERE table_catalog = 'neo4j_catalog'
ORDER BY table_schema, table_name;

-- Check column definitions
SELECT table_name, column_name, data_type, is_nullable, comment
FROM neo4j_catalog.information_schema.columns
WHERE table_schema = 'nodes'
ORDER BY table_name, ordinal_position;

-- Check connections
SELECT * FROM system.information_schema.connections
WHERE connection_name = 'neo4j_connection';
```

For External Metadata API objects (if using Approach 2):
```bash
# List external metadata objects
GET /api/2.0/lineage-tracking/external-metadata

# These do NOT appear in INFORMATION_SCHEMA; REST API only
```

---

## Open Questions

1. **Which registration API to use?** The Tables API provides richer integration (INFORMATION_SCHEMA, access control) but requires a `storage_location` for EXTERNAL tables. The External Metadata API is purpose-built for external systems but has limited column type support and no INFORMATION_SCHEMA visibility. Need to clarify with Databricks which they recommend for this use case.

2. **Query routing:** Registering metadata makes Neo4j objects visible in UC, but does clicking a table in Catalog Explorer route queries back to Neo4j via the JDBC connection? Or is this metadata-only with queries still requiring explicit `spark.read.format('jdbc')` calls? If the former, how is the mapping configured?

3. **Sync frequency:** How often should the schema be re-synced? Neo4j schemas evolve dynamically (new labels/properties appear when data is written). Options: on-demand, scheduled (hourly/daily), or event-driven (Neo4j triggers).

4. **Multi-label nodes:** A node with labels `:Person:Actor` has properties that span both labels. Should these be represented as separate tables with shared columns, or as a combined table?

5. **Relationship connectivity:** Should the `relationships` schema tables include `source_label` and `target_label` columns, or should this be captured only in table metadata/comments?

6. **Delta materialization scope:** Which labels/relationships should be materialized as Delta tables vs. metadata-only entries? Materializing everything may be impractical for large graphs.

---

## Related Notebooks

Two notebooks in `advanced-patterns/` implement the approaches described above:

| Notebook | Implements | What It Does |
|----------|-----------|--------------|
| [`getting-started/03-materialized-tables.ipynb`](../getting-started/03-materialized-tables.ipynb) | Approach 3 (Materialized Delta Tables) | Reads each Neo4j node label through the UC JDBC connection and writes it as a managed Delta table, then queries `INFORMATION_SCHEMA` to confirm UC has registered the schema automatically. Uses the same JDBC path as federation. |
| [`05_metadata_sync_external_api.ipynb`](../advanced-patterns/05_metadata_sync_external_api.ipynb) | Approach 2 (External Metadata API) | Discovers the Neo4j schema, then registers each node label and relationship type via the [External Metadata REST API](https://docs.databricks.com/api/workspace/externalmetadata). No data is copied; this is metadata-only registration for discoverability and lineage. Encodes Neo4j property types in the metadata properties map. Includes optional cleanup to delete registered objects. |

---

## References

- [Databricks Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/)
- [Databricks JDBC Connection](https://docs.databricks.com/aws/en/connect/jdbc-connection)
- [Unity Catalog REST API: Tables](https://docs.databricks.com/api/workspace/tables)
- [Unity Catalog REST API: External Metadata](https://docs.databricks.com/api/workspace/externalmetadata)
- [Unity Catalog REST API: External Lineage](https://docs.databricks.com/api/workspace/externallineage)
- [Unity Catalog INFORMATION_SCHEMA](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-information-schema.html)
- [Neo4j APOC meta.schema()](https://neo4j.com/docs/apoc/current/overview/apoc.meta/apoc.meta.schema/)
- [Neo4j JDBC Driver](https://neo4j.com/docs/jdbc-manual/current/)
