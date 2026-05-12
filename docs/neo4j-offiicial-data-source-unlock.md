# What Official Federated Data Source Status Unlocks for the Neo4j UC Connector

Today the Neo4j connector works via Unity Catalog's custom JDBC connection (beta). Becoming an officially supported federated data source would unlock the following capabilities.

## Catalog-Level Integration

- **Foreign catalog creation** — `CREATE FOREIGN CATALOG neo4j_graph USING CONNECTION neo4j_conn` would register the Neo4j database as a browsable three-level namespace (`catalog.schema.table`) in Unity Catalog, making graph data discoverable alongside Delta tables
- **Automatic metadata sync** — Node labels and relationship types would appear as tables in Catalog Explorer without manual materialization or External Metadata API workarounds; Unity Catalog auto-refreshes metadata on query, with manual `REFRESH FOREIGN CATALOG` available
- **Workspace binding** — Foreign catalogs can be bound to specific workspaces, enabling multi-workspace isolation within a single metastore

## Governance and Access Control

- **Table-level permissions** — UC grants (`SELECT`, `USE CATALOG`, `USE SCHEMA`, `BROWSE`) on individual Neo4j-backed tables rather than the current connection-level-only governance where anyone with `USE CONNECTION` can reach all data
- **Data tagging and classification** — Apply governed tags and key-value metadata to individual graph-backed tables for compliance and data discovery
- **Column-level data lineage** — Unity Catalog would automatically track which Neo4j tables and columns are read by notebooks, jobs, and dashboards
- **Per-table audit logging** — Every query against a Neo4j foreign table would appear in `system.access.audit` with full context (who, what table, when, from which notebook/job)

> **Trade-off:** Official Lakehouse Federation status makes foreign tables **read-only**. The current JDBC connection supports writes. If write access to Neo4j from Databricks is needed, the JDBC connection approach must be retained.

## Authentication

- **Connector-level OAuth flows** — Some official connectors (e.g., Snowflake) support OAuth; a Neo4j connector could expose the same capability, storing credentials securely in the connection object and hiding them from querying users. This would need to be built into the Neo4j connector specifically — it is not automatically inherited from official status.
- **Service credential integration** — Native support for Databricks service principals

## Query Performance

- **Improved query pushdown** — Native connectors benefit from Databricks-managed dialect-specific translation with broader coverage: filter, projection, aggregate, and sort pushdown. The current connector already does SQL→Cypher translation via `SqlToCypherTranslator`, but official status would expand what operations are pushed down and tested by Databricks.
- **Join pushdown** — Cross-table joins within the same Neo4j source could be pushed down as graph traversals rather than pulling data into Spark. Databricks documents join pushdown as a supported optimization for native federation connectors with specific requirements; whether Neo4j graph traversals map cleanly to this requires validation.
- **Parallel read optimization** — Connector-specific partitioning strategies tailored to graph data distribution

## AI/BI and Analytics

- **AI/BI Dashboard support** — Neo4j foreign tables queryable via standard Databricks SQL, enabling drag-and-drop dashboard creation against live graph data
- **Genie natural language queries** — Users could ask questions about graph data in plain English; Genie is confirmed to work with foreign tables (tested with PostgreSQL Lakehouse Federation)
- **Materialized view integration** — `CREATE MATERIALIZED VIEW` directly referencing Neo4j foreign catalog tables is supported by Databricks (docs show `CREATE MATERIALIZED VIEW xyz AS SELECT * FROM federated_catalog...`), providing caching and incremental refresh

> **Caveat:** Databricks documentation notes that foreign tables are "not intended for production workloads" at high query frequency — materialized Delta copies are recommended for those cases.

## What This Replaces

Today, achieving equivalent functionality requires:

- A custom shaded JDBC JAR with SPI-based translator pipeline
- SafeSpark JVM sandbox tuning (`maxMetaspace`, `maxHeap`, `sandboxSize`)
- Manual metadata sync via materialization jobs or the External Metadata API
- Connection-level-only access control (no per-table permissions)
- Materialized Delta tables as a prerequisite for Genie and full SQL support
- Databricks preview features enabled (Custom JDBC on UC Compute, `remote_query` TVF)
- Manual credential management (user/password in connection options, visible to connection admins)

Official status would collapse this complexity into a first-class, governed, zero-configuration integration — at the cost of write access and with the caveat that foreign tables carry a "not for production" advisory for high-frequency query patterns.
