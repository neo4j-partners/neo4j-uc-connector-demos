# Neo4j + Databricks Lakehouse Federation

**Documentation Site:** [https://neo4j-partners.github.io/neo4j-uc-connector-demos](https://neo4j-partners.github.io/neo4j-uc-connector-demos)

This project shows how to use the [Neo4j Unity Catalog Connector](https://github.com/neo4j-labs/neo4j-unity-catalog-connector) to query Neo4j graph data with SQL from Databricks. You write SQL against Neo4j as if it were a relational source. The Neo4j JDBC driver translates that SQL into Cypher, runs it against the graph, and returns the results as tables. You query Neo4j without learning Cypher.

The connection works through Unity Catalog's JDBC support, so Neo4j access is governed by the same permissions, credentials, and audit controls as the rest of your lakehouse. Once connected, you can join Neo4j graph data with your existing Delta tables in a single query. A typical example joins flights, airports, and their relationships from the graph with sensor or transactional data from Delta.

The project also demonstrates how to make Neo4j's schema visible in Databricks Catalog Explorer through metadata synchronization, and how to make Neo4j data queryable in plain English through Databricks AI/BI Genie. Schema synchronization covers node labels, relationship types, and properties.

All queries shown in this repo ran on a live Databricks cluster running Runtime 17.3 LTS connected to Neo4j Aura. The output is real, not mocked.

For a step-by-step setup and usage guide, see [neo4j_uc_jdbc_guide.md](./docs/neo4j_uc_jdbc_guide.md).

---

## Getting Started with the Neo4j Federated JDBC UC Connection

The `getting-started/` directory contains four notebooks that walk through the Neo4j Federated JDBC UC Connection end to end. The dataset is an aircraft digital twin: 20 aircraft, 160 sensors, 800 flights, and 172,800 sensor readings stored across Neo4j and a Delta table.

| Notebook | What It Covers |
|----------|---------------|
| `00-load-graph.ipynb` | Loads the aircraft digital twin dataset into Neo4j from CSV files in a UC Volume |
| `01-neo4j-uc-connection-setup.ipynb` | Creates the UC JDBC connection and runs basic SQL queries against Neo4j |
| `02-federated-queries.ipynb` | Federated queries joining Neo4j graph topology with Delta sensor time-series data |
| `03-materialized-tables.ipynb` | Materializes Neo4j node labels as managed Delta tables for unrestricted SQL access |

For prerequisites, cluster configuration, and setup steps, see [getting-started/README.md](./getting-started/README.md). For the full JDBC setup reference and query patterns, see [docs/neo4j_uc_jdbc_guide.md](./docs/neo4j_uc_jdbc_guide.md).

---

## Repository Directories

| Directory | What It Is | Value |
|-----------|------------|-------|
| `getting-started/` | Four introductory notebooks that cover loading graph data, establishing the UC JDBC connection, running federated queries, and materializing Neo4j labels as Delta tables | The fastest path from zero to a working Neo4j and Databricks integration. Self-contained and ordered for a first-time setup |
| `advanced-patterns/` | A deeper set of Databricks notebooks covering metadata sync via Delta tables and the External Metadata API, advanced federated query patterns, and performance diagnostics | Reference for integration patterns beyond the intro notebooks |
| `validation/` | Python scripts that upload to a Databricks workspace and run as one-time jobs against a live Neo4j connection. Covers data load, connection validation, federated queries, metadata sync, and advanced Spark patterns | Automated end-to-end smoke testing of the full stack, including the JDBC driver, UC connection, Spark connector, and `remote_query`, without manual notebook execution |
| `driver-tests/` | Local Java tests for Neo4j JDBC SQL-to-Cypher translation, runnable without Databricks | Fast local verification of SQL translation behavior before deploying to a cluster. No Spark, UC, or cloud credentials required |
| `docs/` | Markdown reference documentation: JDBC setup guide, metadata sync design, and the Spark subquery cleaner explanation | Detailed technical reference for the patterns shown in the notebooks, including type mappings, query examples, and troubleshooting |
| `site/` | Antora documentation site in AsciiDoc, published to GitHub Pages | The public documentation site at [neo4j-partners.github.io/neo4j-uc-connector-demos](https://neo4j-partners.github.io/neo4j-uc-connector-demos) |

---

## Overview of Neo4j Integration Patterns

**JDBC Connectivity.** Neo4j connects to Unity Catalog via a generic JDBC connection of `TYPE JDBC`, using the [Neo4j JDBC driver](https://neo4j.com/docs/jdbc-manual/current/) with built-in SQL-to-Cypher translation. A SafeSpark compatibility issue caused by metaspace memory exhaustion was resolved in collaboration with Databricks engineering. With three Spark configuration settings, the Neo4j Federated JDBC UC Connection works correctly across queries, aggregates, GROUP BY, HAVING, ORDER BY, JOINs, and schema discovery.

**Federated Queries.** Once connected, Neo4j graph data such as flights, airports, maintenance events, and component hierarchies can be joined with Delta lakehouse tables such as sensor readings and time-series analytics in a single Spark SQL statement. No ETL pipelines are required. Each database is queried where the data lives, and results are combined at read time.

**Metadata Synchronization.** The JDBC connection registers credentials and a driver, but does not expose Neo4j's schema as browsable UC objects. This project prototypes two approaches to metadata sync. The first is materialized Delta tables, which give a full data copy with Catalog Explorer, `INFORMATION_SCHEMA`, and SQL access. The second is the External Metadata API, which registers metadata only for discoverability and lineage. The graph-to-relational mapping is well-defined: node labels become tables in a `nodes` schema, relationship types become tables in a `relationships` schema, and properties become columns with mapped types. For design details, type mappings, and implementation, see [docs/metadata_synchronization.md](./docs/metadata_synchronization.md).

**Natural Language via Genie.** Neo4j data materialized as managed Delta tables becomes transparently queryable through Databricks AI/BI Genie. Users ask plain-English questions and Genie generates SQL that federates across Neo4j graph data and Delta lakehouse tables, all governed by Unity Catalog. The LLM never sees Cypher and the user never writes SQL.

---

## What First-Class Lakehouse Federation Support Would Unlock

The integration works today through Unity Catalog's custom JDBC connection. Elevating Neo4j to an officially supported Lakehouse Federation source as `TYPE NEO4J` would replace the current manual workarounds with native platform capabilities:

- **Foreign catalog.** `CREATE FOREIGN CATALOG neo4j_graph USING CONNECTION neo4j_conn` registers the full Neo4j schema as a browsable three-level namespace in Unity Catalog, making graph data discoverable alongside Delta tables without materialization jobs or External Metadata API workarounds.
- **Table-level governance.** UC grants such as `SELECT`, `USE SCHEMA`, and `BROWSE` apply to individual Neo4j-backed tables rather than connection-level-only access control. Data tagging and classification work per table.
- **Column-level lineage and audit.** Every query is tracked in `system.access.audit` with full context, and column-level lineage covers notebooks, jobs, and dashboards.
- **Improved query pushdown.** Broader filter, projection, aggregate, and sort pushdown managed by Databricks, with potential join pushdown mapping cross-table joins to native graph traversals.
- **Genie and AI/BI Dashboards.** Neo4j foreign tables queryable via natural language and drag-and-drop dashboards without materialization as a prerequisite.
- **Service principal and OAuth support.** Native credential management rather than user and password stored in connection options.

For the full capability breakdown and trade-offs, see [docs/unlock.md](docs/neo4j-offiicial-data-source-unlock.md).

---

## Validated Components

| Component | Status | Notes |
|-----------|--------|-------|
| Network Connectivity | **PASS** | TCP to Neo4j port 7687 |
| Neo4j Python Driver | **PASS** | Bolt protocol works |
| Neo4j Spark Connector | **PASS** | `org.neo4j.spark.DataSource` works |
| Neo4j JDBC SQL-to-Cypher | **PASS** | Aggregates, GROUP BY, HAVING, ORDER BY, JOINs, dbtable all work |
| Direct JDBC (Non-UC) | **PASS** | Works with `customSchema` workaround |
| **Unity Catalog JDBC** | **PASS** | Works with SafeSpark memory configuration |
| **UC Schema Discovery** | **PASS** | Works with SafeSpark memory configuration |

**All supported patterns pass. Use the Neo4j Spark Connector for relationship property aggregation.**

---

## How It Works

### JDBC Driver JAR

A single shaded JAR is uploaded to a Unity Catalog Volume:

| JAR | Purpose |
|-----|---------|
| `neo4j-unity-catalog-connector-<version>.jar` | Neo4j JDBC Lakehouse Federation Connector. Bundles the Neo4j JDBC driver, SQL-to-Cypher translator, and Spark subquery cleaner |

Download the latest release from [neo4j-unity-catalog-connector releases](https://github.com/neo4j-labs/neo4j-unity-catalog-connector/tags). See the [neo4j-unity-catalog-connector](https://github.com/neo4j-labs/neo4j-unity-catalog-connector) repo for details on what the JAR contains and how it is built.

The JAR includes a Spark subquery cleaner that handles a Spark-specific behavior. When Spark connects via JDBC, it wraps queries in a subquery for schema probing, such as `SELECT * FROM (<query>) SPARK_GEN_SUBQ_0 WHERE 1=0`. The cleaner detects this marker, extracts the inner query, and routes it correctly. See [neo4j_jdbc_cleaner.md](./docs/neo4j_jdbc_cleaner.md) for details.

### SafeSpark Configuration

Databricks runs custom JDBC drivers in an isolated SafeSpark sandbox. The Neo4j JDBC driver requires more metaspace than the default allocation. Without these settings, the sandbox JVM crashes with "Connection was closed before the operation completed" errors:

```
spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib 128
spark.databricks.safespark.jdbcSandbox.jvm.xmx.mib 300
spark.databricks.safespark.jdbcSandbox.size.default.mib 512
```

### UC Connection

```sql
CREATE CONNECTION neo4j_connection TYPE JDBC
ENVIRONMENT (
  java_dependencies '["path/to/neo4j-unity-catalog-connector-<version>.jar"]'  -- must be a UC Volume path
)
OPTIONS (
  url 'jdbc:neo4j+s://your-host:7687/neo4j?enableSQLTranslation=true',
  user secret('scope', 'neo4j-user'),
  password secret('scope', 'neo4j-password'),
  driver 'org.neo4j.jdbc.Neo4jDriver',
  externalOptionsAllowList 'dbtable,query,customSchema'
)
```

### Query Neo4j

```python
df = spark.read.format("jdbc") \
    .option("databricks.connection", "neo4j_connection") \
    .option("query", "SELECT COUNT(*) AS cnt FROM Flight") \
    .option("customSchema", "cnt LONG") \
    .load()
df.show()
```

### Prerequisites

Enable these preview features in your Databricks workspace:

| Feature | Required For |
|---------|--------------|
| Custom JDBC on UC Compute | Loading custom JDBC drivers in UC connections |
| remote_query table-valued function | Using `remote_query()` SQL function |

---

## SQL-to-Cypher Translation

The Neo4j JDBC driver automatically translates SQL to Cypher when `enableSQLTranslation=true`. For the full translation reference, supported patterns, and examples, see [docs/neo4j_uc_jdbc_guide.md](./docs/neo4j_uc_jdbc_guide.md).

---

## References

- [Neo4j JDBC Driver](https://neo4j.com/docs/jdbc-manual/current/)
- [Neo4j SQL2Cypher Translation](https://neo4j.com/docs/jdbc-manual/current/sql2cypher/)
- [Databricks Unity Catalog JDBC](https://docs.databricks.com/aws/en/connect/jdbc-connection)
- [Databricks Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/)
- [Neo4j Spark Connector](https://neo4j.com/docs/spark/current/)
- [getting-started/README.md](./getting-started/README.md). Notebook prerequisites and setup.
- [docs/neo4j_uc_jdbc_guide.md](./docs/neo4j_uc_jdbc_guide.md). JDBC setup, query patterns, troubleshooting.
