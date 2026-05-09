# In-Depth Examples — Neo4j JDBC Lakehouse Federation

Advanced Databricks notebooks that build on the four-step tutorial in
[`../getting-started`](../getting-started). Run the tutorial first to load the
aircraft digital twin graph and create the UC JDBC connection — these notebooks
assume that baseline is already in place.

## Notebooks

| Notebook | Topic |
|----------|-------|
| `04_metadata_sync_delta_tables.ipynb` | Materialize all Neo4j labels and relationships as managed Delta tables in Unity Catalog |
| `05_metadata_sync_external_api.ipynb` | Register the Neo4j schema through the UC External Metadata API (no data copy) |
| `06_new_federated_queries.ipynb` | Federated query patterns combining `remote_query()`, Spark Connector reads, and Delta joins |
| `07_performance_diagnostics.ipynb` | Probe latency and throughput characteristics of the federation paths |

For programmatic versions of these flows, see [`../validation`](../validation).
For local driver-level SQL-to-Cypher tests, see [`../driver-tests`](../driver-tests).

## Prerequisites

### 1. Connector JAR in a UC Volume

Download the latest Neo4j JDBC Lakehouse Federation Connector from
[neo4j-unity-catalog-connector releases](https://github.com/neo4j-labs/neo4j-unity-catalog-connector/tags)
and upload it to a UC Volume:

```sql
CREATE SCHEMA IF NOT EXISTS main.jdbc_drivers;
CREATE VOLUME IF NOT EXISTS main.jdbc_drivers.jars;
```

Upload to `/Volumes/main/jdbc_drivers/jars/neo4j-unity-catalog-connector-<version>.jar`.

### 2. Databricks Secrets

```bash
cp .env.sample .env
# Edit .env with your Neo4j credentials
./setup.sh
```

| Secret key | Required | Description |
|------------|----------|-------------|
| `host` | Yes | Neo4j host (e.g., `xxxxx.databases.neo4j.io`) |
| `user` | Yes | Neo4j username |
| `password` | Yes | Neo4j password |
| `connection_name` | Yes | UC JDBC connection name |
| `database` | No | Defaults to `neo4j` |

### 3. Cluster Requirements

| Requirement | Metadata Sync (Delta) | Metadata Sync (External API) | Federated Queries | Performance Diagnostics |
|-------------|-----------------------|------------------------------|-------------------|------------------------|
| Access mode | **Single user** | Any | **Single user** | **Single user** |
| Neo4j Spark Connector | **Required** | Not needed | **Required** | **Required** |
| Neo4j Python driver | **Required** | **Required** | Not needed | Not needed |
| SafeSpark metaspace tuning | Not needed | Not needed | **Required** | **Required** |

Install cluster libraries:

- Neo4j Spark Connector — Maven: `org.neo4j:neo4j-connector-apache-spark_2.12:5.4.0_for_spark_3`
- Neo4j Python driver — PyPI: `neo4j`

SafeSpark metaspace setting:

```text
spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib 128
```

## Reference Documentation

- [`../docs/neo4j_uc_jdbc_guide.md`](../docs/neo4j_uc_jdbc_guide.md) — UC JDBC connection setup
- [`../docs/metadata_synchronization.md`](../docs/metadata_synchronization.md) — Metadata sync design
- [Neo4j JDBC Driver Manual](https://neo4j.com/docs/jdbc-manual/current/)
- [Databricks JDBC Unity Catalog Connection](https://docs.databricks.com/aws/en/connect/jdbc-connection)
