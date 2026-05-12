# Advanced Patterns - Neo4j JDBC Lakehouse Federation

Advanced Databricks notebooks that build on the four-step tutorial in
[`../getting-started`](../getting-started). Run the tutorial first to load the
aircraft digital twin graph and create the UC JDBC connection — these notebooks
assume that baseline is already in place.

## Notebooks

| Notebook | Topic |
|----------|-------|
| `05_metadata_sync_external_api.ipynb` | Register the Neo4j schema through the UC External Metadata API (no data copy) |
| `06_new_federated_queries.ipynb` | Federated query patterns combining `remote_query()`, Spark Connector reads, and Delta joins |
| `07_performance_diagnostics.ipynb` | Probe latency and throughput characteristics of the federation paths |

For the materialization pattern (reading Neo4j via UC JDBC and writing managed Delta tables), see [`../getting-started/03-materialized-tables.ipynb`](../getting-started/03-materialized-tables.ipynb), which also shows how Unity Catalog tracks the materialized schema automatically.

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

Configuration and secrets live in the repo-root `.env` and are provisioned by
the repo-root `create_secrets.sh`:

```bash
# From the repo root:
cp .env.sample .env
# Edit .env with your Neo4j credentials
./create_secrets.sh
```

`create_secrets.sh` stores `NEO4J_USERNAME` and `NEO4J_PASSWORD` in the scope
named by `DATABRICKS_SECRET_SCOPE` (default `neo4j-uc-demos`). Each notebook's
configuration cell hardcodes non-secret values (host, connection name, lakehouse
catalog/schema) — edit them for your environment.

### 3. Cluster Requirements

| Requirement | Metadata Sync (External API) | Federated Queries | Performance Diagnostics |
|-------------|------------------------------|-------------------|------------------------|
| Access mode | Any | **Single user** | **Single user** |
| Neo4j Spark Connector | Not needed | **Required** | **Required** |
| Neo4j Python driver | **Required** | Not needed | Not needed |
| SafeSpark metaspace tuning | Not needed | **Required** | **Required** |

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
