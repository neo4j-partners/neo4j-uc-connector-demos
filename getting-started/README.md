# Getting Started: Neo4j + Databricks Unity Catalog Federation

An aircraft operations graph stores which aircraft have which sensors, how systems are connected, and which flights experienced delays. A lakehouse stores the actual sensor telemetry — 172,800 hourly readings from 160 sensors over 45 days. An analyst needs both in the same query: traverse graph topology in Neo4j to find relevant sensors, then join against the time-series readings in Delta. Neither system can answer that question alone.

These four notebooks walk through the integration pattern that connects Neo4j to Databricks through Unity Catalog's JDBC support. The connector translates SQL to Cypher automatically, so Spark treats Neo4j labels like tables. The progression moves from loading the graph, to validating the connection, to running federated queries across both systems, to materializing graph data as Delta tables where standard SQL and Genie can reach it.

## Architecture

```
                        ┌─────────────────────────┐
                        │   Spark SQL / Genie      │
                        └────────┬────────────────┘
                                 │
                 ┌───────────────┼───────────────┐
                 │               │               │
                 ▼               ▼               ▼
          ┌────────────┐  ┌───────────┐  ┌─────────────────┐
          │ Delta      │  │ UC JDBC   │  │ Materialized    │
          │ Tables     │  │ Connection│  │ Delta Tables    │
          │            │  │           │  │ (from Neo4j)    │
          │ sensor_    │  │ SQL→Cypher│  │                 │
          │ readings   │  │ translation│ │ neo4j_aircraft  │
          │            │  │           │  │ neo4j_sensors   │
          └────────────┘  └─────┬─────┘  │ neo4j_flights   │
                                │        │ neo4j_delays    │
                                │        └────────┬────────┘
                                │                 │
                                ▼          periodic refresh
                        ┌──────────────┐          │
                        │  Neo4j Aura  │◄─────────┘
                        │              │
                        │  Aircraft    │
                        │  System      │
                        │  Sensor      │
                        │  Flight      │
                        │  Delay       │
                        └──────────────┘
```

## Data Model

The dataset is an aircraft digital twin: 20 aircraft across three operators, each with systems, components, sensors, flights, maintenance events, and delays.

**Neo4j (graph-native data)**
- 20 Aircraft nodes with `aircraftId`, `model`, `manufacturer`, `operator`
- 80 System nodes linked by `HAS_SYSTEM` relationships
- 320 Component nodes linked by `HAS_COMPONENT`
- 160 Sensor nodes linked by `HAS_SENSOR`
- 800 Flight nodes with `HAS_DELAY` and `DEPARTS_FROM`/`ARRIVES_AT` airport connections
- 300 MaintenanceEvent nodes linked by `HAS_EVENT`
- 514 Delay nodes
- 12 Airport nodes

**Databricks Delta (tabular data)**
- `sensor_readings` — 172,800 rows: `readingId`, `sensorId`, `ts`, `value`

The `sensorId` is the join key across both systems. Graph topology answers which sensors belong to which aircraft systems; Delta analytics answers what those sensors actually measured.

## Notebooks

### 00 — Load Graph

Loads the full aircraft digital twin dataset into Neo4j from CSV files in a UC Volume. Clears any existing data, creates indexes on all ID properties, loads all eight node types, creates all eight relationship types, and verifies expected counts.

Run this once before the other notebooks.

### 01 — Simple Connect Test

Creates the `sensor_readings` Delta table from the CSV in the UC Volume, creates and validates the Unity Catalog JDBC connection, and runs basic SQL queries against Neo4j through the connector.

The UC JDBC connection created here is reused by notebooks 02 and 03.

### 02 — Federated Queries

Runs three queries that span both systems. Query 1 joins Neo4j graph topology (aircraft → system → sensor via NATURAL JOIN) with Delta sensor statistics. Query 2 combines Neo4j maintenance event counts with Delta sensor averages per aircraft. Query 3 runs pure Neo4j graph analytics on flight operations and delay causes.

### 03 — Materialized Tables

Reads all Neo4j node labels via UC JDBC and writes them as managed Delta tables in Unity Catalog. Once materialized, the data supports full SQL: GROUP BY, ORDER BY, WHERE, aggregations, DISTINCT, and multi-table JOINs — all without JDBC at query time. Four federated queries then join the materialized graph tables with `sensor_readings` in pure SQL.

## Setup

### Prerequisites

#### 1. Databricks Preview Features

Enable these preview features in your Databricks workspace:

| Feature | Required For |
|---------|--------------|
| Custom JDBC on UC Compute | Loading custom JDBC drivers in UC connections |
| remote_query table-valued function | Using `remote_query()` SQL function |

#### 2. Neo4j Aura Instance

A Neo4j Aura instance with Bolt connectivity on port 7687. The notebooks use the `neo4j+s://` URI scheme (Bolt over TLS), which is the default for Aura.

#### 3. Neo4j JDBC Lakehouse Federation Connector JAR

Download the latest release from [neo4j-unity-catalog-connector releases](https://github.com/neo4j-labs/neo4j-unity-catalog-connector/tags) and upload it to a Unity Catalog Volume.

The `java_dependencies` option in `CREATE CONNECTION TYPE JDBC` only accepts UC Volume paths (e.g., `/Volumes/catalog/schema/jars/neo4j-unity-catalog-connector.jar`). The JAR must be in a UC Volume.

#### 4. Cluster Libraries

Install on your Databricks cluster:

| Library | Version | Purpose |
|---------|---------|---------|
| neo4j (Python) | 6.0+ | Neo4j Python Driver — required for notebook 00 |

For UC JDBC connections, the `java_dependencies` option in `CREATE CONNECTION` references the JAR in a UC Volume. The Python driver is only needed for notebook 00 (graph loading).

### Required Spark Configuration

Add these settings to your Databricks cluster configuration:

```
spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib 128
spark.databricks.safespark.jdbcSandbox.jvm.xmx.mib 300
spark.databricks.safespark.jdbcSandbox.size.default.mib 512
```

Without these settings, UC JDBC connections to Neo4j will fail with: `Connection was closed before the operation completed`

### Upload CSV Data

CSV files are in `getting-started/data/aircraft_digital_twin_data/`. Upload them to your UC Volume before running the notebooks:

```bash
cd validation
cp .env.sample .env   # fill in UC_CATALOG, UC_SCHEMA, UC_VOLUME, DATABRICKS_PROFILE
./upload_data.sh
```

This copies all CSV files to `/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{UC_VOLUME}/`.

### Set Up Databricks Secrets

Notebooks use Databricks secrets for Neo4j credentials rather than hardcoded values. Set up the secret scope from the same `.env` file:

```bash
cd validation
./create_secrets.sh
```

This creates a secret scope named `validation` (configurable via `DATABRICKS_SECRET_SCOPE` in `.env`) and stores `NEO4J_USERNAME` and `NEO4J_PASSWORD` as secrets.

The notebooks retrieve credentials at runtime:

```python
SECRET_SCOPE = "validation"
NEO4J_USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="NEO4J_USERNAME")
NEO4J_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="NEO4J_PASSWORD")
```

For the full reference on connection setup, query patterns, and troubleshooting, see [docs/neo4j_uc_jdbc_guide.md](../docs/neo4j_uc_jdbc_guide.md).

## Getting Started

1. Upload CSV data: `cd validation && ./upload_data.sh`
2. Create secrets: `cd validation && ./create_secrets.sh`
3. Update the configuration cell in each notebook with your `NEO4J_URI`, `UC_CATALOG`, and `JDBC_JAR_PATH`.
4. Run `00-load-graph.ipynb` to load the aircraft graph into Neo4j.
5. Run `01-simple-connect-test.ipynb` to create the `sensor_readings` table and UC JDBC connection.
6. Run `02-federated-queries.ipynb` for live federated queries.
7. Run `03-materialized-tables.ipynb` to materialize graph data as Delta tables.

## Tradeoffs

**Real-time JDBC vs. materialized tables.** The UC JDBC connection in notebook 02 queries Neo4j on every read, so results reflect the current graph state. But SQL operations are limited to what the connector's SQL-to-Cypher translator supports (aggregates, WHERE, GROUP BY, ORDER BY, LIMIT, NATURAL JOINs mapped to graph traversals). Materialized tables in notebook 03 support unrestricted SQL but show a snapshot that must be refreshed.

**camelCase properties.** Neo4j best practice uses camelCase for property names (`aircraftId`, `sensorId`, `flightId`). Graph properties and Delta tables in these notebooks follow that convention. Raw CSV import headers may use snake_case, but notebook 01 normalizes them when creating Delta tables.

**Data volume.** The dataset uses 20 aircraft and 172,800 sensor readings — small enough for quick iteration. The same patterns apply to larger graphs, though materialization becomes more important as graph size and query latency grow.
