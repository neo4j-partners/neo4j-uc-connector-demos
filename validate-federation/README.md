# validate-federation

Batch validation scripts for Neo4j Unity Catalog federation on Databricks.

The scripts use `databricks-job-runner==0.4.8` to upload local Python files to a
Databricks workspace and submit them as one-time jobs. Runtime configuration is
read from `.env` and passed to each job as task parameters. Neo4j credentials are
stored in a Databricks secret scope.

## What Runs

| Script | Purpose |
|--------|---------|
| `test_hello.py` | Smoke test for remote Python and Spark execution |
| `run_01_connection_validation.py` | Validates Neo4j driver, Spark connector, direct JDBC, UC connection creation, and `remote_query()` |
| `run_02_federated_queries.py` | Runs federated query patterns across Neo4j and Delta lakehouse tables |
| `run_03_metadata_sync_tables.py` | Materializes discovered Neo4j labels and relationships as Delta tables |
| `run_04_metadata_sync_api.py` | Registers discovered Neo4j schema through the External Metadata API |
| `run_05_advanced_spark_queries.py` | Validates advanced Spark `remote_query()` SQL and remote_query + Delta joins |

## Prerequisites

- `uv`
- Databricks CLI configured, or Databricks SDK environment auth
- Access to a Databricks workspace with Unity Catalog enabled
- A cluster ID for `cluster` mode, or access to Databricks serverless jobs
- Neo4j Aura host, username, password, and database
- Neo4j Unity Catalog connector JAR uploaded to a UC Volume
- Delta lakehouse tables used by `run_02`: `aircraft`, `systems`, `sensors`, and `sensor_readings`

The Delta tables should use the same lowerCamelCase identifier convention as
the Neo4j graph properties:

| Table | Required identifier columns |
|-------|-----------------------------|
| `aircraft` | `aircraftId` |
| `systems` | `systemId`, `aircraftId` |
| `sensors` | `sensorId`, `systemId` |
| `sensor_readings` | `readingId`, `sensorId` |

Do not expose raw Neo4j import headers such as `:ID(Aircraft)` in the lakehouse
tables. The sample loader in `sample-validation/scripts/run_01_connect_test.py`
normalizes those CSV headers when creating the Delta tables.

## Data Setup

Populate the aircraft digital twin sample data before running this validation
suite. The dataset comes from:

```text
getting-started/data/aircraft_digital_twin_data/
```

The baseline setup is loaded by the `sample-validation` flow. From the
repository root:

```bash
cd sample-validation
cp .env.sample .env
# Fill in UC_CATALOG, UC_SCHEMA, UC_VOLUME, Neo4j credentials, and
# Databricks cluster/workspace config.

./upload_data.sh
./create_secrets.sh

uv run python -m cli upload run_00_load_graph.py
uv run python -m cli submit run_00_load_graph.py

uv run python -m cli upload run_01_connect_test.py
uv run python -m cli submit run_01_connect_test.py
```

Use the same Neo4j instance, UC connection name, JDBC JAR path, and Databricks
workspace settings when configuring `validate-federation/.env`. Set
`LAKEHOUSE_CATALOG` and `LAKEHOUSE_SCHEMA` to the `UC_CATALOG` and `UC_SCHEMA`
used by `sample-validation`.

This sample flow uploads the CSV files to the UC Volume, loads the Neo4j graph,
creates the Databricks secrets, creates the UC JDBC connection, and creates the
`sensor_readings` Delta table. The `run_02_federated_queries.py` validation also
checks for Delta tables named `aircraft`, `systems`, and `sensors`; make sure
those lakehouse tables exist in `LAKEHOUSE_CATALOG.LAKEHOUSE_SCHEMA` before
running the full suite.

## Quick Start

From this folder:

```bash
cp .env.sample .env
```

Edit `.env` and set:

```text
DATABRICKS_PROFILE=
DATABRICKS_COMPUTE_MODE=cluster
DATABRICKS_CLUSTER_ID=
DATABRICKS_WORKSPACE_DIR=/Users/<your-email>/validate_federation
DATABRICKS_SECRET_SCOPE=validate_federation
NEO4J_HOST=<instance>.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=
NEO4J_DATABASE=neo4j
UC_CONNECTION_NAME=aircraft_connection_v2
JDBC_JAR_PATH=/Volumes/<catalog>/<schema>/<volume>/neo4j-unity-catalog-connector.jar
LAKEHOUSE_CATALOG=
LAKEHOUSE_SCHEMA=lakehouse
METADATA_CATALOG=neo4j_metadata
NODES_SCHEMA=nodes
RELATIONSHIPS_SCHEMA=relationships
METADATA_GRANT_PRINCIPAL=
```

`NEO4J_HOST` should be the host only. `NEO4J_URI=neo4j+s://...` is also
supported for compatibility with `sample-validation`.

Create the Databricks secret scope and store the Neo4j credentials:

```bash
./create_secrets.sh
```

Verify the local CLI resolves `databricks-job-runner`:

```bash
uv sync --locked
uv run python -m cli --help
uv run python -c "from importlib.metadata import version; print(version('databricks-job-runner'))"
```

Expected version: `0.4.8`. In this repository, `pyproject.toml` resolves the
runner from the local editable checkout at `../../databricks-job-runner`.

Validate Neo4j credentials locally before submitting jobs:

```bash
uv run --with neo4j python tools/check_neo4j_auth.py
```

Upload and run the smoke test:

```bash
./upload.sh test_hello.py
./submit.sh test_hello.py
```

Run the full validation suite:

```bash
./validate.sh
```

`validate.sh` first runs local checks:

```bash
uv sync --locked
uv run python -c "from importlib.metadata import version; print(version('databricks-job-runner'))"
uv run python -c "from databricks_job_runner import Runner; print(Runner)"
uv run python -m cli --help
uv run python -m compileall cli scripts tools
```

It also syntax-checks the shell wrappers, uploads all validation scripts, and
submits `run_01_connection_validation.py`, `run_02_federated_queries.py`,
`run_03_metadata_sync_tables.py`, `run_04_metadata_sync_api.py`, and
`run_05_advanced_spark_queries.py` as Databricks jobs. The suite exits non-zero
if any local check or remote job fails.

To use serverless compute for a run:

```bash
./submit.sh run_01_connection_validation.py --compute serverless
./validate.sh --compute serverless
```

## Metadata

### Overview

The metadata validation has two paths:

- `run_03_metadata_sync_tables.py` materializes discovered Neo4j labels and
  relationship types as Delta tables in Unity Catalog.
- `run_04_metadata_sync_api.py` registers the same discovered Neo4j schema as
  Unity Catalog External Metadata entries through
  `/api/2.0/lineage-tracking/external-metadata`.

External Metadata registration is metadata-only. It does not copy graph data;
it records labels, relationship types, property names, and encoded Neo4j type
details so governance and lineage workflows can discover the graph schema.

`run_04_metadata_sync_api.py` requires `CREATE_EXTERNAL_METADATA` on the
metastore for the workspace principal that submits the job. The reusable helper
uses the working approach from `grant_magic.md`: submit a one-shot PySpark job
to the configured workspace cluster and issue the grant with Spark SQL.

### Quick Start

Run the metadata suite end to end from `.env`:

```bash
./validate_metadata.sh
```

By default this creates or updates the Databricks secret scope from `.env`,
grants `CREATE_EXTERNAL_METADATA`, uploads the validation scripts, and submits
`run_03_metadata_sync_tables.py` and `run_04_metadata_sync_api.py`.

To grant the privilege to a specific principal during the automated run, set
`METADATA_GRANT_PRINCIPAL` in `.env`, or run the grant helper directly:

```bash
./grant_external_metadata.sh user@example.com
```

For repeat runs where secrets, grants, or uploads are already current:

```bash
./validate_metadata.sh --skip-secrets --skip-grant --skip-upload
```

To run the metadata jobs on serverless compute:

```bash
./validate_metadata.sh --compute serverless
```

If the script reports `PERMISSION_DENIED`, confirm the grant job ran on a
cluster where the submitting user is a metastore admin or otherwise allowed to
grant metastore-level privileges.

## Common Commands

```bash
uv run python -m cli upload --all
uv run python -m cli upload run_01_connection_validation.py
uv run python -m cli submit run_01_connection_validation.py
uv run python -m cli submit run_05_advanced_spark_queries.py
uv run python -m cli submit run_01_connection_validation.py --compute serverless
./validate_metadata.sh
uv run python -m cli validate run_01_connection_validation.py
uv run python -m cli logs
uv run python -m cli logs <run-id>
```

## Direct JDBC Split

Driver-level SQL translation coverage lives in
[`../direct-jdbc-validation`](../direct-jdbc-validation). This Spark suite keeps
the Databricks behaviors: `remote_query()` schema inference, Spark execution,
and joins between remote Neo4j results and Delta lakehouse tables.

## Notes

- `NEO4J_USERNAME` and `NEO4J_PASSWORD` are listed in `cli/__init__.py` as secret keys, so they are not passed as plaintext job parameters.
- Keep `.env` local. Use `.env.sample` for documented defaults.
- The wrapper scripts default `UV_CACHE_DIR` to `.uv-cache/` inside this folder. Override `UV_CACHE_DIR` if you want to use a shared cache.
- `run_04_metadata_sync_api.py` requires `CREATE_EXTERNAL_METADATA` on the metastore. See the Metadata section, `grant_magic.md`, and `grant_external_metadata.sh` for the grant workflow.
