# validation

Batch validation scripts for Neo4j Unity Catalog federation on Databricks.

The scripts use `databricks-job-runner==0.4.8` to upload local Python files to a
Databricks workspace and submit them as one-time jobs. Runtime configuration is
read from the repo-root `../.env` and passed to each job as task parameters.
Neo4j credentials are stored in a Databricks secret scope, provisioned by the
repo-root `../create_secrets.sh`.

## What Runs By Default

`validate.sh` runs notebook-parity scripts in notebook order. These scripts are
the non-notebook execution path for the code demonstrated in `getting-started/`
and `advanced-patterns/`.

| Script | Purpose |
|--------|---------|
| `run_00_load_graph.py` | Matches `getting-started/00-load-graph.ipynb`: loads the aircraft graph into Neo4j from CSV in a UC Volume |
| `run_01_connection_setup.py` | Matches `getting-started/01-neo4j-uc-connection-setup.ipynb`: creates `sensor_readings`, lakehouse helper tables, and the UC JDBC connection |
| `run_02_federated_queries.py` | Matches `getting-started/02-federated-queries.ipynb`: runs the three live federated query sections |
| `run_03_materialized_tables.py` | Matches `getting-started/03-materialized-tables.ipynb`: materializes `neo4j_*` Delta tables and validates SQL/federated queries |
| `run_05_metadata_sync_external_api.py` | Matches `advanced-patterns/05_metadata_sync_external_api.ipynb`: registers Neo4j schema through the External Metadata API |
| `run_06_new_federated_queries.py` | Matches `advanced-patterns/06_new_federated_queries.ipynb`: validates advanced `remote_query()` SQL and Delta joins |

`advanced-patterns/07_performance_diagnostics.ipynb` is optional and excluded
from the default pass/fail suite. Run it with `./validate.sh --include-performance`.

See [`coverage_manifest.md`](coverage_manifest.md) for the notebook-to-script
coverage map and intentional differences.

## Extra Regression Coverage

The previous broader smoke checks are preserved, but they are not notebook
parity. Run them with `./validate.sh --include-extras`.

| Script | Purpose |
|--------|---------|
| `run_extra_connection_smoke.py` | Broader connection checks across Python driver, Spark Connector, direct JDBC, UC JDBC, and `remote_query()` |
| `run_extra_federated_regression.py` | Broader federated query regression checks beyond notebook 02 |
| `run_extra_metadata_sync_tables.py` | Discovers and materializes all Neo4j labels and relationships as Delta tables |

## Prerequisites

- `uv`
- Databricks CLI configured, or Databricks SDK environment auth
- Access to a Databricks workspace with Unity Catalog enabled
- A cluster ID for `cluster` mode, or access to Databricks serverless jobs
- Neo4j Aura host, username, password, and database
- Neo4j Unity Catalog connector JAR uploaded to a UC Volume
- A UC Volume configured by `UC_CATALOG`, `UC_SCHEMA`, and `UC_VOLUME`

The aligned validation path creates the Delta tables it needs. It no longer
assumes `aircraft`, `systems`, `sensors`, or `sensor_readings` already exist in
the lakehouse schema.

## Data Setup

`validate.sh` sets up data before submitting Databricks jobs. The dataset comes
from:

```text
getting-started/data/aircraft_digital_twin_data/
```

After configuring the repo-root `../.env` (see Quick Start below), the default
validation flow runs these setup steps for you:

```bash
./getting-started/upload_data.sh
./create_secrets.sh
```

`getting-started/upload_data.sh` creates the configured UC schema and managed
volume when needed, then copies the CSV files to
`/Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}/`. `run_00_load_graph.py`
reads them on the cluster and writes the graph into Neo4j.
`run_01_connection_setup.py` creates the tutorial `sensor_readings` table and
the normalized lakehouse helper tables used by advanced-patterns/06.

## Quick Start

From the repo root:

```bash
cp .env.sample .env
```

Edit the root `.env` and fill in the values listed in `.env.sample`. Important
keys for the validation suite:

```text
DATABRICKS_PROFILE=
DATABRICKS_COMPUTE_MODE=cluster
DATABRICKS_CLUSTER_ID=
DATABRICKS_WORKSPACE_DIR=/Users/<your-email>/neo4j-uc-connector-demos
DATABRICKS_SECRET_SCOPE=neo4j-uc-demos
NEO4J_HOST=<instance>.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=
NEO4J_DATABASE=neo4j
UC_CONNECTION_NAME=sample_neo4j_jdbc_connection
JDBC_JAR_PATH=/Volumes/<catalog>/<schema>/<volume>/neo4j-unity-catalog-connector.jar
UC_CATALOG=<catalog>
UC_SCHEMA=neo4j_getting_started
UC_VOLUME=aircraft_data
LAKEHOUSE_CATALOG=
LAKEHOUSE_SCHEMA=lakehouse
METADATA_CATALOG=neo4j_metadata
NODES_SCHEMA=nodes
RELATIONSHIPS_SCHEMA=relationships
METADATA_GRANT_PRINCIPAL=
```

`NEO4J_HOST` should be the host only. `NEO4J_URI=neo4j+s://...` is also
supported.

Create the Databricks secret scope and store the Neo4j credentials (from the
repo root):

```bash
./create_secrets.sh
```

The rest of the commands below run from this `validation/` folder.

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

It also syntax-checks the shell wrappers, prints the coverage manifest, uploads
sample data, creates or updates Databricks secrets, uploads all validation
scripts, and submits the notebook-parity scripts listed above. The suite exits
non-zero if any local check or remote job fails.

For repeat runs where sample data, secrets, or uploaded scripts are already
current:

```bash
./validate.sh --skip-data --skip-secrets --skip-upload
```

To include the broader non-notebook regression scripts:

```bash
./validate.sh --include-extras
```

To include optional timing diagnostics:

```bash
./validate.sh --include-performance
```

To use serverless compute for a run:

```bash
./submit.sh run_01_connection_setup.py --compute serverless
./validate.sh --compute serverless
```

## Metadata

### Overview

The metadata validation has two paths:

- `run_extra_metadata_sync_tables.py` materializes discovered Neo4j labels and
  relationship types as Delta tables in Unity Catalog.
- `run_05_metadata_sync_external_api.py` registers the same discovered Neo4j schema as
  Unity Catalog External Metadata entries through
  `/api/2.0/lineage-tracking/external-metadata`.

`run_05_metadata_sync_external_api.py` requires `CREATE_EXTERNAL_METADATA` on the
metastore. See [docs/metadata_synchronization.md](../docs/metadata_synchronization.md#prerequisites-granting-create_external_metadata)
for the grant setup, including why the grant must run as a cluster job and how
`grant_external_metadata.sh` automates it.

### Quick Start

Run the metadata suite end to end from `.env`:

```bash
./validate_metadata.sh
```

By default this creates or updates the Databricks secret scope from `.env`,
grants `CREATE_EXTERNAL_METADATA`, uploads the validation scripts, and submits
`run_extra_metadata_sync_tables.py` and
`run_05_metadata_sync_external_api.py`.

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
uv run python -m cli upload run_01_connection_setup.py
uv run python -m cli submit run_01_connection_setup.py
uv run python -m cli submit run_06_new_federated_queries.py
uv run python -m cli submit run_01_connection_setup.py --compute serverless
./validate_metadata.sh
uv run python -m cli validate run_01_connection_setup.py
uv run python -m cli logs
uv run python -m cli logs <run-id>
```

## Driver Tests Split

Driver-level SQL translation coverage lives in
[`../driver-tests`](../driver-tests). This Spark suite keeps the Databricks
behaviors: `remote_query()` schema inference, Spark execution, and joins
between remote Neo4j results and Delta lakehouse tables.

## Notes

- `NEO4J_USERNAME` and `NEO4J_PASSWORD` are listed in `cli/__init__.py` as secret keys, so they are not passed as plaintext job parameters.
- Keep the repo-root `.env` local. Use `../.env.sample` for documented defaults.
- The wrapper scripts default `UV_CACHE_DIR` to `.uv-cache/` inside this folder. Override `UV_CACHE_DIR` if you want to use a shared cache.
- `run_05_metadata_sync_external_api.py` requires `CREATE_EXTERNAL_METADATA` on the metastore. See [docs/metadata_synchronization.md](../docs/metadata_synchronization.md#prerequisites-granting-create_external_metadata) and `grant_external_metadata.sh` for the grant workflow.
