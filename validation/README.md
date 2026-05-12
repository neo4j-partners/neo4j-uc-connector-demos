# validation

Batch validation scripts for Neo4j Unity Catalog federation on Databricks.

Use `validate.py` as the only validation entry point:

```bash
cd validation
uv run python validate.py --help
```

The script is a thin wrapper around `databricks bundle deploy` and
`databricks bundle run`. The Databricks Asset Bundle is defined in the repo-root
`databricks.yml` and `resources/*.yml` (the `notebook_parity`, `extras`, and
`metadata` jobs). Runtime configuration is read from the repo-root `../.env`,
converted into `--var name=value` flags, and passed to the bundle commands.
Neo4j credentials are stored in a Databricks secret scope by `./create_secrets.sh`
at the repo root.

## What Runs By Default

`uv run python validate.py run` deploys the bundle and runs the `notebook_parity`
job. The job executes notebook-parity scripts in notebook order as a multi-task
DAG. These scripts are the non-notebook execution path for the code demonstrated
in `getting-started/` and `advanced-patterns/`.

| Script | Purpose |
|--------|---------|
| `run_00_load_graph.py` | Matches `getting-started/00-load-graph.ipynb`: loads the aircraft graph into Neo4j from CSV in a UC Volume |
| `run_01_connection_setup.py` | Matches `getting-started/01-neo4j-uc-connection-setup.ipynb`: creates `sensor_readings`, lakehouse helper tables, and the UC JDBC connection |
| `run_02_federated_queries.py` | Matches `getting-started/02-federated-queries.ipynb`: runs the three live federated query sections |
| `run_03_materialized_tables.py` | Matches `getting-started/03-materialized-tables.ipynb`: materializes `neo4j_*` Delta tables and validates SQL/federated queries |
| `run_05_metadata_sync_external_api.py` | Matches `advanced-patterns/05_metadata_sync_external_api.ipynb`: registers Neo4j schema through the External Metadata API |
| `run_06_new_federated_queries.py` | Matches `advanced-patterns/06_new_federated_queries.ipynb`: validates advanced `remote_query()` SQL and Delta joins |

`advanced-patterns/07_performance_diagnostics.ipynb` is a manual notebook for
investigating latency and throughput. It has no automated validation.

## Extra Regression Coverage

Broader smoke checks live in a separate `extras` job:

```bash
uv run python validate.py extras
```

| Script | Purpose |
|--------|---------|
| `run_extra_connection_smoke.py` | Broader connection checks across Python driver, Spark Connector, direct JDBC, UC JDBC, and `remote_query()` |
| `run_extra_federated_regression.py` | Broader federated query regression checks beyond notebook 02 |
| `run_extra_metadata_sync_tables.py` | Discovers and materializes all Neo4j labels and relationships as Delta tables |

## Prerequisites

- `uv`
- Databricks CLI installed and a configured profile (see `databricks.yml` for the target profile name)
- Access to a Databricks workspace with Unity Catalog enabled
- An all-purpose cluster ID for the jobs
- Neo4j Aura host, username, password, and database
- Neo4j Unity Catalog connector JAR uploaded to a UC Volume
- A UC Volume configured by `UC_CATALOG`, `UC_SCHEMA`, and `UC_VOLUME`

The validation jobs create the Delta tables they need. They do not
assume `aircraft`, `systems`, `sensors`, or `sensor_readings` already exist in
the lakehouse schema.

## Data Setup

Run the data and secrets setup once before the first `validate.py run`. The
dataset comes from:

```text
getting-started/data/aircraft_digital_twin_data/
```

From the repo root:

```bash
./getting-started/upload_data.sh
./create_secrets.sh
```

`upload_data.sh` creates the configured UC schema and managed volume when
needed, then copies the CSV files to
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
DATABRICKS_CLUSTER_ID=
DATABRICKS_SECRET_SCOPE=neo4j-uc-demos
NEO4J_URI=neo4j+s://<instance>.databases.neo4j.io
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

First-time setup, run once from the repo root:

```bash
./getting-started/upload_data.sh
./create_secrets.sh
```

Then run from `validation/`:

```bash
uv sync --locked
uv run python validate.py check
uv run --with neo4j python tools/check_neo4j_auth.py
uv run python validate.py run
```

For repeat runs where the bundle is already deployed and only the job needs
to run:

```bash
uv run python validate.py run --skip-deploy
```

To target a non-default bundle target:

```bash
uv run python validate.py run --target prod
```

## Metadata

The metadata validation has two paths:

- `run_extra_metadata_sync_tables.py` materializes discovered Neo4j labels and
  relationship types as Delta tables in Unity Catalog.
- `run_05_metadata_sync_external_api.py` registers the same discovered Neo4j
  schema as Unity Catalog External Metadata entries through
  `/api/2.0/lineage-tracking/external-metadata`.

`run_05_metadata_sync_external_api.py` requires `CREATE_EXTERNAL_METADATA` on
the metastore. The `metadata` DAB job handles this automatically: its first
task (`grant_external_metadata`) issues the grant, then the materialization
and External Metadata API tasks run in parallel.

Run the metadata suite end to end from `.env`:

```bash
uv run python validate.py metadata
```

This deploys the bundle and runs the `metadata` job. Run `./create_secrets.sh`
from the repo root first if the secret scope is not already provisioned.

To grant the privilege to a specific principal, set `METADATA_GRANT_PRINCIPAL`
in `.env`. An empty value grants to the current run user.

For repeat runs where the bundle is already deployed:

```bash
uv run python validate.py metadata --skip-deploy
```

The grant task requires `DATABRICKS_CLUSTER_ID` because the privilege is
issued through SQL on a workspace cluster.

## Common Commands

```bash
uv run python validate.py check                  # ruff + databricks bundle validate
uv run python validate.py run                    # deploy + run notebook_parity job
uv run python validate.py extras                 # deploy + run extras job
uv run python validate.py metadata               # deploy + run metadata job
uv run python validate.py run --skip-deploy      # rerun without redeploying
uv run python validate.py run --target prod      # use a non-default bundle target
```

Data and secrets setup (run from the repo root, not `validation/`):

```bash
./getting-started/upload_data.sh
./create_secrets.sh
```

## Driver Tests Split

Driver-level SQL translation coverage lives in
[`../driver-tests`](../driver-tests). This Spark suite keeps the Databricks
behaviors: `remote_query()` schema inference, Spark execution, and joins
between remote Neo4j results and Delta lakehouse tables.

## Notes

- `NEO4J_USERNAME` and `NEO4J_PASSWORD` are secret keys for validation, so they
  are not passed as plaintext bundle variables. `create_secrets.sh` also stores
  the notebook UC and JDBC settings in the same scope.
- Keep the repo-root `.env` local. Use `../.env.sample` for documented
  defaults.
- `run_05_metadata_sync_external_api.py` requires `CREATE_EXTERNAL_METADATA` on
  the metastore. See
  [docs/metadata_synchronization.md](../docs/metadata_synchronization.md#prerequisites-granting-create_external_metadata).
