# Validation Coverage Manifest

This manifest maps notebook code paths to validation scripts. The validation
suite executes Python scripts as Databricks jobs instead of running notebooks.
Configuration cells are represented by `.env` parameters and Databricks
secrets.

| Notebook | Validation script | Default | Coverage | Intentional differences |
|----------|-------------------|---------|----------|-------------------------|
| `getting-started/00-load-graph.ipynb` | `scripts/run_00_load_graph.py` | Yes | Clears Neo4j, creates indexes, loads node CSVs, loads relationship CSVs, verifies graph counts. | Adds structured pass/fail result collection. |
| `getting-started/01-neo4j-uc-connection-setup.ipynb` | `scripts/run_01_connection_setup.py` | Yes | Creates tutorial schema and volume, creates `sensor_readings`, creates the UC JDBC connection, validates `SELECT 1`, aircraft count, airport count, and flights by operator. | Also creates lakehouse helper tables `aircraft`, `systems`, `sensors`, and `sensor_readings` for advanced notebook 06 so the default suite does not assume pre-created lakehouse tables. |
| `getting-started/02-federated-queries.ipynb` | `scripts/run_02_federated_queries.py` | Yes | Runs the three notebook federated query sections against the tutorial schema and UC JDBC connection. | Notebook `show()` output is kept, with row-count assertions added. |
| `getting-started/03-materialized-tables.ipynb` | `scripts/run_03_materialized_tables.py` | Yes | Verifies sources, materializes all `neo4j_*` tables, checks `INFORMATION_SCHEMA`, runs SQL validation tests, and runs the four materialized-table federated queries. | Display-only cells are converted into assertions that fail the Databricks job on empty or mismatched results. |
| `advanced-patterns/05_metadata_sync_external_api.ipynb` | `scripts/run_05_metadata_sync_external_api.py` | Yes | Discovers Neo4j schema, registers a test label, registers all labels and relationship types, lists registered metadata. | Adds idempotent handling for existing metadata, relationship pattern metadata for relationship types without properties, and best-effort cleanup for objects created during validation. |
| `advanced-patterns/06_new_federated_queries.ipynb` | `scripts/run_06_new_federated_queries.py` | Yes | Runs the advanced `remote_query()` GROUP BY, HAVING, ORDER BY, DISTINCT, LIMIT, OFFSET, JOIN, and federated Delta join cells. | Adds assertions around each display query. |
| `advanced-patterns/07_performance_diagnostics.ipynb` | `scripts/run_07_performance_diagnostics.py` | No | Optional timing diagnostics for direct Neo4j, `remote_query()`, Delta, Spark Connector, compound `remote_query()`, explain plans, and warm-up timing. | Runs only with `uv run python validate.py run --include-performance`; timings are diagnostic and have no thresholds. |

## Extra Regression Scripts

These scripts are not notebook-parity checks. They preserve useful broader
coverage from the previous validation suite and run only with
`uv run python validate.py run --include-extras`.

| Script | Purpose |
|--------|---------|
| `scripts/run_extra_connection_smoke.py` | Broader connection smoke checks across Python driver, Spark Connector, direct JDBC, UC JDBC, and `remote_query()`. |
| `scripts/run_extra_federated_regression.py` | Broader federated regression checks that combine Neo4j, Spark Connector reads, and Delta joins beyond notebook 02. |
| `scripts/run_extra_metadata_sync_tables.py` | Full schema discovery and materialization of all labels and relationship types into metadata schemas. |

## Review Rule

When notebook code cells change, update the mapped validation script and this
manifest in the same change. If a notebook cell is intentionally not validated,
record the reason in the `Intentional differences` column.
