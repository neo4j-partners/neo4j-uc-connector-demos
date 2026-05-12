# Validation Alignment Plan

## Goal

Make `validation/` prove that the code paths demonstrated in `getting-started/`
and `advanced-patterns/` still work without manually running the notebooks.

The intended flow is:

1. Upload sample data with `./getting-started/upload_data.sh`.
2. Create or update Databricks secrets with `./create_secrets.sh`.
3. Run validation jobs that execute the same setup, SQL, DataFrame, metadata,
   and materialization logic as the notebooks.
4. Exclude performance diagnostics from the default run unless explicitly
   requested.

## Assumptions

- The notebooks remain the user-facing walkthroughs.
- Validation runs as Databricks Python jobs, not notebook jobs.
- Validation should run the same logical code as the notebooks, with assertions
  added around the notebook operations.
- Extra smoke or regression coverage is useful, but it should be labeled as
  extra coverage rather than notebook parity.

## Risks

- The current validation scripts are hand-written and may drift from notebook
  code as notebooks change.
- Some current checks are broader than notebook parity, so renaming or splitting
  them needs care to avoid losing useful regression coverage.
- Databricks cluster and serverless behavior can differ, especially for
  federation planning and performance timing.
- The External Metadata API path requires metastore privileges; validation needs
  clear setup and skip/error behavior for that permission.

## Phase Checklist

### Phase 1: Align Script Names And Order

Status: Complete

- [x] Create or rename validation scripts so each non-performance notebook has a
      direct validation counterpart.
- [x] Map `getting-started/00-load-graph.ipynb` to
      `validation/scripts/run_00_load_graph.py`.
- [x] Map `getting-started/01-neo4j-uc-connection-setup.ipynb` to
      `validation/scripts/run_01_connection_setup.py`.
- [x] Map `getting-started/02-federated-queries.ipynb` to
      `validation/scripts/run_02_federated_queries.py`.
- [x] Map `getting-started/03-materialized-tables.ipynb` to
      `validation/scripts/run_03_materialized_tables.py`.
- [x] Map `advanced-patterns/05_metadata_sync_external_api.ipynb` to
      `validation/scripts/run_05_metadata_sync_external_api.py`.
- [x] Map `advanced-patterns/06_new_federated_queries.ipynb` to
      `validation/scripts/run_06_new_federated_queries.py`.
- [x] Decide whether existing broader checks should move to clearly named
      `run_extra_*` scripts.

Validation:

- [x] `validation/README.md` lists notebook-parity scripts in notebook order.
- [x] `validation/validate.py run` submits scripts in notebook order.

Notes:

- Original validation started at the old connection validation script;
  aligned validation now includes graph loading after sample data upload.
- Existing broader checks were preserved as `run_extra_connection_smoke.py`,
  `run_extra_federated_regression.py`, and
  `run_extra_metadata_sync_tables.py`.

### Phase 2: Make Data Setup First-Class

Status: Complete

- [x] Document `./getting-started/upload_data.sh` as the data upload step run
      before submitting Databricks jobs.
- [x] Document `./create_secrets.sh` as the secret-scope provisioning step run
      before submitting Databricks jobs.
- [x] Ensure `run_00_load_graph.py` is uploaded and submitted before all query,
      materialization, and metadata jobs.
- [x] Ensure `run_01_connection_setup.py` creates the Unity Catalog schema and
      volume expected by the tutorial flow.
- [x] Ensure `run_01_connection_setup.py` creates or replaces the
      `sensor_readings` Delta table from `nodes_readings.csv`, matching notebook
      01.
- [x] Ensure `run_01_connection_setup.py` creates or replaces the UC JDBC
      connection, matching notebook 01.
- [x] Remove any default-run assumption that `aircraft`, `systems`, `sensors`,
      or `sensor_readings` already exist unless those tables are created earlier
      in the aligned flow.

Validation:

- [x] The setup path creates the configured UC schema and volume before upload,
      then `run_00` and `run_01` create graph and lakehouse data without
      pre-created lakehouse tables.
- [x] `sensor_readings` row count is validated as 172,800.
- [x] Neo4j graph node counts are validated after load.

Notes:

- If extra lakehouse helper tables such as `aircraft`, `systems`, and `sensors`
  are still required by later validation, they should be created explicitly in
  the aligned setup path or replaced with notebook-equivalent logic.
- `run_01_connection_setup.py` now creates `aircraft`, `systems`, `sensors`,
  and `sensor_readings` in the configured lakehouse schema from the uploaded
  CSV files for advanced-patterns/06.
- Full remote validation still requires Databricks credentials and live
  workspace access; local review verified script syntax and setup ordering.

### Phase 3: Match Notebook Code Cells

Status: Complete

- [x] For each non-performance notebook, inventory the code cells that should be
      represented in validation.
- [x] Copy or port the notebook SQL and DataFrame operations into the matching
      validation script with minimal changes.
- [x] Add assertions around the notebook operations instead of replacing them
      with different queries.
- [x] Keep table names, schemas, projections, joins, filters, and materialized
      outputs consistent with the notebooks.
- [x] Preserve the notebook order of operations inside each validation script.
- [x] Replace notebook display-only calls with checks that fail the job when
      expected results are missing.

Validation:

- [x] Each validation script prints the notebook and section it is validating.
- [x] Each script exits non-zero on missing tables, failed counts, failed API
      calls, or empty query results where the notebook expects rows.
- [x] A manual diff of notebook code cells against validation sections shows no
      unexplained query or operation changes.

Notes:

- Exact code identity may not be practical for configuration, printing, or
  assertions. The important target is identical Spark SQL, DataFrame, JDBC,
  metadata API, and materialization behavior.

### Phase 4: Separate Notebook Parity From Extra Regression Coverage

Status: Complete

- [x] Review existing validation checks that do not correspond directly to a
      notebook cell.
- [x] Keep useful broader checks, but move them to clearly labeled extra
      scripts or sections.
- [x] Decide whether the previous connection validation remains as extra
      connection smoke coverage or is split into notebook parity plus extras.
- [x] Decide whether the current broader federated query checks remain as extras
      after exact notebook query validation exists.
- [x] Update `validate.py run` so the default behavior is clear:
      notebook-parity-only, notebook-parity-plus-extras, or configurable.
- [x] Add a flag if needed, such as `--include-extras`.

Validation:

- [x] README distinguishes notebook-parity validation from extra smoke or
      regression validation.
- [x] Default validation behavior is documented and matches the script behavior.

Notes:

- Do not delete useful checks just because they are not notebook parity. The goal
  is clarity and alignment, not reduced coverage.
- Broader checks now run only with `uv run python validate.py run --include-extras`.

## Completion Criteria

- [ ] A fresh configured workspace can run the aligned validation from sample
      data upload through graph load, connection setup, query validation,
      materialization, metadata sync, and advanced SQL validation. *(Requires
      a live workspace run; verified by executing `./getting-started/upload_data.sh`,
      `./create_secrets.sh`, then `cd validation && uv run python validate.py run`
      against a fresh-configured Databricks workspace.)*
- [x] Every non-performance notebook has a direct validation script, or its
      omission from the default suite is documented in `validation/README.md`.
- [x] Validation executes the same core SQL, DataFrame, JDBC, metadata API, and
      materialization behavior as the notebooks.
- [x] Extra smoke and regression tests are clearly labeled as extra coverage.
- [x] `validation/README.md` explains the setup flow and the notebook coverage
      model.
- [x] The checklist in this file is updated as phases are completed.
