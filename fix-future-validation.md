# Future Validation Cleanup

Notes from running the end-to-end validation pipeline on 2026-05-11.

## Workspace Upload Reliability

- `uv run python validate.py run` reported that all scripts uploaded to
  `/Users/ryan.knight@neo4j.com/validate_federation/scripts`, but a later
  `databricks workspace list` could not see that directory.
- The first three jobs still ran successfully, then
  `run_03_materialized_tables.py` failed once with:
  `Cannot read the python file ... Either it does not exist, or the identity used to run this job lacks the required permissions.`
- Workaround used during the run:
  `databricks workspace mkdirs .../validate_federation/scripts`, followed by
  `databricks workspace import-dir validation/scripts .../validate_federation/scripts --overwrite`.
- Fix: make the runner upload path and the Databricks CLI-visible workspace
  path agree, then add a post-upload existence check for each script before
  jobs are submitted.

## Error Handling

- A Databricks SDK `OperationFailed` escaped as a traceback from
  `validate.py run` instead of being reported as a normal validation failure.
- Fix: catch the relevant Databricks SDK job wait exception in `main()` or
  inside `run_suite()`, then print a concise error plus the run ID/log command.

## Buffered Output

- During long remote job waits, local output was buffered for several minutes.
  This made it look like the command might be stuck even though Databricks was
  actively running the job.
- Fix: run Python unbuffered in documentation, flush key `print()` calls, or add
  periodic progress output around job waits.

## Workspace Diagnostic Command

- `uv run python validate.py workspace run_03_materialized_tables.py` reported
  that the remote directory did not exist and suggested
  `python -m cli upload --all`, which does not match this repo's documented
  command.
- Fix: update the runner hint to use `uv run python validate.py upload --all`
  when `cli_command` is configured.

## Generated Files In Uploads

- The manual `databricks workspace import-dir validation/scripts ...` uploaded
  `__pycache__` files, including stale bytecode for removed scripts such as the
  deleted performance diagnostics script.
- Fix: document the CLI import workaround with an exclude step, or avoid the
  workaround once runner upload verification is fixed.

## Documentation Drift

- `validate-align.md` still contains a completed Phase 6 section describing an
  optional performance validation script and `--include-performance` flag, but
  performance validation code has been removed.
- Fix: update the plan document so it says performance diagnostics are manual
  only and no validation CLI performance path exists.

## End-to-End Definition

- The default `validate.py run` path includes notebook parity, including the
  external metadata API script, but `validate.py metadata` is a separate command
  with the grant step and extra metadata materialization script.
- Fix: document whether "complete end-to-end validation" means:
  `upload_data.sh` + `create_secrets.sh` + `validate.py run`, or whether it
  also requires `validate.py metadata`.
- During the run, `validation/validate.py` changed from the direct
  `databricks-job-runner` submitter to a Databricks Asset Bundle wrapper. Make
  sure docs and future runbooks describe the bundle workflow:
  `uv run python validate.py check`, then `uv run python validate.py run`.

## Notebook Parity Assertions

- `run_06_new_federated_queries.py` originally expected the
  `Federated: GROUP BY + Delta` query to return at least 20 joined
  aircraft/severity rows and sum to all 300 maintenance events.
- The notebook cell is display-only, and the current sample sensor readings
  join to one aircraft in that Delta pattern. The strict row-count expectation
  failed even though the notebook-equivalent query returned a valid joined row.
- Fix applied during the run: validate that the result is non-empty, maintenance
  counts are positive, and joined sensor aggregates are present.
- Future cleanup: define explicit expected cardinalities only where the
  notebook or dataset contract actually guarantees them.

## Notebook Query Semantics

- `getting-started/03-materialized-tables.ipynb` Section 5,
  "Federated Query 1: Aircraft Health Overview", appears to overcount
  `critical_events`.
- The query joins aircraft to maintenance events, sensors, and all
  `sensor_readings`, then calculates:
  `SUM(CASE WHEN m.severity = 'CRITICAL' THEN 1 ELSE 0 END)`.
- Because each maintenance row is multiplied by sensor reading rows, the output
  showed values such as `69120` critical events for one aircraft, while the
  dataset has only 300 total maintenance events.
- Fix: pre-aggregate maintenance events per aircraft before joining to sensor
  readings, or change the expression to count distinct critical event IDs.
  Update the matching validation script at the same time.

## Local Environment Drift

- An old direct-runner waiter failed locally with a missing certifi CA bundle
  path after the validation environment changed. The Databricks workload was
  still running; only local polling failed.
- Fix: avoid changing validation dependencies while a local waiter is running,
  and document how to recover by checking the Databricks run directly with
  `databricks jobs get-run <run-id>`.
