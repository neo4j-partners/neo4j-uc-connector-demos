# Test Plan: Refactor Verification

This plan verifies the cleanup that renamed `validate-federation/` → `validation/`,
renamed `direct-jdbc-validation/` → `driver-tests/`, consolidated
`sample-validation/` into `validation/`, deleted `deploy-lakebase/`, removed
seven overlapping notebooks from `examples/`, and updated all docs and site
references.

Run phases in order. Phase 1 is fast and requires no Databricks workspace. Stop
and fix any failure before moving to the next phase.

## Status (2026-05-08)

| Phase | Status | Notes |
|-------|--------|-------|
| 1. Local static checks | ✅ PASSED | Covered during the quality-review pass after the refactor |
| 2. Documentation sanity | ✅ PASSED | Covered during the quality-review pass after the refactor |
| 3. Validation suite | ✅ PASSED | `./validate.sh` (5/5) and `./validate_metadata.sh` (2/2) green; 7 Databricks runs SUCCESS |
| 3.9 Serverless variant | ⏭️ skipped | Optional |
| 4. Tutorial notebooks | ⏳ pending | Manual workspace runs |
| 5. Examples notebooks | ⏳ pending | Manual workspace runs |
| 6. Git history spot checks | ⏳ pending | Run before commit |

## Phase 1: Local Static Checks (no Databricks)

Goal: catch broken paths, stale references, and code that no longer parses.

1. **Stale path grep.** No tracked file should reference the old folder names.

   ```bash
   git grep -nE 'validate-federation|sample-validation|sample_validation|direct-jdbc-validation|deploy-lakebase|neo4j-uc-federation-lab|worklog/'
   ```

   Expected: zero results. Hits in `.archive/` are acceptable (gitignored).

2. **Top-level layout matches CLAUDE.md.**

   ```bash
   ls -1 | grep -vE '^(\.|README|CLAUDE|TEST_CHANGES|LICENSE|mvnw)' 
   ```

   Expected directories only: `getting-started`, `examples`, `validation`,
   `driver-tests`, `docs`, `site`. (`.archive/` is gitignored.)

3. **`.gitignore` honors `.archive/`.**

   ```bash
   git check-ignore -v .archive/worklog-uc-next-steps.md
   ```

   Expected: matches `.archive/` rule.

4. **Notebook JSON parses.** All four getting-started notebooks must be valid
   JSON after the in-place sed edits.

   ```bash
   for nb in getting-started/*.ipynb; do
     python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$nb" \
       && echo "OK $nb" || echo "BAD $nb"
   done
   ```

   Expected: `OK` for every notebook.

5. **Python compiles.**

   ```bash
   cd validation
   uv sync --locked
   uv run python -m compileall cli scripts tools
   uv run python -m cli --help
   uv run python -c "from data_utils import csv_rows, get_neo4j_driver, get_config, inject_params, ValidationResults; print('imports OK')"
   ```

   Expected: no errors; `--help` shows the CLI; imports succeed.

6. **Shell wrappers parse.**

   ```bash
   cd validation
   for s in upload.sh submit.sh validate.sh validate_metadata.sh \
            create_secrets.sh grant_external_metadata.sh upload_data.sh; do
     bash -n "$s" && echo "OK $s"
   done
   ```

   Expected: `OK` for every script.

7. **Java driver tests compile.**

   ```bash
   cd driver-tests
   ./mvn compile test-compile -q
   ```

   Expected: clean compile, no errors.

8. **Java tests run locally** (no Databricks; uses Neo4j JDBC translator only).

   ```bash
   cd driver-tests
   ./mvn test
   ```

   Expected: the supported local quality gate passes. The advanced and
   potential-bug probe classes are excluded by default and can be run with
   `./mvn test -Pprobe-tests`.

## Phase 2: Documentation Sanity

Goal: confirm rewritten docs describe what is actually in the repo.

1. **Examples README references only existing notebooks.**

   ```bash
   ls examples/*.ipynb
   grep -oE '0[4-7]_[a-z_]+\.ipynb' examples/README.md | sort -u
   ```

   Expected: the two lists match.

2. **Validation README scripts table matches `scripts/` directory.**

   ```bash
   ls validation/scripts/run_*.py
   grep -oE 'run_[0-9]+_[a-z_]+\.py' validation/README.md | sort -u
   ```

   Expected: every script in the directory is in the README and vice versa.

3. **Site pages 06, 08, 09 build under Antora.** If the site is being
   published, run the existing Antora build (or whatever CI uses) and confirm
   no broken xrefs after the deploy-lakebase removal.

   ```bash
   # Whatever your existing site build command is, e.g.:
   # cd site && npx antora antora-playbook.yml
   ```

   Expected: clean build; no warnings about missing pages or anchors.

## Phase 3: Databricks-Dependent — Validation Suite

**Last run: 2026-05-08 — ✅ ALL PASSED**

Goal: end-to-end happy path on a real workspace. Run only after Phase 1 and 2
are clean.

Requires: Databricks profile, cluster ID, Neo4j Aura instance, connector JAR
already in a UC volume.

Workspace used: `azure-rk-knight` profile, cluster `0104-134656-pvz3m00y`,
Aura instance `0582a1b1.databases.neo4j.io`, connector
`neo4j-unity-catalog-connector-1.3.3-local.jar` in
`/Volumes/uc-w-neo4j/jdbc_drivers/jars/`, secret scope `validate_federation`.

1. **Configure environment.** ✅

   `validation/.env` already populated with the workspace settings above.
   `examples/.env` synced to the same Neo4j instance and JDBC jar.

   ```bash
   cd validation
   cp .env.sample .env
   # Edit .env — fill DATABRICKS_*, NEO4J_*, UC_*, LAKEHOUSE_*, METADATA_* values.
   ```

2. **Verify Neo4j credentials before touching Databricks.** ✅

   ```bash
   uv run --with neo4j python tools/check_neo4j_auth.py
   ```

   Result: connected, 2,206 nodes already loaded.

3. **Create the secret scope.** ✅

   ```bash
   ./create_secrets.sh
   ```

   Result: scope `validate_federation` updated with `NEO4J_USERNAME` and
   `NEO4J_PASSWORD`.

4. **Upload sample CSVs to the UC volume.** ⏭️ skipped

   Data already present in Neo4j from a prior run; CSV upload not required for
   `validate.sh` (only needed by `run_00_load_graph.py`, which is not part of
   the suite).

   ```bash
   ./upload_data.sh
   ```

   Expected: files land in `/Volumes/${UC_CATALOG}/${UC_SCHEMA}/${UC_VOLUME}/`.

5. **Load the aircraft graph into Neo4j.** ⏭️ skipped

   Same reason — graph already loaded.

   ```bash
   uv run python -m cli upload run_00_load_graph.py
   uv run python -m cli submit run_00_load_graph.py
   ```

   Expected: job succeeds; Neo4j contains 20 aircraft, 160 sensors, 800
   flights, 172800 readings.

6. **Smoke test the runner.** ✅

   ```bash
   ./upload.sh test_hello.py
   ./submit.sh test_hello.py
   ```

   Result: SUCCESS (run id `1091447504928692`).

7. **Run the full federation suite.** ✅

   ```bash
   ./validate.sh
   ```

   Result: `ALL PASSED`. All five scripts SUCCESS:
   - `run_01_connection_validation.py`
   - `run_02_federated_queries.py`
   - `run_03_metadata_sync_tables.py`
   - `run_04_metadata_sync_api.py`
   - `run_05_advanced_spark_queries.py`

8. **Run the metadata-only flow.** ✅

   ```bash
   ./validate_metadata.sh --skip-secrets --skip-upload
   ```

   Result: `ALL PASSED`. Grant step succeeded, both `run_03_metadata_sync_tables.py`
   and `run_04_metadata_sync_api.py` SUCCESS. No `PERMISSION_DENIED`.

9. **Serverless variant (optional but covers the `--compute serverless` path).** ⏭️ skipped

   ```bash
   ./validate.sh --compute serverless
   ```

   Expected: same outcome on serverless compute.

## Phase 4: Tutorial Notebooks

Goal: confirm the four `getting-started/` notebooks still execute against the
data loaded in Phase 3.

Run each notebook in order in the Databricks workspace, top to bottom, on the
same cluster used by Phase 3:

1. `01_load_graph.ipynb` (or equivalent first notebook) — should be idempotent
   against the graph already loaded in Phase 3.
2. `02_connect.ipynb` — UC connection creation.
3. `03_federate.ipynb` — federated SELECTs through `remote_query()`.
4. `04_materialize.ipynb` — materialize a Neo4j label as a Delta table.

Expected for each: every cell runs cleanly; no broken paths to the renamed
`validation/` folder or `validation` secret scope.

## Phase 5: Examples Notebooks

Goal: confirm the four remaining example notebooks still execute end to end.

Run each in the workspace, on the cluster meeting the prerequisites listed in
`examples/README.md`:

1. `04_metadata_sync_delta_tables.ipynb`
2. `05_metadata_sync_external_api.ipynb` — needs
   `CREATE_EXTERNAL_METADATA` already granted (Phase 3 step 8 covers this).
3. `06_new_federated_queries.ipynb`
4. `07_performance_diagnostics.ipynb`

Expected for each: completes without errors; results align with what the
README describes.

## Phase 6: Git History Spot Checks

Goal: confirm the cleanup preserved file history rather than rewriting it as
delete + add.

```bash
git log --follow --oneline validation/scripts/data_utils.py | head
git log --follow --oneline driver-tests/pom.xml | head
git log --follow --oneline validation/scripts/run_00_load_graph.py | head
```

Expected: each command shows commits from before the rename, proving `git mv`
preserved history across the folder rename and the `sample-validation/` →
`validation/` consolidation.

## Sign-off Checklist

- [x] Phase 1 — all eight local checks pass
- [x] Phase 2 — docs match repo state
- [x] Phase 3 — `./validate.sh` and `./validate_metadata.sh` succeed
- [ ] Phase 4 — all four tutorial notebooks run clean
- [ ] Phase 5 — all four example notebooks run clean
- [ ] Phase 6 — `git log --follow` shows pre-rename history for moved files
