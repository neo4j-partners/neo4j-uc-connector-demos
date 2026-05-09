# Project Status & Next Steps

_Last updated: 2026-05-08 after splitting direct JDBC and Spark validation._

---

## Script Layout

### validate-federation/scripts (Spark, Unity Catalog, and metadata)

| Script | Purpose |
|--------|---------|
| run_01_connection_validation.py | All 4 connection entry points (Python driver, Spark Connector, direct JDBC, UC JDBC) |
| run_02_federated_queries.py | Spark Connector + remote_query + Delta joins |
| run_03_metadata_sync_tables.py | Auto-discover Neo4j schema → materialize all labels/rels as Delta |
| run_04_metadata_sync_api.py | Register Neo4j schema in UC External Metadata API |
| run_05_advanced_spark_queries.py | Advanced remote_query SQL, schema inference path, and remote_query + Delta joins |

**run_05 sections:** GROUP BY (projected, non-projected, multi-aggregate) · HAVING (simple, non-projected, compound) · ORDER BY (alias, multi-key) · DISTINCT+GROUP BY · LIMIT+OFFSET · WHERE+GROUP BY+HAVING+ORDER BY+LIMIT/OFFSET · Federated GROUP BY+Delta · Federated HAVING+Delta · JOIN+GROUP BY · LIKE literal behavior.

### direct-jdbc-validation/src/test/java/com/neo4j/uc/directjdbc/

| File | Purpose |
|------|---------|
| DirectJdbcSqlToCypherTest.java | Baseline driver SQL-to-Cypher coverage mirrored from validate-federation run_01/run_02 |
| DirectJdbcAdvancedSqlTest.java | Driver-level advanced SQL regression probes |
| DirectJdbcPotentialBugTest.java | Additional direct JDBC probes, including schema probe wrapper variants |
| DirectJdbcKnownLimitationsTest.java | Expected failures for unsupported or malformed SQL forms |

---

## Validation Run Results

### 2026-04-09 — Full sweep on fresh DB

| Script | Result | Notes |
|--------|--------|-------|
| run_00 (graph load) | ✅ passed | 17 CSVs uploaded, all nodes/relationships loaded |
| run_01 (UC JDBC connection) | ✅ passed | Connection created, 172,800 sensor readings loaded |
| run_02 (federated queries) | ✅ passed | |
| run_03 (remote_query) | ✅ passed | |
| run_04 (materialized tables) | ✅ passed | |
| run_05 (advanced SQL) | ⚠️ 21/23 | §1–11 and §13 green; §12 percentileCont/percentileDisc fail |

### 2026-04-10 — run_05 fix + run_06 first attempt

| Script | Result | Notes |
|--------|--------|-------|
| run_05 (advanced SQL) | ✅ 21/21 | percentileCont/percentileDisc removed from §12; tracked in `direct-jdbc-validation/JDBC-DRIVER-BUGS.md` |
| run_06 (advanced federation) | ✅ 1/1 | §1 passed (20 aircraft, 24s); §2 and §3 removed — see non-driver issues below |

---

## Open Non-Driver Issues

Real Neo4j JDBC driver bugs have been moved to
[`direct-jdbc-validation/JDBC-DRIVER-BUGS.md`](../direct-jdbc-validation/JDBC-DRIVER-BUGS.md).
That document excludes validation mistakes such as grouping airports by
`a.code`; the aircraft digital twin dataset uses `Airport.iata` and
`Airport.icao`.

### Spark AQE mis-plans remote_query CTE + Delta WHERE join

**Symptom:** `SELECT ... FROM neo4j_aircraft a JOIN flight_activity f ... JOIN engine_health e ...` where `engine_health` has `WHERE sys.type = 'Engine'` returns 1 row instead of 20.

**Root cause:** Spark AQE produces a bad plan when a remote_query CTE is joined with a Delta CTE that has a WHERE clause on a joined column. Without the WHERE clause, the identical join returns 20 rows (confirmed in run_06 §1). Confirmed via SQL warehouse: `engine_health` subquery alone returns all 20 aircraft correctly — the plan issue only manifests in the Spark cluster execution path.

**Data note:** All sensors in this dataset belong to Engine systems, so the `WHERE sys.type = 'Engine'` filter is also redundant — all sensor types (EGT, FuelFlow, N1Speed, Vibration) are on Engine systems only.

**Resolution:** Removed §2 from run_06. To re-add: drop the `WHERE sys.type = 'Engine'` clause and rely on CASE WHEN sensor type filters, or file a Spark bug.

### run_06 §3 operator_maint fan-out

**Symptom:** Section 3 (Operator-Level Fleet Health Dashboard) produced inflated maintenance totals. The `operator_maint` CTE joins `neo4j_maintenance_events → neo4j_aircraft → neo4j_flights`, causing each maintenance event to be counted once per flight per aircraft (e.g., 15 events × 47 flights = 705 counted rows).

**Resolution:** Removed §3 from run_06. To re-add, aggregate maintenance per aircraft before joining to flights. Any remaining JDBC syntax error should be reproduced directly and added to `direct-jdbc-validation/JDBC-DRIVER-BUGS.md` only after it is isolated from the fan-out bug.

### Non-aggregate SELECT fails for snake_case property names in Databricks

**Symptom:** `SELECT aircraft_id, severity, fault FROM MaintenanceEvent` fails on Databricks with `JDBC_EXTERNAL_ENGINE_SYNTAX_ERROR.DURING_QUERY_EXECUTION`. Same query passes locally. Airport queries (single-word properties) pass — possibly because small row count triggers a different execution path.

**Hypothesis:** `remote_query()` wraps non-aggregate SELECT in a format that doesn't match either known probe pattern (SPARK_GEN_SUBQ with WHERE 1=0 or SELECT 1).

**To investigate:** Enable JDBC debug logging on the Databricks cluster, or add a `translate()` input logger to `SparkSubqueryCleaningTranslator`, to capture the raw SQL sent for a non-aggregate SELECT on a large label.

---

## Cleanup

- [x] ~~Delete `validate-federation/scripts/run_06_advanced_sql.py`~~ — done
- [x] ~~Delete `validate-federation/scripts/run_03_materialized_tables.py`~~ — done (95% duplicate of sample-validation/run_04)
- [x] ~~Renumber: run_04→run_03, run_05→run_04 in validate-federation~~ — done
- [x] ~~**Remove** `percentileCont` and `percentileDisc` test cases from Spark validation §12~~ — done
- [x] ~~**Update CLAUDE.md** — clarify "subqueries with aggregates" only applies to SPARK_GEN_SUBQ probe wrapper path, not arbitrary derived tables~~ — done

---

## Upstream & Release

- [ ] Upstream the `SparkSubqueryCleaningTranslator` fix to the neo4j-jdbc repo as a PR
  - Driver issues are tracked in `direct-jdbc-validation/JDBC-DRIVER-BUGS.md`
  - Include translator unit tests plus the direct JDBC regression cases from this repository
- [ ] Update getting-started notebooks to reference the released connector version (currently pinned to `1.3.3-local`)
- [ ] Verify run_04 idempotency: run twice back-to-back on the same cluster without dropping tables — both runs must pass

---

## Regression Coverage

- [ ] Add an end-to-end test for the raw Cypher passthrough path in `SparkSubqueryCleaningTranslator`: a user passes raw Cypher as the Spark JDBC `query` option, Spark wraps it, and the translator re-wraps it as `/*+ NEO4J FORCE_CYPHER */ CALL {...} RETURN * LIMIT 1`. Unit tests cover it but no Databricks run exercises it.

---

## Known Limitations (not bugs — by design)

| Pattern | Reason |
|---------|--------|
| WHERE LIKE | Databricks may strip quotes from LIKE literals in remote_query() before passing to JDBC. `run_05_advanced_spark_queries.py` records this as a known Spark-side behavior. |
| Non-aggregate SELECT (large labels) | Fails on Databricks only; direct JDBC local forms pass. Airport (small) works as a workaround. |
| Subqueries with aggregates (derived tables) | Driver bug tracked in `direct-jdbc-validation/JDBC-DRIVER-BUGS.md`. SPARK_GEN_SUBQ path only works for some wrapper shapes. |
| percentileCont / percentileDisc | Driver bug tracked in `direct-jdbc-validation/JDBC-DRIVER-BUGS.md`; single-arg SQL form has no valid Cypher mapping. |
