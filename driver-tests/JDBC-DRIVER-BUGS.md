# Neo4j JDBC Driver SQL Translation Bugs

This document lists SQL translation issues reproduced by sending SQL directly
through the Neo4j JDBC driver with SQL translation enabled.

## Validation Context

- Driver artifact: `org.neo4j:neo4j-jdbc-full-bundle`
- Driver versions checked: `6.10.0`, `6.10.5`, and local
  `6.12.3-SNAPSHOT` from `/Users/ryanknight/projects/neo4j-jdbc`
- JDBC URL mode: `enableSQLTranslation=true`
- Dataset: a Neo4j graph with labels such as `Aircraft`, `MaintenanceEvent`,
  `Flight`, `Airport`, and `Delay`
- Reproduction command:

```bash
cd driver-tests
./mvn test -Dtest=DirectJdbcAdvancedSqlTest,DirectJdbcKnownLimitationsTest
```

Local driver snapshot command:

```bash
cd driver-tests
./mvn test -Dneo4j.jdbc.version=6.12.3-SNAPSHOT
```

Baseline result with `6.10.0`:

```text
DirectJdbcAdvancedSqlTest: Tests run: 26, Failures: 2, Errors: 8
DirectJdbcKnownLimitationsTest: Tests run: 4, Failures: 0, Errors: 0
```

`6.10.5` showed the same failing behavior for the advanced SQL cases.

Result with local `6.12.3-SNAPSHOT` for the bug-focused suites:

```text
DirectJdbcAdvancedSqlTest: Tests run: 26, Failures: 0, Errors: 2
DirectJdbcKnownLimitationsTest: Tests run: 4, Failures: 0, Errors: 0
```

## Direct JDBC SQL Translation Bugs

### 1. Non-projected GROUP BY collapses all groups into one row

The driver returns one aggregate row when the grouping key is not projected.
SQL semantics require one row per group even when the grouping expression is not
part of the select list.

Repro:

```sql
SELECT COUNT(*) AS cnt
FROM MaintenanceEvent
GROUP BY severity
```

Expected: at least 3 rows, one per severity.

Actual with `6.10.0` and `6.10.5`: 1 row.

The same failure reproduces through a relationship traversal:

```sql
SELECT COUNT(*) AS flight_count
FROM Flight f
NATURAL JOIN DEPARTS_FROM r
NATURAL JOIN Airport a
GROUP BY a.iata
```

Expected: multiple airport groups.

Actual with `6.10.0` and `6.10.5`: 1 row.

Control query that works:

```sql
SELECT severity, COUNT(*) AS cnt
FROM MaintenanceEvent
GROUP BY severity
```

Status with local `6.12.3-SNAPSHOT`: fixed. Both
`groupByNonProjectedKey` and
`joinGroupByNonProjectedReturnsMultipleAirportGroups` now pass.

### 2. Aggregate ORDER BY uses invalid property paths for aggregate aliases

When ordering by an aggregate alias, the translator qualifies the alias with the
node variable. Neo4j then rejects the generated Cypher because variables from
before an aggregating `RETURN` cannot be accessed that way.

Repro:

```sql
SELECT severity, COUNT(*) AS cnt
FROM MaintenanceEvent
GROUP BY severity
ORDER BY cnt DESC
```

Generated Cypher excerpt from `6.10.0`:

```cypher
MATCH (maintenanceevent:MaintenanceEvent)
RETURN maintenanceevent.severity AS severity, count(*) AS cnt
ORDER BY maintenanceevent.cnt DESC
```

Neo4j error:

```text
It is not possible to access variables declared before the WITH/RETURN:
maintenanceevent
```

Other repros with the same root cause:

```sql
SELECT operator, COUNT(*) AS cnt, COUNT(DISTINCT origin) AS routes
FROM Flight
GROUP BY operator
ORDER BY cnt DESC, routes
```

```sql
SELECT cause, COUNT(*) AS cnt
FROM Delay
WHERE minutes > 30
GROUP BY cause
ORDER BY cnt DESC
```

Status with local `6.12.3-SNAPSHOT`: fixed for the direct aggregate forms.
`orderByAggregateAlias`, `orderByMultiKey`, and `whereGroupByComparison` now
pass.

### 3. Aggregate ORDER BY plus LIMIT/OFFSET fails before returning rows

The same alias-qualification bug blocks aggregate queries that combine
`ORDER BY` with `LIMIT` and `OFFSET`.

Repro:

```sql
SELECT operator, COUNT(*) AS cnt
FROM Flight
GROUP BY operator
ORDER BY cnt DESC
LIMIT 3 OFFSET 1
```

Generated Cypher excerpt from `6.10.0`:

```cypher
MATCH (flight:Flight)
RETURN flight.operator AS operator, count(*) AS cnt
ORDER BY flight.cnt DESC
LIMIT 3
```

Neo4j error:

```text
It is not possible to access the variable `flight` declared before the RETURN
clause when using DISTINCT or an aggregation.
```

This may be the same underlying defect as Bug 2, but it is listed separately
because `LIMIT/OFFSET` is a distinct user-visible SQL pattern.

Status with local `6.12.3-SNAPSHOT`: fixed for direct aggregate queries.
`limitOffset`, `allClausesCombined`, and `havingOrderByLimitOffsetIsolated` now
pass.

### 4. Derived-table aggregate subqueries leak the outer table alias

Arbitrary derived tables with aggregates are translated using the outer derived
table alias as if it were a graph variable.

Repro:

```sql
SELECT *
FROM (
    SELECT severity, COUNT(*) AS cnt
    FROM MaintenanceEvent
    GROUP BY severity
) t
```

Observed generated Cypher shape:

```cypher
MATCH (maintenanceevent:MaintenanceEvent)
RETURN t.severity, count(*) AS cnt
```

Neo4j rejects this because `t` is a SQL derived-table alias, not a Cypher
variable.

Status with local `6.12.3-SNAPSHOT`: not fixed or not intentionally supported.
`arbitraryDerivedTableWithAggregateFailsDirectly` still expects this query to
fail.

Expected behavior should be one of:

- translate the derived table correctly, or
- reject the unsupported SQL shape during translation with a clear driver error.

### 5. Single-argument percentile functions translate to invalid Cypher

The SQL translator accepts single-argument percentile functions and emits Cypher
that Neo4j cannot execute.

Repro:

```sql
SELECT cause, percentileCont(minutes) AS p50
FROM Delay
GROUP BY cause
```

```sql
SELECT cause, percentileDisc(minutes) AS p50
FROM Delay
GROUP BY cause
```

Root issue: Cypher percentile functions require two arguments:
`percentileCont(expression, percentile)` or
`percentileDisc(expression, percentile)`. The single-argument SQL form has no
valid Cypher equivalent unless the translator supplies or requires a percentile
argument.

Status with local `6.12.3-SNAPSHOT`: still unsupported. The known-limitation
tests still expect both single-argument percentile forms to fail.

Expected behavior should be one of:

- support standard SQL ordered-set syntax such as
  `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY minutes)`, or
- reject the unsupported single-argument form during SQL translation with a
  clear driver error.

## Additional Direct SQL Tests To Add

Add these to the JDBC driver project:

- Regression tests for non-projected `GROUP BY`.
- Regression tests for join `GROUP BY` with a non-projected key.
- Regression tests for aggregate alias `ORDER BY`.
- Regression tests for multi-key aggregate `ORDER BY`.
- Regression tests for `HAVING` plus `ORDER BY/LIMIT/OFFSET`.
- Derived-table aggregate tests that pin the intended behavior: either support
  ordinary aggregate derived tables, or reject them with a clear unsupported SQL
  error rather than leaking the SQL alias into Cypher.
- Percentile tests for the intended contract: keep clear failures for
  single-argument percentile functions, and add support tests if ordered-set SQL
  syntax such as `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY minutes)` is in
  scope.

## Spark JDBC Schema Probe Reproductions

This section is separate because these are not ordinary user-authored SQL
queries. They are SQL strings shaped like the metadata probes Spark commonly
sends through JDBC before executing a real read.

The validation still sends these strings directly through the Neo4j JDBC
driver. Spark is not running in this validation path. The purpose is to verify
whether the driver can translate the SQL text that Spark sends for schema
discovery.

A Spark-style schema probe usually wraps the user's query in a derived table,
assigns an alias such as `SPARK_GEN_SUBQ_0`, and adds a predicate that returns
zero rows:

```sql
SELECT *
FROM (
    SELECT severity, COUNT(*) AS cnt
    FROM MaintenanceEvent
    GROUP BY severity
) SPARK_GEN_SUBQ_0
WHERE 1=0
```

Spark uses this pattern to ask the JDBC source what columns and types the query
would return without fetching data.

### Probe wrappers can leak `SPARK_GEN_SUBQ_0` into Cypher

The driver still sees and translates some schema-probe wrappers instead of
stripping the wrapper and translating the inner query.

Repro:

```sql
SELECT *
FROM (
    SELECT severity, COUNT(*) AS cnt
    FROM MaintenanceEvent
    GROUP BY severity
) SPARK_GEN_SUBQ_0
WHERE 1=0
```

Generated Cypher excerpt:

```cypher
MATCH (maintenanceevent:MaintenanceEvent)
RETURN spark_gen_subq_0.severity, count(*) AS cnt LIMIT 1
```

Neo4j error:

```text
Variable `spark_gen_subq_0` not defined.
```

Additional confirmed failing wrapper shapes:

```sql
SELECT *
FROM (
    SELECT operator, COUNT(*) AS cnt
    FROM Flight
    GROUP BY operator
    HAVING COUNT(*) > 20
) SPARK_GEN_SUBQ_0
WHERE 1=0
```

```sql
SELECT *
FROM (
    SELECT DISTINCT operator, COUNT(*) AS cnt
    FROM Flight
    GROUP BY operator
) SPARK_GEN_SUBQ_0
WHERE 1=0
```

```sql
SELECT *
FROM (
    SELECT aircraftId, severity, COUNT(*) AS cnt
    FROM MaintenanceEvent
    GROUP BY aircraftId, severity
) SPARK_GEN_SUBQ_0
WHERE 1=0
```

```sql
SELECT *
FROM (
    SELECT aircraftId, severity, fault
    FROM MaintenanceEvent
) SPARK_GEN_SUBQ_0
WHERE 1=0
```

These variants produce Cypher with references such as
`spark_gen_subq_0.operator`, `spark_gen_subq_0.aircraftId`, or
`spark_gen_subq_0.fault`, followed by `Variable spark_gen_subq_0 not defined`.

Status with local `6.12.3-SNAPSHOT`: still open. The seven remaining execution
errors are all schema-probe wrapper shapes:

- `sparkProbeWrapperForGroupByProjectedKey`
- `sparkProbeWrapperForHavingOrderLimitOffset`
- `sparkProbeWrapperForHavingOnly`
- `sparkProbeWrapperForDistinctGroupBy`
- `sparkProbeWrapperForLimitOffset`
- `sparkProbeWrapperForMultiKeyGroupBy`
- `sparkProbeWrapperForNonAggregateSelect`

Additional tests to add:

- Probe-wrapper tests for projected aggregate `GROUP BY`.
- Probe-wrapper tests for `HAVING` only.
- Probe-wrapper tests for aggregate `ORDER BY/LIMIT/OFFSET`.
- Probe-wrapper tests for `DISTINCT` plus `GROUP BY`.
- Probe-wrapper tests for multi-key `GROUP BY`.
- Probe-wrapper tests for non-aggregate select lists.
- Alias normalization tests using `SPARK_GEN_SUBQ_0`, `spark_gen_subq_0`,
  quoted aliases, and aliases with and without `AS`.
- Boundary tests that prove schema-probe handling only strips zero-row metadata
  probes such as `WHERE 1=0`, and does not rewrite normal runtime derived-table
  queries.
