# driver-tests

Local Java tests for Neo4j JDBC SQL-to-Cypher translation.

This project validates the Neo4j JDBC and SQL-to-Cypher behavior that
`validation/` currently exercises from Databricks. It intentionally
does not test Spark, Unity Catalog connection creation, `remote_query()` as a
Databricks SQL function, Spark Connector reads, or Delta lakehouse joins.

## Prerequisites

- Java 17
- Python 3.10+ for the optional local graph loader
- A Neo4j database populated with the aircraft digital twin graph
- The sample data loaded first through the local loader below

From the repository root, load the sample graph before running these tests. The
loader reads from `getting-started/data/aircraft_digital_twin_data` and writes
directly with the Neo4j Python driver. It does not require Databricks, Spark,
or UC Volumes.

```bash
cd driver-tests
cp .env.sample .env
# Fill in NEO4J_URI or NEO4J_HOST, NEO4J_USERNAME, NEO4J_PASSWORD,
# and optionally NEO4J_DATABASE.

uv run --with neo4j python scripts/load_sample_graph.py
```

The loader follows Neo4j naming conventions for the graph schema it controls:
node labels use `PascalCase`, relationship types use `UPPER_SNAKE_CASE`,
identifier properties use lower camel case such as `aircraftId`, and generated
constraint names use `snake_case`. Existing CSV-derived properties such as
`tail_number`, `reported_at`, and `corrective_action` are kept stable because
the JDBC validation queries and materialization examples already depend on
those names.

## Configuration

Copy the sample environment file:

```bash
cd driver-tests
cp .env.sample .env
```

Set either `NEO4J_URI` or `NEO4J_HOST`, plus `NEO4J_USERNAME`,
`NEO4J_PASSWORD`, and optionally `NEO4J_DATABASE`.

Environment variables take precedence over values in `.env`.

Most tests build a JDBC URL with SQL translation enabled:

```text
jdbc:neo4j+s://<host>:7687/<database>?enableSQLTranslation=true
```

The two Cypher connectivity checks from `run_01_connection_validation.py` use
the same JDBC URL without `enableSQLTranslation=true`.

## Run

```bash
./mvn test
```

This is the clean local quality gate for currently supported and documented
driver behavior. The advanced and potential-bug classes are intentional
regression probes for upstream driver gaps and are excluded from the default
test run.

Run only the baseline validation coverage:

```bash
./mvn test -Dtest=DirectJdbcSqlToCypherTest
```

Run only the advanced driver-level regression probes:

```bash
./mvn test -Dtest=DirectJdbcAdvancedSqlTest
```

Run only the additional potential bug probes:

```bash
./mvn test -Dtest=DirectJdbcPotentialBugTest
```

Run all tests including the expected-failing probe classes:

```bash
./mvn test -Pprobe-tests
```

Run only the currently documented unsupported forms:

```bash
./mvn test -Dtest=DirectJdbcKnownLimitationsTest
```

To test a different Neo4j JDBC version:

```bash
./mvn test -Dneo4j.jdbc.version=<version>
```

The local `./mvn` launcher downloads Apache Maven into `.maven/` on first use
and uses `.maven/repository/` as the Maven dependency cache, so Maven does not
need to be installed globally and dependencies do not write to `~/.m2`.

## Coverage

The suite mirrors the runnable local Neo4j portions of
`validation/scripts/run_01_connection_validation.py` and
`run_02_federated_queries.py` as independent JUnit tests:

- Cypher connectivity checks from the Python driver and Spark Connector sections
- `SELECT 1 AS value`
- Read the `Aircraft` label as a SQL table
- `COUNT(*)` over `Flight`
- `NATURAL JOIN` traversal from `Flight` to `Airport`
- `WHERE` predicate over aircraft manufacturer
- Multiple aggregates over aircraft identifiers
- `COUNT(DISTINCT manufacturer)`
- Counts for `Aircraft`, `MaintenanceEvent`, and `Flight`
- Neo4j query components used by the fleet summary and fleet dashboard sections
- Direct JDBC equivalents for the `MaintenanceEvent` and `Flight` label
  aggregates used after Spark Connector loads

The expected exact counts come from the aircraft digital twin dataset loaded by
`scripts/load_sample_graph.py`.

`DirectJdbcAdvancedSqlTest` exercises the direct JDBC form of advanced SQL
patterns that are also covered end to end by
`validation/scripts/run_05_advanced_spark_queries.py`:

- `GROUP BY` with projected and non-projected keys
- multiple aggregates, `COUNT(DISTINCT ...)`, and `DISTINCT + GROUP BY`
- `HAVING`, including non-projected and compound aggregate predicates
- aggregate `ORDER BY`, `LIMIT`, `OFFSET`, and combined post-aggregate clauses
- `LIKE` string literals called directly through JDBC
- Neo4j-side portions of the advanced federated maintenance queries
- `JOIN + GROUP BY` over `Flight -> DEPARTS_FROM -> Airport`
- Spark schema inference wrappers shaped like
  `SELECT * FROM (<query>) SPARK_GEN_SUBQ_0 WHERE 1=0`

`DirectJdbcPotentialBugTest` adds independent probes for query patterns that are
not yet part of the confirmed driver bug report:

- snake_case property projection, aggregation, and predicates
- projected multi-hop `NATURAL JOIN` topology reads
- multi-column `GROUP BY`, both label-only and traversal-based
- non-aggregate `WHERE + ORDER BY + LIMIT`
- isolated `HAVING + ORDER BY + LIMIT/OFFSET`
- numeric `AVG`, `SUM`, `MIN`, and `MAX`
- `CASE` expressions inside aggregates
- additional Spark schema inference wrapper variants

`DirectJdbcKnownLimitationsTest` asserts failures for query forms that are
documented as unsupported or malformed:

- single-argument `percentileCont(...)`
- single-argument `percentileDisc(...)`
- arbitrary derived-table aggregate subqueries
- unquoted `LIKE` literals

With the default `neo4j-jdbc-full-bundle` version in this project, the baseline
and known-limitations classes pass, while the advanced class currently exposes
direct JDBC SQL-to-Cypher failures. That is intentional: those tests encode the
desired behavior so a future driver upgrade can be validated by rerunning the
same class.
