# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Neo4j + Databricks Lakehouse Federation Integration — enables SQL queries against Neo4j graph databases from Databricks via Unity Catalog's JDBC connection support. The core component is the Neo4j JDBC Lakehouse Federation Connector, a shaded (fat) JAR that bundles the Neo4j JDBC driver with SQL-to-Cypher translators.

## Connector JAR

The Neo4j JDBC Lakehouse Federation Connector JAR is built and released from a separate repo: [neo4j-unity-catalog-connector](https://github.com/neo4j-labs/neo4j-unity-catalog-connector). Download the latest release from [releases](https://github.com/neo4j-labs/neo4j-unity-catalog-connector/tags).

## Architecture

### Translator Pipeline (SPI-based)

Translators are discovered via Java ServiceLoader (`META-INF/services/org.neo4j.jdbc.translator.spi.TranslatorFactory`). The pipeline chains translators by `Translator.getOrder()`:

1. **SparkSubqueryCleaningTranslator** (highest precedence) — strips Spark's `SPARK_GEN_SUBQ_0 WHERE 1=0` wrapping that Databricks adds to JDBC queries
2. **SqlToCypherTranslator** — converts cleaned SQL into Cypher

Each translator returns `null` for queries it doesn't handle, passing to the next in the chain.

### Shaded JAR Strategy

The Maven shade plugin merges `neo4j-jdbc`, `neo4j-jdbc-translator-impl`, and `neo4j-jdbc-translator-sparkcleaner` into a single JAR. All dependencies are relocated under `org.neo4j.jdbc.internal.shaded.*` to avoid classpath conflicts with Databricks SafeSpark's isolated JVM. The `ServicesResourceTransformer` merges SPI registration files across the bundled JARs — this is critical for translator discovery.

### SafeSpark Compatibility

Databricks runs custom JDBC drivers in an isolated JVM sandbox. The connector requires metaspace tuning:
`spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib 128`

### Project Layout

- `getting-started/` — Tutorial: 4 ordered Databricks notebooks (load graph → connect → federate → materialize)
- `examples/` — In-depth notebooks: metadata sync (Delta + External API), advanced federated queries, performance diagnostics
- `validation/` — Programmatic Python scripts run as Databricks jobs (data load, connection validation, federated queries, metadata sync, advanced Spark patterns)
- `driver-tests/` — Local Java/Maven tests for Neo4j JDBC SQL-to-Cypher translation (no Databricks required)
- `docs/` — Markdown reference documentation
- `site/` — Antora documentation site (AsciiDoc, published to GitHub Pages)
- `.archive/` — Internal scratch and superseded notes (gitignored)

The connector JAR itself lives in a separate repo:
[neo4j-unity-catalog-connector](https://github.com/neo4j-labs/neo4j-unity-catalog-connector).

## Testing

JUnit 5 tests in `driver-tests/src/test/java/` validate driver-level SQL-to-Cypher
translation locally. Run `./mvn test` from `driver-tests/` (the local launcher
downloads Maven into `driver-tests/.maven/` on first use; no system Maven
required). The connector JAR's own JUnit suite and Spotless formatting lives in
the separate
[neo4j-unity-catalog-connector](https://github.com/neo4j-labs/neo4j-unity-catalog-connector)
repo.

## Release Process

Tag with `connector-*` pattern triggers GitHub Actions to build and publish a release:
```bash
git tag connector-1.0.0
git push origin connector-1.0.0
```

## SQL-to-Cypher Support via Neo4j Federated JDBC UC Connection

Supported: `SELECT COUNT(*)`, aggregates with `WHERE`, `COUNT DISTINCT`, `NATURAL JOIN` (graph traversals), `GROUP BY` (implicit and explicit WITH-clause generation), `HAVING` (simple, compound, mixed aggregates, without GROUP BY), `ORDER BY` (including on aggregate aliases and after WITH clauses), `DISTINCT` with GROUP BY/HAVING, `LIMIT`/`OFFSET` with WITH clauses, `WHERE` + `GROUP BY` combinations, `JOIN` + `GROUP BY`, `COUNT(DISTINCT)` in HAVING, additional aggregate functions (`stDev`, `stDevP`), full clause combinations.

Not supported (use Spark Connector instead): relationship property aggregation, user-authored derived-table subqueries (`SELECT * FROM (...) alias`).

Note: Spark's `SPARK_GEN_SUBQ_N` probe wrappers are stripped automatically by the spark cleaner JAR before translation. This is internal plumbing — it is not the same as support for user-authored subqueries.
