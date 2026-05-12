# Guide: Neo4j JDBC Lakehouse Federation Connector

This guide explains how to connect to Neo4j from Databricks using a Unity Catalog JDBC connection, including required configuration, supported query patterns, and best practices.

---

## Overview

This integration was validated by working with Databricks engineering to resolve a SafeSpark compatibility issue. The root cause was **metaspace memory exhaustion** in the SafeSpark sandbox during Neo4j JDBC driver initialization. With the correct Spark configuration, Unity Catalog JDBC connections to Neo4j work correctly.

### Component Test Results

| Component | Status | Notes |
|-----------|--------|-------|
| Network Connectivity | **PASS** | TCP to Neo4j port 7687 |
| Neo4j Python Driver | **PASS** | Bolt protocol works |
| Neo4j Spark Connector | **PASS** | `org.neo4j.spark.DataSource` works |
| Neo4j JDBC SQL-to-Cypher | **PASS** | Aggregates, GROUP BY, HAVING, ORDER BY, JOINs, dbtable all work |
| Direct JDBC (Non-UC) | **PASS** | Works with `customSchema` workaround |
| **Unity Catalog JDBC** | **PASS** | Works with SafeSpark memory configuration |
| **UC Schema Discovery** | **PASS** | Works with SafeSpark memory configuration |

---

## Prerequisites

### 1. Databricks Preview Features

Enable these preview features in your Databricks workspace:

| Feature | Required For |
|---------|--------------|
| Custom JDBC on UC Compute | Loading custom JDBC drivers in UC connections |
| remote_query table-valued function | Using `remote_query()` SQL function |

### 2. Neo4j JDBC Lakehouse Federation Connector JAR

Download the latest release from [neo4j-unity-catalog-connector releases](https://github.com/neo4j-labs/neo4j-unity-catalog-connector/tags) and upload it to a Unity Catalog Volume:

| JAR | Purpose |
|-----|---------|
| `neo4j-unity-catalog-connector-<version>.jar` | Neo4j JDBC Lakehouse Federation Connector — bundles the JDBC driver, SQL-to-Cypher translator, and Spark subquery cleaner |

See the [neo4j-unity-catalog-connector](https://github.com/neo4j-labs/neo4j-unity-catalog-connector) repo for details on what the JAR contains and how it is built.

**Important:** The `java_dependencies` option in `CREATE CONNECTION TYPE JDBC` only supports Unity Catalog Volume paths (e.g., `/Volumes/catalog/schema/jars/`). Cluster-installed libraries (Maven coordinates, uploaded JARs) cannot be referenced here — they are a separate system. JARs must be uploaded to a UC Volume and referenced by their Volume path.

Example path: `/Volumes/catalog/schema/jars/`

### 3. Cluster Libraries

For comprehensive testing, install these libraries on your cluster:

| Library | Version | Purpose |
|---------|---------|---------|
| org.neo4j:neo4j-connector-apache-spark | 5.3.10 (Spark 3) | Neo4j Spark Connector |
| neo4j (Python) | 6.0.2 | Neo4j Python Driver |
| neo4j-jdbc-full-bundle | 6.10.5 | JDBC driver (cluster library for Direct JDBC) |

For UC JDBC connections, cluster libraries are **not used**. The `java_dependencies` option only accepts UC Volume paths — cluster-installed libraries (Maven coordinates or uploaded JARs) cannot be referenced in `CREATE CONNECTION`. The JDBC JARs must be stored in a UC Volume.

---

## Required Spark Configuration

**Critical:** Add these settings to your Databricks cluster to prevent SafeSpark sandbox memory exhaustion:

```
spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib 128
spark.databricks.safespark.jdbcSandbox.jvm.xmx.mib 300
spark.databricks.safespark.jdbcSandbox.size.default.mib 512
```

Without these settings, you'll see: `Connection was closed before the operation completed`

**Why this is needed:** The Neo4j JDBC driver requires more memory for class loading than the default SafeSpark sandbox allocation. When metaspace is exhausted, the sandbox JVM crashes.

---

## Creating the Unity Catalog Connection

```sql
CREATE CONNECTION neo4j_connection TYPE JDBC
ENVIRONMENT (
  java_dependencies '[
    "/Volumes/catalog/schema/jars/neo4j-unity-catalog-connector-<version>.jar"
  ]'
)
OPTIONS (
  url 'jdbc:neo4j+s://your-host:7687/neo4j?enableSQLTranslation=true',
  user secret('scope', 'neo4j-user'),
  password secret('scope', 'neo4j-password'),
  driver 'org.neo4j.jdbc.Neo4jDriver',
  externalOptionsAllowList 'dbtable,query,customSchema'
)
```

**Key options:**
- `enableSQLTranslation=true` - Enables automatic SQL-to-Cypher translation
- `externalOptionsAllowList` - Allows passing `customSchema` at query time

---

## Querying Neo4j through UC

### Method 1: Spark DataFrame API

```python
df = spark.read.format("jdbc") \
    .option("databricks.connection", "neo4j_connection") \
    .option("query", "SELECT COUNT(*) AS cnt FROM Flight") \
    .option("customSchema", "cnt LONG") \
    .load()

df.show()
```

### Method 2: remote_query() Function

```sql
SELECT * FROM remote_query(
    'neo4j_connection',
    query => 'SELECT COUNT(*) AS cnt FROM Flight'
)
```

**Note:** Always use `customSchema` with the DataFrame API. Neo4j JDBC returns `NullType()` during Spark's schema inference, causing errors.

---

## Validated Test Results

The following results are from testing against a Neo4j Aura instance:

| Test | Status | Result |
|------|--------|--------|
| Basic Query (`SELECT 1`) | **PASS** | 1 |
| COUNT Aggregate | **PASS** | 2,400 flights |
| Multiple Aggregates (COUNT, MIN, MAX) | **PASS** | 60 total, AC1001-AC1020 |
| COUNT DISTINCT | **PASS** | 3 unique manufacturers |
| Aggregate with WHERE (equals) | **PASS** | 15 Boeing aircraft |
| Aggregate with WHERE (IN clause) | **PASS** | 45 Boeing+Airbus aircraft |
| Aggregate with WHERE (AND) | **PASS** | 15 Boeing with model |
| Aggregate with WHERE (IS NOT NULL) | **PASS** | 60 aircraft with icao24 |
| JOIN with Aggregate (2-hop) | **PASS** | 11,200 relationships |
| GROUP BY | **PASS** | Implicit and explicit WITH-clause grouping |
| ORDER BY | **PASS** | Including aggregate alias resolution |
| HAVING | **PASS** | Simple, compound, and mixed aggregates |
| LIMIT / OFFSET | **PASS** | Correct attachment to final RETURN |
| DISTINCT + GROUP BY | **PASS** | Correct RETURN DISTINCT placement |

**Success Rate: 100%** (15/15 supported patterns passed)

---

## Supported Query Patterns

These SQL patterns have been validated to work through Unity Catalog JDBC:

### Simple Expressions
```sql
SELECT 1 AS test
-- Result: 1
```

### Aggregates (COUNT, MIN, MAX, SUM, AVG)
```sql
SELECT COUNT(*) AS total FROM Flight
-- Result: 2,400
```

### Multiple Aggregates
```sql
SELECT COUNT(*) AS total,
       MIN(aircraftId) AS first_id,
       MAX(aircraftId) AS last_id
FROM Aircraft
-- Result: {total: 60, first_id: 'AC1001', last_id: 'AC1020'}
```

### COUNT DISTINCT
```sql
SELECT COUNT(DISTINCT manufacturer) AS unique_manufacturers FROM Aircraft
-- Result: 3
```

### Aggregates with WHERE (Multiple Patterns)
```sql
-- Equals
SELECT COUNT(*) AS boeing_count
FROM Aircraft
WHERE manufacturer = 'Boeing'
-- Result: 15

-- IN clause
SELECT COUNT(*) AS cnt
FROM Aircraft
WHERE manufacturer IN ('Boeing', 'Airbus')
-- Result: 45

-- AND with IS NOT NULL
SELECT COUNT(*) AS cnt
FROM Aircraft
WHERE manufacturer = 'Boeing' AND model IS NOT NULL
-- Result: 15

-- IS NOT NULL
SELECT COUNT(*) AS cnt
FROM Aircraft
WHERE icao24 IS NOT NULL
-- Result: 60
```

### Aggregates with JOIN (Graph Traversal)
```sql
SELECT COUNT(*) AS relationship_count
FROM Flight f
NATURAL JOIN DEPARTS_FROM r
NATURAL JOIN Airport a
-- Result: 11,200
```

This translates to Cypher: `MATCH (f:Flight)-[:DEPARTS_FROM]->(a:Airport) RETURN count(*)`

---

## New SQL Functionality Supported

The SQL-to-Cypher translator now supports GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, DISTINCT, and their full combinations. These patterns previously failed through UC JDBC but are now fully translated to Cypher:

- **GROUP BY** — implicit grouping (columns match SELECT) and explicit WITH-clause generation (columns differ from SELECT)
- **HAVING** — simple conditions, compound conditions (AND/OR), mixed SELECT/HAVING aggregates, HAVING without GROUP BY (implicit single-group), HAVING on non-aggregate GROUP BY columns
- **ORDER BY on aggregate aliases** — `ORDER BY cnt` where `cnt` aliases `count(*)`, with correct alias resolution after WITH clauses
- **DISTINCT with GROUP BY/HAVING** — correct `RETURN DISTINCT` placement
- **LIMIT and OFFSET with WITH clauses** — correct attachment to the final RETURN
- **WHERE + GROUP BY combinations** — WHERE filters before aggregation, HAVING filters after
- **JOIN + GROUP BY** — aggregation across relationships, with and without WITH clauses
- **COUNT(DISTINCT) in HAVING** — the DISTINCT flag is preserved through the entire pipeline
- **Compound HAVING with multiple aggregates** — each HAVING-only aggregate gets a hidden WITH column; aggregates already in SELECT are deduplicated
- **Additional aggregate functions** — `percentileCont`, `percentileDisc`, `stDev` (stddev_samp), `stDevP` (stddev_pop)
- **Full clause combinations** — all of the above working together: WHERE + GROUP BY + HAVING + DISTINCT + ORDER BY + LIMIT + OFFSET

> **Note:** All aggregation support applies to node properties only; aggregating over relationship properties remains Cypher-only.

### GROUP BY Examples

```sql
-- Implicit grouping (GROUP BY columns match SELECT)
SELECT name, count(*) FROM People p GROUP BY name
-- Cypher: MATCH (p:People) RETURN p.name AS name, count(*)

-- GROUP BY column not in SELECT (WITH clause generated)
SELECT sum(age) FROM People p GROUP BY name
-- Cypher: MATCH (p:People) WITH sum(p.age) AS __with_col_0, p.name AS __group_col_1 RETURN __with_col_0
```

### HAVING Examples

```sql
-- HAVING with alias
SELECT name, count(*) AS cnt FROM People p GROUP BY name HAVING cnt > 5
-- Cypher: MATCH (p:People) WITH p.name AS name, count(*) AS cnt WHERE cnt > 5 RETURN name, cnt

-- HAVING with aggregate not in SELECT
SELECT name FROM People p GROUP BY name HAVING count(*) > 5
-- Cypher: MATCH (p:People) WITH p.name AS name, count(*) AS __having_col_0 WHERE __having_col_0 > 5 RETURN name
```

### Full Combination Example

```sql
SELECT DISTINCT department, count(*) AS cnt, max(age) AS max_age
FROM People p WHERE age > 18
GROUP BY department HAVING count(*) > 1 AND max(age) > 25
ORDER BY cnt DESC LIMIT 10 OFFSET 2
-- Cypher: MATCH (p:People) WHERE p.age > 18
--         WITH p.department AS department, count(*) AS cnt, max(p.age) AS max_age
--         WHERE (cnt > 1 AND max_age > 25)
--         RETURN DISTINCT department, cnt, max_age ORDER BY cnt DESC SKIP 2 LIMIT 10
```

> **Not supported:** relationship property aggregation (aggregating over properties stored on Neo4j relationships). Use the Neo4j Spark Connector for this pattern.

## Unsupported Query Patterns

These patterns **do not work** through UC JDBC:

| Pattern | Why It Fails |
|---------|--------------|
| Relationship property aggregation | SQL has no syntax for referencing properties on join edges |

### Workaround

**For relationship property aggregation:** Use Cypher directly via the Neo4j Spark Connector. In the property graph model, relationships carry their own properties (e.g., `claimCount` on a `SHARES_CLAIMS_WITH` relationship), but SQL has no concept of a join edge carrying data.

---

## SQL-to-Cypher Translation Reference

The Neo4j JDBC driver automatically translates SQL to Cypher when `enableSQLTranslation=true`:

| SQL | Cypher |
|-----|--------|
| `SELECT COUNT(*) FROM Flight` | `MATCH (n:Flight) RETURN count(n)` |
| `SELECT COUNT(DISTINCT manufacturer) FROM Aircraft` | `MATCH (n:Aircraft) RETURN count(DISTINCT n.manufacturer)` |
| `SELECT COUNT(*) FROM Aircraft WHERE manufacturer = 'Boeing'` | `MATCH (n:Aircraft) WHERE n.manufacturer = 'Boeing' RETURN count(n)` |
| `FROM A NATURAL JOIN REL NATURAL JOIN B` | `MATCH (a:A)-[:REL]->(b:B)` |
| `SELECT name, count(*) FROM People p GROUP BY name` | `MATCH (p:People) RETURN p.name AS name, count(*)` |
| `SELECT sum(age) FROM People p GROUP BY name` | `MATCH (p:People) WITH sum(p.age) AS __with_col_0, p.name AS __group_col_1 RETURN __with_col_0` |
| `SELECT name, count(*) AS cnt FROM People p GROUP BY name HAVING cnt > 5` | `MATCH (p:People) WITH p.name AS name, count(*) AS cnt WHERE cnt > 5 RETURN name, cnt` |
| `SELECT name FROM People p GROUP BY name HAVING count(*) > 5` | `MATCH (p:People) WITH p.name AS name, count(*) AS __having_col_0 WHERE __having_col_0 > 5 RETURN name` |
| `SELECT name FROM People p GROUP BY name HAVING count(*) > 5 AND max(age) > 50` | `MATCH (p:People) WITH p.name AS name, count(*) AS __having_col_0, max(p.age) AS __having_col_1 WHERE (__having_col_0 > 5 AND __having_col_1 > 50) RETURN name` |
| `SELECT sum(age) FROM People p GROUP BY name ORDER BY sum(age)` | `MATCH (p:People) WITH sum(p.age) AS __with_col_0, p.name AS __group_col_1 RETURN __with_col_0 ORDER BY __with_col_0` |
| `SELECT c.name, count(*) FROM Customers c JOIN Orders o ON c.id = o.customer_id GROUP BY c.name` | `MATCH (c:Customers)<-[customer_id:CUSTOMER_ID]-(o:Orders) RETURN c.name, count(*)` |
| `SELECT DISTINCT name FROM People p GROUP BY name HAVING count(*) > 5 ORDER BY name LIMIT 10 OFFSET 5` | `MATCH (p:People) WITH p.name AS name, count(*) AS __having_col_0 WHERE __having_col_0 > 5 RETURN DISTINCT name ORDER BY name SKIP 5 LIMIT 10` |

See [Neo4j JDBC SQL2Cypher docs](https://neo4j.com/docs/jdbc-manual/current/sql2cypher/) for complete translation rules.

---

## Best Practices

### 1. Always Use customSchema

```python
# Good
df = spark.read.format("jdbc") \
    .option("databricks.connection", "neo4j_connection") \
    .option("query", "SELECT COUNT(*) AS cnt FROM Flight") \
    .option("customSchema", "cnt LONG") \  # Required!
    .load()

# Bad - will fail with NullType errors
df = spark.read.format("jdbc") \
    .option("databricks.connection", "neo4j_connection") \
    .option("query", "SELECT COUNT(*) AS cnt FROM Flight") \
    .load()
```

### 2. Use Aggregates and GROUP BY for UC JDBC

UC JDBC works well for aggregate queries (analytics, counts, summaries) including GROUP BY, HAVING, ORDER BY, and LIMIT. For row-level data access, use the Neo4j Spark Connector.

### 3. Use NATURAL JOIN for Relationships

```sql
-- Query relationships between nodes
SELECT COUNT(*) AS cnt
FROM Flight f
NATURAL JOIN DEPARTS_FROM r
NATURAL JOIN Airport a
```

The relationship type becomes the "table" name in the JOIN.

### 4. Store Credentials in Secrets

```sql
CREATE CONNECTION neo4j_connection TYPE JDBC
OPTIONS (
  user secret('neo4j-scope', 'username'),
  password secret('neo4j-scope', 'password'),
  ...
)
```

### 5. Create a Helper Function for Tests

```python
def query_neo4j(sql_query, schema):
    """Execute a SQL query against Neo4j via UC JDBC."""
    return spark.read.format("jdbc") \
        .option("databricks.connection", "neo4j_connection") \
        .option("query", sql_query) \
        .option("customSchema", schema) \
        .load()

# Usage
df = query_neo4j(
    "SELECT COUNT(*) AS cnt FROM Flight",
    "cnt LONG"
)
```

---

## Choosing the Right Integration Method

| Use Case | Recommended Method | JAR Source |
|----------|-------------------|------------|
| Aggregate analytics (COUNT, SUM, etc.) | UC JDBC Connection | UC Volume (`java_dependencies`) |
| GROUP BY / HAVING analytics | UC JDBC Connection | UC Volume (`java_dependencies`) |
| Graph traversal counts | UC JDBC with NATURAL JOIN | UC Volume (`java_dependencies`) |
| Row-level data access | Neo4j Spark Connector | Cluster library (Maven coordinate) |
| Complex Cypher queries | Neo4j Spark Connector | Cluster library (Maven coordinate) |
| Relationship property aggregation | Neo4j Spark Connector | Cluster library (Maven coordinate) |
| Ad-hoc exploration | Neo4j Python Driver | pip package |

---

## Troubleshooting

### "Connection was closed before the operation completed"

**Cause:** SafeSpark sandbox ran out of memory.

**Fix:** Add these Spark settings to your cluster:
```
spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib 128
spark.databricks.safespark.jdbcSandbox.jvm.xmx.mib 300
spark.databricks.safespark.jdbcSandbox.size.default.mib 512
```

### "syntax error or access rule violation - invalid syntax"

**Cause:** Query pattern not supported through UC (relationship property aggregation).

**Fix:** Use Neo4j Spark Connector for unsupported patterns.

### "No column has been read prior to this call" / NullType errors

**Cause:** Missing `customSchema` option.

**Fix:** Always specify `customSchema` with explicit column types.

---

## References

### Documentation
- [Neo4j JDBC Driver Documentation](https://neo4j.com/docs/jdbc-manual/current/)
- [Neo4j SQL2Cypher Translation](https://neo4j.com/docs/jdbc-manual/current/sql2cypher/)
- [Neo4j Spark Connector](https://neo4j.com/docs/spark/current/)
- [Databricks Unity Catalog JDBC](https://docs.databricks.com/aws/en/connect/jdbc-connection)
- [Spark JDBC Data Sources](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

### Notebooks

| File | Description |
|------|-------------|
| `getting-started/01-neo4j-uc-connection-setup.ipynb` | Create the UC JDBC connection and run basic SQL queries |
| `getting-started/02-federated-queries.ipynb` | Federated queries joining Neo4j graph topology with Delta |
| `getting-started/03-materialized-tables.ipynb` | Materialize Neo4j node labels as managed Delta tables |
