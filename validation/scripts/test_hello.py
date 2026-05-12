"""Minimal smoke test to verify remote execution on Databricks.

Usage:
    Manual smoke test, not part of any DAB job. Run on a cluster directly with
    `databricks workspace import` + `databricks jobs submit`, or invoke the body
    locally for a sanity check.
"""

import os
import sys

print("=" * 60)
print("validation: Remote execution test")
print("=" * 60)
print(f"Python version: {sys.version}")
print(f"DATABRICKS_RUNTIME_VERSION: {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'not set')}")

try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print(f"Spark version: {spark.version}")
except ImportError as e:
    print(f"Spark not available: {e}")

try:
    import neo4j
    print(f"Neo4j Python driver: {neo4j.__version__}")
except ImportError:
    print("Neo4j Python driver: NOT found")

print("=" * 60)
print("SUCCESS: Remote execution verified")
print("=" * 60)
