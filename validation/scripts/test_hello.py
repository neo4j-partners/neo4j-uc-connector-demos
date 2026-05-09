"""Minimal smoke test to verify remote execution on Databricks.

Usage:
    ./upload.sh test_hello.py && ./submit.sh test_hello.py
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
except Exception as e:
    print(f"Spark not available: {e}")

try:
    import neo4j
    print(f"Neo4j Python driver: {neo4j.__version__}")
except ImportError:
    print("Neo4j Python driver: NOT found")

print("=" * 60)
print("SUCCESS: Remote execution verified")
print("=" * 60)
