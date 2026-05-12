"""One-shot task: grant CREATE_EXTERNAL_METADATA on metastore to a principal.

Required for run_05_metadata_sync_external_api.py to register external metadata
entries through the Lineage Tracking API.

Reads the principal from the first CLI argument. When unset, defaults to the
run-scoped Databricks principal returned by current_user().
"""

import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

if len(sys.argv) > 1 and sys.argv[1].strip():
    principal = sys.argv[1].strip()
else:
    principal = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

escaped = principal.replace("`", "``")
spark.sql(f"GRANT CREATE_EXTERNAL_METADATA ON METASTORE TO `{escaped}`")
print(f"Granted CREATE_EXTERNAL_METADATA on metastore to {principal}")
