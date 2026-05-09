# How We Granted CREATE_EXTERNAL_METADATA

## The Problem

`run_04_metadata_sync_api.py` registers Neo4j node labels and relationship types
in Unity Catalog's External Metadata API (`/api/2.0/lineage-tracking/external-metadata`).
Every registration call failed with:

```
PERMISSION_DENIED: User does not have CREATE EXTERNAL METADATA on Metastore
```

## The Solution

The `CREATE_EXTERNAL_METADATA` privilege is a metastore-level grant. It cannot be
granted from the account-level API (the `azure-account-admin` profile points to
`accounts.azuredatabricks.net`, which doesn't serve the UC permissions endpoint).

The grant must run as a **SQL statement on the workspace itself**. The trick: the
user's own cluster can execute the GRANT because the cluster runs with the user's
identity, and Databricks allows users who are metastore admins (or have sufficient
privileges) to grant metastore-level permissions via SQL.

### What Worked

We submitted a one-shot PySpark job on the existing all-purpose cluster:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("GRANT CREATE_EXTERNAL_METADATA ON METASTORE TO `ryan.knight@neo4j.com`")
```

Submitted via:

```bash
databricks workspace import --file grant_metadata.py ...
databricks jobs submit --json '{
  "tasks": [{
    "task_key": "grant",
    "spark_python_task": {"python_file": ".../grant_metadata.py"},
    "existing_cluster_id": "0104-134656-pvz3m00y"
  }]
}'
```

The job succeeded because the cluster's user identity had sufficient privilege
to issue the GRANT (either as a metastore admin or through inherited permissions).

### What Didn't Work

1. **Account-level API (`azure-account-admin` profile):** The account host
   (`accounts.azuredatabricks.net`) returns HTML for the
   `/api/2.1/unity-catalog/permissions/metastore/{id}` endpoint. This API path
   only works on workspace hosts, not account hosts.

2. **Account-level path prefix:** Tried
   `/api/2.0/accounts/{account_id}/unity-catalog/permissions/metastore/{id}` —
   returned "No API found".

3. **SQL warehouse:** Could have worked, but both warehouses were stopped and
   starting one would have added latency. The all-purpose cluster was already running.

## Key Details

| Detail | Value |
|--------|-------|
| Metastore ID | `62b20e7e-cd6f-4c4f-9475-14dbf36230f5` |
| Metastore Name | `metastore_azure_eastus` |
| Granted To | `ryan.knight@neo4j.com` |
| Privilege | `CREATE_EXTERNAL_METADATA` |
| Method | SQL via `spark_python_task` on workspace cluster |
| Workspace Profile | `azure-rk-knight` |
| Cluster | `0104-134656-pvz3m00y` |

## Reusable Script

`grant_external_metadata.sh` automates discovering the current workspace user,
uploads a one-shot PySpark task, and runs the SQL grant through the configured
workspace cluster. For a different user, pass the principal explicitly:

```bash
./grant_external_metadata.sh other.user@example.com
```

## Result After Grant

`run_04_metadata_sync_api.py` registered all 9 Neo4j node labels successfully
(14/15 checks passed — the only failure was cleanup/deletion, which may require
a separate `DELETE EXTERNAL METADATA` privilege or a different API path).
