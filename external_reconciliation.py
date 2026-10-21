# Databricks notebook source
# MAGIC %md
# MAGIC # Reconcile the isolated external Delta fixture
# MAGIC This diagnostic lists only the exact NEW external test-table prefix. It refuses managed tables.
# MAGIC It does not list a bucket root or access managed-table storage paths.

# COMMAND ----------

import json
from datetime import datetime, timezone

for name, default in (
    ("source_catalog", ""), ("source_schema", ""), ("table_name", "fixture_external"),
    ("expected_location", ""), ("max_files", "1000"), ("max_directories", "100"),
):
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("source_catalog").strip()
schema = dbutils.widgets.get("source_schema").strip()
table = dbutils.widgets.get("table_name").strip()
expected_location = dbutils.widgets.get("expected_location").strip().rstrip("/")
max_files = int(dbutils.widgets.get("max_files"))
max_directories = int(dbutils.widgets.get("max_directories"))
if not catalog or not schema.startswith("storage_metrics_test_") or table != "fixture_external":
    raise ValueError("This diagnostic is restricted to the owned isolated storage_metrics_test_* fixture_external table.")
if not expected_location.startswith("s3://") or not expected_location.endswith("/fixture_external"):
    raise ValueError("Supply the exact new external fixture prefix, never a bucket root.")
if not 1 <= max_files <= 10000 or not 1 <= max_directories <= 1000:
    raise ValueError("Diagnostic bounds must be 1..10000 files and 1..1000 directories.")


def q(value):
    return "`" + value.replace("`", "``") + "`"


name = ".".join(q(p) for p in (catalog, schema, table))
metadata = spark.sql(f"""
SELECT table_type, data_source_format FROM {q(catalog)}.information_schema.tables
WHERE table_catalog = :catalog AND table_schema = :schema AND table_name = :table
""", args={"catalog": catalog, "schema": schema, "table": table}).first()
if metadata is None or metadata["table_type"] != "EXTERNAL" or metadata["data_source_format"].upper() != "DELTA":
    raise ValueError("Refusing to list a table that is not an explicitly registered EXTERNAL Delta fixture.")
detail = spark.sql(f"DESCRIBE DETAIL {name}").first().asDict(recursive=True)
if detail["location"].rstrip("/") != expected_location:
    raise ValueError("DESCRIBE DETAIL location differs from expected_location; nothing was listed.")


def listing():
    root = expected_location + "/"
    pending, visited, files = [root], set(), []
    while pending:
        directory = pending.pop()
        if not directory.startswith(root) or directory in visited:
            raise ValueError("Unexpected directory outside the exact fixture prefix or repeated directory.")
        visited.add(directory)
        if len(visited) > max_directories:
            raise ValueError("Directory bound exceeded; reconciliation was not truncated.")
        for item in dbutils.fs.ls(directory):
            if not item.path.startswith(root):
                raise ValueError("Listing returned an item outside the exact fixture prefix.")
            if item.isDir():
                pending.append(item.path)
            else:
                files.append({"relative_path": item.path[len(root):], "size_bytes": int(item.size)})
                if len(files) > max_files:
                    raise ValueError("File bound exceeded; reconciliation was not truncated.")
    files.sort(key=lambda item: item["relative_path"])
    return {"file_count": len(files), "total_bytes": sum(item["size_bytes"] for item in files),
            "directory_count": len(visited), "files": files}


started_at = datetime.now(timezone.utc).isoformat()
before = listing()
native_frame = spark.sql(f"ANALYZE TABLE {name} COMPUTE STORAGE METRICS")
native_rows = [row.asDict(recursive=True) for row in native_frame.collect()]
native = {row["metric_name"]: int(row["metric_value"]) for row in native_rows}
after = listing()
unchanged = before["files"] == after["files"]
matches = (after["total_bytes"] == native["total_bytes"] and after["file_count"] == native["num_total_files"])
result = {
    "result": "INCONCLUSIVE_CHANGED_DURING_CHECK" if not unchanged else ("PASS" if matches else "MISMATCH_REQUIRES_INVESTIGATION"),
    "table": f"{catalog}.{schema}.{table}", "table_type": metadata["table_type"],
    "exact_location": expected_location, "table_id": detail["id"],
    "started_at": started_at, "finished_at": datetime.now(timezone.utc).isoformat(),
    "listing_before": before, "listing_after": after, "listing_unchanged": unchanged,
    "native_schema": native_frame.schema.jsonValue(), "native_rows": native_rows,
    "native_metrics": native, "byte_difference": after["total_bytes"] - native["total_bytes"],
    "file_count_difference": after["file_count"] - native["num_total_files"],
    "limitations": "One small unmodified owned external Delta fixture. No managed path listing, bucket-wide accounting, object versions, replicas or multipart uploads.",
}
result["environment"] = {
    "spark_version": spark.version,
    "current_version": spark.sql("SELECT current_version() AS version").first().asDict(recursive=True),
    "principal": spark.sql("SELECT current_user()").first()[0],
    "metastore": spark.sql("SELECT current_metastore()").first()[0],
}
print(json.dumps(result, indent=2, default=str))
dbutils.notebook.exit(json.dumps(result, sort_keys=True, default=str))
