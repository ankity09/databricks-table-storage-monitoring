# Databricks notebook source
# MAGIC %md
# MAGIC # Collect Unity Catalog table storage
# MAGIC Import this file as a Python notebook. Use Runtime 18.0+ for `full_storage`.
# MAGIC Scope is limited to visible UC managed/external Delta tables; it is not an account-wide inventory.
# MAGIC `active_snapshot` is an explicit alternative and leaves full-storage metrics NULL.
# MAGIC This notebook does not run VACUUM, configure cloud inventory, or create a schedule.

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import types as T


METRIC_NAMES = (
    "total_bytes", "active_bytes", "time_travel_bytes", "vacuumable_bytes",
    "num_total_files", "num_active_files", "num_time_travel_files", "num_vacuumable_files",
)
OUTPUT_NAMES = (
    "storage_collection_runs", "table_storage_snapshots", "storage_collection_runs_latest",
    "storage_collection_scope_latest", "storage_collection_scope_health",
    "table_storage_current", "table_storage_collection_errors",
)


def quote_identifier(value):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("Identifier components must be nonempty strings without NUL characters.")
    return "`" + value.replace("`", "``") + "`"


def qualified_name(*parts):
    return ".".join(quote_identifier(part) for part in parts)


def utc_now():
    return datetime.now(timezone.utc)


def error_fields(exc):
    condition = None
    for method in ("getCondition", "getErrorClass"):
        try:
            condition = getattr(exc, method)()
            if condition:
                break
        except Exception:
            pass
    return condition or type(exc).__name__, str(exc)[:8000]


def parse_storage_metrics(rows):
    metrics, raw = {}, []
    for row in rows:
        value = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        if not {"metric_name", "metric_value", "metric_description"}.issubset(value):
            raise ValueError("Unexpected storage-command output columns; no metrics were recorded.")
        name = value["metric_name"]
        if name in metrics:
            raise ValueError(f"Duplicate metric returned: {name}")
        metric_value = value["metric_value"]
        if metric_value is None or isinstance(metric_value, bool):
            raise ValueError(f"NULL or invalid storage metric: {name}")
        converted = int(metric_value)
        if str(converted) != str(metric_value).strip() or not 0 <= converted <= 9223372036854775807:
            raise ValueError(f"Storage metric is not a nonnegative BIGINT: {name}")
        metrics[name] = converted
        raw.append({**value, "metric_value": converted})
    missing = set(METRIC_NAMES) - set(metrics)
    if missing:
        raise ValueError(f"Storage-command output is missing metrics: {sorted(missing)}")
    if metrics["total_bytes"] < sum(metrics[k] for k in ("active_bytes", "time_travel_bytes", "vacuumable_bytes")):
        raise ValueError("Storage categories exceed total_bytes; result requires investigation.")
    return {name: metrics[name] for name in METRIC_NAMES}, json.dumps(raw, default=str, sort_keys=True)


RUN_SCHEMA = T.StructType([
    T.StructField("scope_id", T.StringType(), False),
    T.StructField("run_id", T.StringType(), False),
    T.StructField("event_sequence", T.IntegerType(), False),
    T.StructField("event_at", T.TimestampType(), False),
    T.StructField("run_started_at", T.TimestampType(), False),
    T.StructField("run_finished_at", T.TimestampType(), True),
    T.StructField("run_status", T.StringType(), False),
    T.StructField("collection_mode", T.StringType(), False),
    T.StructField("collection_method", T.StringType(), False),
    T.StructField("source_catalog", T.StringType(), False),
    T.StructField("source_schema", T.StringType(), False),
    T.StructField("account_id", T.StringType(), True),
    T.StructField("metastore_id", T.StringType(), True),
    T.StructField("workspace_id", T.StringType(), True),
    T.StructField("run_as_principal", T.StringType(), True),
    T.StructField("runtime_json", T.StringType(), True),
    T.StructField("scope_definition_json", T.StringType(), False),
    T.StructField("inventory_mode", T.StringType(), False),
    T.StructField("inventory_completed", T.BooleanType(), False),
    T.StructField("visible_object_count", T.LongType(), True),
    T.StructField("inventory_count", T.LongType(), True),
    T.StructField("success_count", T.LongType(), False),
    T.StructField("error_count", T.LongType(), False),
    T.StructField("error_type", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
])

SNAPSHOT_SCHEMA = T.StructType([
    T.StructField("scope_id", T.StringType(), False),
    T.StructField("run_id", T.StringType(), False),
    T.StructField("collection_mode", T.StringType(), False),
    T.StructField("collection_method", T.StringType(), False),
    T.StructField("account_id", T.StringType(), True),
    T.StructField("metastore_id", T.StringType(), True),
    T.StructField("table_catalog", T.StringType(), False),
    T.StructField("table_schema", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("table_type", T.StringType(), True),
    T.StructField("data_source_format", T.StringType(), True),
    T.StructField("table_id", T.StringType(), True),
    T.StructField("collection_started_at", T.TimestampType(), False),
    T.StructField("collection_finished_at", T.TimestampType(), False),
    T.StructField("observed_at", T.TimestampType(), True),
    T.StructField("collection_status", T.StringType(), False),
    *[T.StructField(name, T.LongType(), True) for name in METRIC_NAMES],
    T.StructField("raw_metrics_json", T.StringType(), True),
    T.StructField("metadata_warning", T.StringType(), True),
    T.StructField("error_type", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
])


def setup_statements(target_catalog, target_schema):
    """DDL is returned separately so setup.sql can contain the same definitions."""
    prefix = qualified_name(target_catalog, target_schema)

    def table_ddl(name, schema, comment):
        columns = ",\n  ".join(f"{quote_identifier(f.name)} {f.dataType.simpleString()}" for f in schema.fields)
        return f"CREATE TABLE IF NOT EXISTS {prefix}.{quote_identifier(name)} (\n  {columns}\n) USING DELTA COMMENT '{comment}'"

    return [
        f"CREATE SCHEMA IF NOT EXISTS {prefix}",
        table_ddl("storage_collection_runs", RUN_SCHEMA, "Append-only storage-collection run events; RUNNING followed by a terminal event."),
        table_ddl("table_storage_snapshots", SNAPSHOT_SCHEMA, "Append-only per-table observations; failed metrics are NULL, never zero."),
        f"""CREATE VIEW IF NOT EXISTS {prefix}.storage_collection_runs_latest AS
SELECT * EXCEPT (_rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY scope_id, collection_mode, run_id
    ORDER BY event_sequence DESC, event_at DESC
  ) AS _rn
  FROM {prefix}.storage_collection_runs
) WHERE _rn = 1""",
        f"""CREATE VIEW IF NOT EXISTS {prefix}.storage_collection_scope_latest AS
SELECT * EXCEPT (_rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY scope_id, collection_mode
    ORDER BY run_started_at DESC, run_id DESC
  ) AS _rn
  FROM {prefix}.storage_collection_runs_latest
  WHERE run_status IN ('COMPLETE', 'PARTIAL', 'FAILED')
) WHERE _rn = 1""",
        f"""CREATE VIEW IF NOT EXISTS {prefix}.storage_collection_scope_health AS
WITH attempts AS (
  SELECT * EXCEPT (_rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY scope_id, collection_mode
      ORDER BY run_started_at DESC, run_id DESC
    ) AS _rn
    FROM {prefix}.storage_collection_runs_latest
  ) WHERE _rn = 1
)
SELECT a.*,
       c.run_id AS current_run_id,
       c.run_status AS current_run_status,
       c.run_finished_at AS current_run_finished_at,
       c.inventory_count AS current_inventory_count,
       c.success_count AS current_success_count,
       c.error_count AS current_error_count
FROM attempts a
LEFT JOIN {prefix}.storage_collection_scope_latest c
  ON a.scope_id = c.scope_id AND a.collection_mode = c.collection_mode""",
        f"""CREATE VIEW IF NOT EXISTS {prefix}.table_storage_current AS
SELECT s.*, r.run_status, r.run_started_at, r.run_finished_at,
       r.inventory_count, r.success_count, r.error_count
FROM {prefix}.table_storage_snapshots s
JOIN {prefix}.storage_collection_scope_latest r
  ON s.scope_id = r.scope_id AND s.collection_mode = r.collection_mode AND s.run_id = r.run_id""",
        f"""CREATE VIEW IF NOT EXISTS {prefix}.table_storage_collection_errors AS
SELECT * FROM {prefix}.table_storage_current WHERE collection_status = 'ERROR'""",
    ]


def append_rows(spark_session, table_name, rows, schema):
    if rows:
        spark_session.createDataFrame(rows, schema=schema).write.format("delta").mode("append").saveAsTable(table_name)


def collect_storage(spark_session, *, scope_id, source_catalog, source_schema,
                    target_catalog, target_schema, collection_mode="full_storage",
                    max_tables=10000, batch_size=100, account_id=None, metastore_id=None,
                    explicit_tables_json=""):
    """Collect a bounded, explicit scope. No implicit fallback and no historical-success carry-forward."""
    for label, value in (("scope_id", scope_id), ("source_catalog", source_catalog),
                         ("source_schema", source_schema), ("target_catalog", target_catalog), ("target_schema", target_schema)):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} must be explicitly provided.")
    if source_catalog.lower() in {"hive_metastore", "system"}:
        raise ValueError("Use an explicit Unity Catalog source catalog, excluding hive_metastore and system.")
    if collection_mode not in {"full_storage", "active_snapshot"}:
        raise ValueError("collection_mode must be full_storage or active_snapshot.")
    if not 1 <= int(max_tables) <= 1000000 or not 1 <= int(batch_size) <= 1000:
        raise ValueError("max_tables must be 1..1000000 and batch_size must be 1..1000.")
    max_tables, batch_size = int(max_tables), int(batch_size)
    allowlist = None
    if explicit_tables_json.strip():
        allowlist = json.loads(explicit_tables_json)
        if source_schema == "*" or not isinstance(allowlist, list) or any(not isinstance(t, str) or not t for t in allowlist):
            raise ValueError("explicit_tables_json must be a JSON list of table-name components within one explicit source_schema.")
        if len({name.casefold() for name in allowlist}) != len(allowlist):
            raise ValueError("explicit_tables_json contains duplicate names.")
        if len(allowlist) > max_tables:
            raise ValueError("Explicit allowlist exceeds max_tables; no tables were collected.")
        if (source_catalog.casefold(), source_schema.casefold()) == (target_catalog.casefold(), target_schema.casefold()):
            if any(name.casefold() in OUTPUT_NAMES for name in allowlist):
                raise ValueError("The collector output objects cannot appear in the source allowlist.")

    for statement in setup_statements(target_catalog, target_schema):
        spark_session.sql(statement).collect()
    prefix = qualified_name(target_catalog, target_schema)
    runs_table, snapshots_table = prefix + ".storage_collection_runs", prefix + ".table_storage_snapshots"

    def optional_sql(statement):
        try:
            return spark_session.sql(statement).first()[0]
        except Exception:
            return None

    runtime = optional_sql("SELECT current_version()")
    if hasattr(runtime, "asDict"):
        runtime = runtime.asDict(recursive=True)
    try:
        workspace_id = spark_session.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    except Exception:
        workspace_id = None
    started = utc_now()
    run = dict(
        scope_id=scope_id, run_id=str(uuid.uuid4()), event_sequence=0, event_at=started,
        run_started_at=started, run_finished_at=None, run_status="RUNNING",
        collection_mode=collection_mode,
        collection_method="DIRECT_LISTING" if collection_mode == "full_storage" else "DESCRIBE_DETAIL",
        source_catalog=source_catalog, source_schema=source_schema,
        account_id=account_id or None, metastore_id=metastore_id or optional_sql("SELECT current_metastore()"),
        workspace_id=workspace_id, run_as_principal=optional_sql("SELECT current_user()"),
        runtime_json=json.dumps(runtime, default=str, sort_keys=True),
        scope_definition_json=json.dumps({"source_catalog": source_catalog, "source_schema": source_schema,
                                         "table_types": ["MANAGED", "EXTERNAL"], "formats": ["DELTA"],
                                         "explicit_tables": allowlist, "output_objects_excluded": list(OUTPUT_NAMES)}, sort_keys=True),
        inventory_mode="ALLOWLIST" if allowlist is not None else "INFORMATION_SCHEMA",
        inventory_completed=False, visible_object_count=None, inventory_count=None,
        success_count=0, error_count=0, error_type=None, error_message=None,
    )
    append_rows(spark_session, runs_table, [run], RUN_SCHEMA)
    pending = []
    try:
        # Spark enumerates the full visible scope. A max+1 guard FAILS rather than silently truncating.
        inventory = spark_session.sql(f"""
SELECT table_catalog, table_schema, table_name, table_type, data_source_format
FROM {qualified_name(source_catalog, 'information_schema', 'tables')}
WHERE lower(table_catalog) = lower(:catalog)
  AND table_schema <> 'information_schema'
  AND (:schema = '*' OR lower(table_schema) = lower(:schema))
""", args={"catalog": source_catalog, "schema": source_schema})
        from pyspark.sql import functions as F
        output_condition = ((F.lower(F.col("table_catalog")) == target_catalog.casefold()) &
                            (F.lower(F.col("table_schema")) == target_schema.casefold()) &
                            F.lower(F.col("table_name")).isin(*OUTPUT_NAMES))
        inventory = inventory.where(~output_condition)
        run["visible_object_count"] = inventory.count()
        eligible = inventory.where(F.col("table_type").isin("MANAGED", "EXTERNAL") & (F.upper(F.col("data_source_format")) == "DELTA"))
        if allowlist is not None:
            existing = {row["table_name"].casefold(): row.asDict() for row in inventory.where(F.lower(F.col("table_name")).isin(*[n.casefold() for n in allowlist])).collect()} if allowlist else {}
            tables = []
            for name in allowlist:
                table = existing.get(name.casefold(), dict(table_catalog=source_catalog, table_schema=source_schema,
                                                        table_name=name, table_type=None, data_source_format=None))
                # An explicit existing object outside the supported scope is a visible collection error.
                tables.append(table)
        else:
            tables = [row.asDict() for row in eligible.orderBy("table_schema", "table_name").limit(max_tables + 1).collect()]
            if len(tables) > max_tables:
                raise ValueError(f"Eligible inventory exceeds max_tables={max_tables}. Increase the explicit bound or narrow the scope; no measurements were made.")
        run["inventory_count"] = len(tables)
        run["inventory_completed"] = True
        for table in tables:
            attempt_at = utc_now()
            result = {name: None for name in SNAPSHOT_SCHEMA.fieldNames()}
            result.update({key: run[key] for key in ("scope_id", "run_id", "collection_mode", "collection_method", "account_id", "metastore_id")})
            result.update(table)
            result["collection_started_at"] = attempt_at
            try:
                if table["table_type"] is not None and (table["table_type"] not in {"MANAGED", "EXTERNAL"} or (table["data_source_format"] or "").upper() != "DELTA"):
                    raise ValueError("Explicit table is outside this collector's UC managed/external Delta scope.")
                name = qualified_name(table["table_catalog"], table["table_schema"], table["table_name"])
                if collection_mode == "full_storage":
                    metrics, raw = parse_storage_metrics(spark_session.sql(f"ANALYZE TABLE {name} COMPUTE STORAGE METRICS").collect())
                    result.update(metrics)
                    result["raw_metrics_json"] = raw
                    # Identity is best-effort metadata; a DESCRIBE failure does not erase valid ANALYZE results.
                    try:
                        detail = spark_session.sql(f"DESCRIBE DETAIL {name}").first().asDict(recursive=True)
                        result["table_id"] = str(detail.get("id")) if detail.get("id") is not None else None
                    except Exception as exc:
                        result["metadata_warning"] = ": ".join(error_fields(exc))
                else:
                    detail = spark_session.sql(f"DESCRIBE DETAIL {name}").first().asDict(recursive=True)
                    result["table_id"] = str(detail.get("id")) if detail.get("id") is not None else None
                    result["active_bytes"] = int(detail["sizeInBytes"])
                    result["num_active_files"] = int(detail["numFiles"])
                    result["raw_metrics_json"] = json.dumps({"sizeInBytes": result["active_bytes"], "numFiles": result["num_active_files"]}, sort_keys=True)
                result["observed_at"] = utc_now()
                result["collection_status"] = "SUCCESS"
                run["success_count"] += 1
            except Exception as exc:
                for metric in METRIC_NAMES:
                    result[metric] = None
                result["raw_metrics_json"] = None
                result["collection_status"] = "ERROR"
                result["error_type"], result["error_message"] = error_fields(exc)
                run["error_count"] += 1
            result["collection_finished_at"] = utc_now()
            pending.append(result)
            if len(pending) >= batch_size:
                append_rows(spark_session, snapshots_table, pending, SNAPSHOT_SCHEMA)
                pending.clear()
        append_rows(spark_session, snapshots_table, pending, SNAPSHOT_SCHEMA)
        pending.clear()
        run["run_status"] = "COMPLETE" if run["error_count"] == 0 else ("PARTIAL" if run["success_count"] > 0 else "FAILED")
        if run["error_count"]:
            run["error_type"] = "TABLE_COLLECTION_ERRORS"
            run["error_message"] = f"{run['error_count']} of {run['inventory_count']} table measurements failed; inspect table_storage_collection_errors."
    except Exception as exc:
        # Discovery/write failures remain terminal FAILED events and never revive an older scope snapshot.
        run["run_status"] = "FAILED"
        run["error_type"], run["error_message"] = error_fields(exc)
    run.update(event_sequence=1, event_at=utc_now(), run_finished_at=utc_now())
    append_rows(spark_session, runs_table, [run], RUN_SCHEMA)
    return {key: value.isoformat() if isinstance(value, datetime) else value for key, value in run.items()}

# COMMAND ----------

for widget_name, default in (
    ("scope_id", ""), ("source_catalog", ""), ("source_schema", ""),
    ("target_catalog", ""), ("target_schema", ""), ("max_tables", "10000"),
    ("batch_size", "100"), ("account_id", ""), ("metastore_id", ""), ("explicit_tables_json", ""),
):
    dbutils.widgets.text(widget_name, default)
dbutils.widgets.dropdown("collection_mode", "full_storage", ["full_storage", "active_snapshot"])
dbutils.widgets.dropdown("exit_on_error", "true", ["true", "false"])

parameters = {name: dbutils.widgets.get(name).strip() for name in (
    "scope_id", "source_catalog", "source_schema", "target_catalog", "target_schema",
    "collection_mode", "max_tables", "batch_size", "account_id", "metastore_id", "explicit_tables_json",
)}
collection_result = collect_storage(spark, **parameters)
print(json.dumps(collection_result, indent=2, sort_keys=True))
if dbutils.widgets.get("exit_on_error") == "true" and collection_result["run_status"] != "COMPLETE":
    raise RuntimeError(f"Storage collection {collection_result['run_id']} finished {collection_result['run_status']}; details were persisted to the run and snapshot tables.")
dbutils.notebook.exit(json.dumps(collection_result, sort_keys=True))
