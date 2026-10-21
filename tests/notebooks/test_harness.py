# Databricks notebook source
# MAGIC %md
# MAGIC # Isolated table-storage feature test
# MAGIC Run only in the explicitly selected test workspace. Creates a NEW unique schema with small owned fixtures.
# MAGIC Set `catalog` to an existing writable UC catalog and `collector_notebook_path` to the imported collector.
# MAGIC Optional `external_base_url` must be an existing permitted external location. A new unique child prefix is used.
# MAGIC No cloud resources, schedules, VACUUM, or shared-compute changes are made. Fixtures remain for review.

# COMMAND ----------

import json
import re
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("collector_notebook_path", "")
dbutils.widgets.text("external_base_url", "")
dbutils.widgets.text("account_id", "")
dbutils.widgets.text("workspace_url", "")
dbutils.widgets.text("compute_description", "")
dbutils.widgets.text("test_suffix", "")

catalog = dbutils.widgets.get("catalog").strip()
collector_path = dbutils.widgets.get("collector_notebook_path").strip()
external_base_url = dbutils.widgets.get("external_base_url").strip()
if not catalog or not collector_path.startswith("/"):
    raise ValueError("Explicit catalog and absolute collector_notebook_path are required.")
if catalog.lower() in {"hive_metastore", "system"}:
    raise ValueError("Use an existing writable Unity Catalog catalog.")

suffix = dbutils.widgets.get("test_suffix").strip() or (datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])
if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", suffix):
    raise ValueError("test_suffix must contain 1..64 letters, digits or underscores; use a new unique suffix for every run.")
test_schema = "storage_metrics_test_" + suffix
scope = "storage-feature-test:" + suffix
metric_names = (
    "total_bytes", "active_bytes", "time_travel_bytes", "vacuumable_bytes",
    "num_total_files", "num_active_files", "num_time_travel_files", "num_vacuumable_files",
)


def q(value):
    return "`" + value.replace("`", "``") + "`"


def fq(table_name):
    return ".".join(q(p) for p in (catalog, test_schema, table_name))


def serializable(value):
    return json.loads(json.dumps(value, default=str))


def sql_rows(statement, args=None):
    return [row.asDict(recursive=True) for row in spark.sql(statement, args=args or {}).collect()]


checks, runs = [], []
results = {
    "test_schema": f"{catalog}.{test_schema}", "scope_id": scope,
    "created_at": datetime.now(timezone.utc).isoformat(), "checks": checks, "collector_runs": runs,
    "fixtures_retained": True,
    "limitations": [
        "Fixture owner execution does not establish minimum production reader privileges.",
        "Inventory-backed collection was not tested; no cloud inventory was created.",
        "Managed/external Delta only; no clones, materialized views, streaming tables, Iceberg or Hive-metastore validation.",
        "Small-fixture testing does not establish large-estate runtime or listing cost.",
    ],
}
results["dashboard_test_scopes"] = {
    "complete": scope,
    "partial": scope + ":partial",
    "failed": scope + ":failed",
    "empty": scope + ":empty",
    "discovery_failed": scope + ":discovery-failed",
    "interrupted": scope + ":interrupted",
    "no_run": scope + ":never-run",
}


def check(name, condition, evidence=None):
    checks.append({"test": name, "result": "PASS" if condition else "FAIL", "evidence": serializable(evidence)})


def record_error(name, exc, classification="INCONCLUSIVE"):
    checks.append({"test": name, "result": classification, "error_type": type(exc).__name__, "error": str(exc)[:12000]})


def metrics_for(table_name):
    frame = spark.sql(f"ANALYZE TABLE {fq(table_name)} COMPUTE STORAGE METRICS")
    rows = [row.asDict(recursive=True) for row in frame.collect()]
    return {
        "schema": frame.schema.jsonValue(), "rows": rows,
        "metrics": {row["metric_name"]: int(row["metric_value"]) for row in rows},
    }


def collect(*, names=None, mode="full_storage", max_tables=10000, collection_scope=None):
    parameters = {
        "scope_id": collection_scope or scope, "source_catalog": catalog, "source_schema": test_schema,
        "target_catalog": catalog, "target_schema": test_schema,
        "collection_mode": mode, "max_tables": str(max_tables), "batch_size": "2",
        "account_id": dbutils.widgets.get("account_id"), "metastore_id": "",
        "explicit_tables_json": "" if names is None else json.dumps(names), "exit_on_error": "false",
    }
    output = dbutils.notebook.run(collector_path, 1800, parameters)
    run = json.loads(output)
    runs.append(run)
    return run


def current_rows(mode="full_storage", collection_scope=None):
    return sql_rows(f"SELECT * FROM {fq('table_storage_current')} WHERE scope_id = :scope AND collection_mode = :mode ORDER BY table_name", {"scope": collection_scope or scope, "mode": mode})


def health(mode="full_storage", collection_scope=None):
    return sql_rows(f"SELECT * FROM {fq('storage_collection_scope_health')} WHERE scope_id = :scope AND collection_mode = :mode", {"scope": collection_scope or scope, "mode": mode})


def totals(mode="full_storage", collection_scope=None):
    return sql_rows(f"SELECT COUNT(*) AS rows, SUM(total_bytes) AS total_bytes, SUM(active_bytes) AS active_bytes FROM {fq('table_storage_current')} WHERE scope_id = :scope AND collection_mode = :mode", {"scope": collection_scope or scope, "mode": mode})[0]


environment = {
    "workspace_url": dbutils.widgets.get("workspace_url"),
    "compute_description": dbutils.widgets.get("compute_description"),
    "spark_version": spark.version,
}
for label, statement in (
    ("current_version", "SELECT current_version() AS value"),
    ("current_user", "SELECT current_user() AS value"),
    ("current_metastore", "SELECT current_metastore() AS value"),
):
    try:
        environment[label] = sql_rows(statement)
    except Exception as exc:
        environment[label] = {"error": str(exc)[:2000]}
for key in ("spark.databricks.clusterUsageTags.sparkVersion", "spark.databricks.clusterUsageTags.clusterId",
            "spark.databricks.clusterUsageTags.clusterOwnerOrgId"):
    try:
        environment[key] = spark.conf.get(key)
    except Exception:
        environment[key] = None
results["environment"] = environment

# COMMAND ----------

# CREATE without IF NOT EXISTS ensures this test cannot reuse someone else's schema.
spark.sql(f"CREATE SCHEMA {q(catalog)}.{q(test_schema)} COMMENT 'Owned isolated table-storage feature test; safe to remove after evidence review'").collect()
fixture_names = ["fixture_current", "fixture_history", "fixture-hyphen"]
for table_name, rows in (("fixture_current", 2048), ("fixture_history", 4096), ("fixture-hyphen", 128)):
    spark.sql(f"""CREATE TABLE {fq(table_name)} USING DELTA
TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days') AS
SELECT id, sha2(concat('deterministic-v1-', cast(id AS STRING)), 256) AS payload
FROM range({rows})""").collect()
results["fixtures"] = [{"table": f"{catalog}.{test_schema}.{name}", "kind": "MANAGED"} for name in fixture_names]

if external_base_url:
    try:
        if not external_base_url.startswith("s3://") or any(c in external_base_url for c in ("'", '"', "\\", "\n", "\r")):
            raise ValueError("external_base_url must be a plain s3:// URI under an existing permitted external location.")
        external_path = external_base_url.rstrip("/") + "/" + test_schema + "/fixture_external"
        # A unique random prefix is still checked before any write.
        try:
            existing = dbutils.fs.ls(external_path)
        except Exception as exc:
            if "FileNotFoundException" not in str(exc) and "PATH_NOT_FOUND" not in str(exc) and "does not exist" not in str(exc):
                raise
            existing = []
        if existing:
            raise RuntimeError(f"Refusing to use an existing external prefix: {external_path}; entries={len(existing)}")
        spark.sql(f"""CREATE TABLE {fq('fixture_external')} USING DELTA LOCATION '{external_path}' AS
SELECT id, sha2(concat('external-v1-', cast(id AS STRING)), 256) AS payload FROM range(512)""").collect()
        fixture_names.append("fixture_external")
        results["fixtures"].append({"table": f"{catalog}.{test_schema}.fixture_external", "kind": "EXTERNAL", "path": external_path})
        results["external_fixture_setup"] = "PASS"
    except Exception as exc:
        record_error("external_fixture_setup", exc)
        results["external_fixture_setup"] = "INCONCLUSIVE"
else:
    results["external_fixture_setup"] = "NOT_TESTED: no permitted existing external location supplied"

baseline_frame = spark.sql(f"DESCRIBE DETAIL {fq('fixture_current')}")
baseline = baseline_frame.first().asDict(recursive=True)
results["describe_detail_baseline"] = {"schema": baseline_frame.schema.jsonValue(), "row": serializable(baseline)}
check("describe_detail_active_snapshot", baseline["sizeInBytes"] > 0 and baseline["numFiles"] > 0, baseline)

full_available = False
try:
    baseline_full = metrics_for("fixture_current")
    results["analyze_baseline"] = baseline_full
    metrics = baseline_full["metrics"]
    check("analyze_output_schema", [f["name"] for f in baseline_full["schema"]["fields"]] == ["metric_name", "metric_value", "metric_description"], baseline_full["schema"])
    check("all_eight_documented_metrics", set(metric_names).issubset(metrics), baseline_full["rows"])
    check("baseline_active_matches_describe_detail", metrics.get("active_bytes") == baseline["sizeInBytes"] and metrics.get("num_active_files") == baseline["numFiles"], {"full": metrics, "detail": baseline})
    check("baseline_storage_categories", metrics["total_bytes"] >= metrics["active_bytes"] + metrics["time_travel_bytes"] + metrics["vacuumable_bytes"] and all(metrics[k] >= 0 for k in metric_names), metrics)
    full_available = set(metric_names).issubset(metrics)
except Exception as exc:
    record_error("full_storage_command_prerequisite", exc)
    results["full_storage_command_available"] = False
results["full_storage_command_available"] = full_available

spark.sql(f"""INSERT OVERWRITE TABLE {fq('fixture_history')}
SELECT id, sha2(concat('deterministic-v2-', cast(id AS STRING)), 256) AS payload FROM range(1024)""").collect()
results["retained_delta_history"] = sql_rows(f"DESCRIBE HISTORY {fq('fixture_history')}")

if full_available:
    try:
        retained = metrics_for("fixture_history")
        results["analyze_retained_history"] = retained
        m = retained["metrics"]
        check("retained_history_is_measured", m["time_travel_bytes"] > 0 and m["num_time_travel_files"] > 0 and m["total_bytes"] > m["active_bytes"], retained)
        if "fixture_external" in fixture_names:
            external = metrics_for("fixture_external")
            external_detail = spark.sql(f"DESCRIBE DETAIL {fq('fixture_external')}").first().asDict(recursive=True)
            results["analyze_external"] = external
            check("external_table_full_storage", set(metric_names).issubset(external["metrics"]) and external["metrics"]["active_bytes"] == external_detail["sizeInBytes"], {"full": external, "detail": external_detail})
    except Exception as exc:
        record_error("retained_or_external_feature_check", exc, "FAIL")

    try:
        first = collect()
        first_current = current_rows()
        results["first_current_rows"] = first_current
        check("first_run_complete_and_output_tables_excluded", first["run_status"] == "COMPLETE" and first["inventory_count"] == len(fixture_names) and {r["table_name"] for r in first_current} == set(fixture_names), {"run": first, "current": first_current})
        snapshot_schema = spark.table(fq("table_storage_snapshots")).schema
        check("persisted_metrics_are_bigint", all(snapshot_schema[name].dataType.simpleString() == "bigint" for name in metric_names), snapshot_schema.jsonValue())

        spark.sql(f"""INSERT OVERWRITE TABLE {fq('fixture_current')}
SELECT id, sha2(concat('larger-v2-', cast(id AS STRING)), 256) AS payload FROM range(8192)""").collect()
        second = collect()
        second_current = current_rows()
        history_count = sql_rows(f"SELECT COUNT(*) AS n, COUNT(DISTINCT run_id) AS run_count FROM {fq('table_storage_snapshots')} WHERE scope_id = :scope AND collection_mode = 'full_storage'", {"scope": scope})[0]
        check("two_runs_append_history_without_current_double_counting", first["run_id"] != second["run_id"] and second["run_status"] == "COMPLETE" and len(second_current) == len(fixture_names) and all(r["run_id"] == second["run_id"] for r in second_current) and history_count["n"] == 2 * len(fixture_names) and history_count["run_count"] == 2, {"current": second_current, "history": history_count, "current_totals": totals()})

        missing = "fixture_does_not_exist"
        partial_scope = results["dashboard_test_scopes"]["partial"]
        partial = collect(names=fixture_names + [missing], collection_scope=partial_scope)
        partial_rows = current_rows(collection_scope=partial_scope)
        missing_rows = [r for r in partial_rows if r["table_name"] == missing]
        check("partial_failure_visible_and_null", partial["run_status"] == "PARTIAL" and partial["success_count"] == len(fixture_names) and partial["error_count"] == 1 and len(partial_rows) == len(fixture_names) + 1 and len(missing_rows) == 1 and missing_rows[0]["collection_status"] == "ERROR" and missing_rows[0]["observed_at"] is None and all(missing_rows[0][k] is None for k in metric_names), {"run": partial, "current": partial_rows})
        results["partial_current_rows"] = partial_rows

        failed_scope = results["dashboard_test_scopes"]["failed"]
        all_errors = collect(names=[missing], collection_scope=failed_scope)
        failed_rows = current_rows(collection_scope=failed_scope)
        check("all_errors_failed_and_total_unknown", all_errors["run_status"] == "FAILED" and all_errors["success_count"] == 0 and all_errors["error_count"] == 1 and len(failed_rows) == 1 and failed_rows[0]["run_id"] == all_errors["run_id"] and totals(collection_scope=failed_scope)["total_bytes"] is None, {"run": all_errors, "current": failed_rows, "totals": totals(collection_scope=failed_scope)})

        empty = collect(names=[])
        check("empty_inventory_does_not_revive_older_rows", empty["run_status"] == "COMPLETE" and empty["inventory_completed"] and empty["inventory_count"] == 0 and current_rows() == [] and totals()["total_bytes"] is None, {"run": empty, "health": health(), "totals": totals()})
        collect(names=[], collection_scope=results["dashboard_test_scopes"]["empty"])

        guard = collect(max_tables=1)
        guard_health = health()
        check("inventory_guard_fails_without_truncation_or_stale_rows", guard["run_status"] == "FAILED" and not guard["inventory_completed"] and guard["inventory_count"] is None and current_rows() == [] and guard_health[0]["run_id"] == guard["run_id"] and guard_health[0]["current_run_id"] == guard["run_id"] and guard_health[0]["error_message"], {"run": guard, "health": guard_health, "totals": totals()})
        collect(max_tables=1, collection_scope=results["dashboard_test_scopes"]["discovery_failed"])

        retry = collect(names=fixture_names + [missing])
        retry_rows = current_rows()
        duplicate_count = sql_rows(f"SELECT COUNT(*) AS n FROM (SELECT table_catalog, table_schema, table_name FROM {fq('table_storage_current')} WHERE scope_id = :scope AND collection_mode = 'full_storage' GROUP BY ALL HAVING COUNT(*) <> 1)", {"scope": scope})[0]["n"]
        check("retry_is_append_only_and_current_is_latest_attempt", retry["run_status"] == "PARTIAL" and len(retry_rows) == len(fixture_names) + 1 and all(row["run_id"] == retry["run_id"] for row in retry_rows) and duplicate_count == 0, {"run": retry, "health": health(), "current": retry_rows})

        # A synthetic manifest event tests interruption semantics; no table metrics are fabricated.
        interrupted_scope = results["dashboard_test_scopes"]["interrupted"]
        qa_terminal = collect(names=["fixture_current"], collection_scope=interrupted_scope)
        qa_current = current_rows(collection_scope=interrupted_scope)
        manifest = spark.table(fq("storage_collection_runs"))
        from pyspark.sql import functions as F
        synthetic = manifest.where((F.col("event_sequence") == 1) & (F.col("run_id") == qa_terminal["run_id"])).first().asDict(recursive=True)
        synthetic.update(run_id=str(uuid.uuid4()), event_sequence=0, event_at=datetime.now(timezone.utc),
                         run_started_at=datetime.now(timezone.utc), run_finished_at=None, run_status="RUNNING",
                         inventory_completed=False, visible_object_count=None, inventory_count=None,
                         success_count=0, error_count=0, error_type=None, error_message=None,
                         scope_definition_json=json.dumps({"synthetic_manifest_test": "Interrupted-run visibility; no observations created"}))
        spark.createDataFrame([synthetic], schema=manifest.schema).write.format("delta").mode("append").saveAsTable(fq("storage_collection_runs"))
        interrupted_health = health(collection_scope=interrupted_scope)
        interrupted_current = current_rows(collection_scope=interrupted_scope)
        check("interrupted_attempt_visible_while_current_stays_terminal", interrupted_health[0]["run_status"] == "RUNNING" and interrupted_health[0]["run_id"] == synthetic["run_id"] and interrupted_health[0]["current_run_id"] == qa_terminal["run_id"] and {row["run_id"] for row in interrupted_current} == {row["run_id"] for row in qa_current}, {"health": interrupted_health, "current": interrupted_current})
        results["interrupted_running_health"] = interrupted_health
        synthetic.update(event_sequence=1, event_at=datetime.now(timezone.utc), run_finished_at=datetime.now(timezone.utc),
                         run_status="FAILED", error_type="SYNTHETIC_INTERRUPTED_RUN_TEST",
                         error_message="Closed synthetic diagnostic event after checking RUNNING visibility; no table observations were created.")
        spark.createDataFrame([synthetic], schema=manifest.schema).write.format("delta").mode("append").saveAsTable(fq("storage_collection_runs"))
        check("closed_interrupted_attempt_cannot_revive_old_measurements", current_rows(collection_scope=interrupted_scope) == [] and health(collection_scope=interrupted_scope)[0]["run_status"] == "FAILED", health(collection_scope=interrupted_scope))

        final_complete = collect()
        final_rows = current_rows()
        check("final_primary_scope_complete", final_complete["run_status"] == "COMPLETE" and len(final_rows) == len(fixture_names) and all(row["run_id"] == final_complete["run_id"] for row in final_rows), {"run": final_complete, "current": final_rows})
        results["final_full_storage_current"] = final_rows
        results["final_full_storage_health"] = health()
    except Exception as exc:
        record_error("collector_end_to_end", exc, "FAIL")

# COMMAND ----------

# This is a separately selected mode; it is never counted as a passing full-storage test.
try:
    active_run = collect(names=fixture_names, mode="active_snapshot")
    active_rows = current_rows("active_snapshot")
    check("explicit_active_mode_has_no_total_or_retained_metrics", active_run["run_status"] == "COMPLETE" and len(active_rows) == len(fixture_names) and all(row["active_bytes"] is not None and row["num_active_files"] is not None and all(row[k] is None for k in ("total_bytes", "time_travel_bytes", "vacuumable_bytes", "num_total_files", "num_time_travel_files", "num_vacuumable_files")) for row in active_rows), {"run": active_run, "current": active_rows})
    if full_available and "final_full_storage_current" in results:
        check("active_mode_cannot_replace_full_storage_scope", {row["run_id"] for row in current_rows()} == {row["run_id"] for row in results["final_full_storage_current"]}, {"full_health": health(), "active_health": health("active_snapshot")})
except Exception as exc:
    record_error("active_snapshot_collector", exc, "FAIL")

try:
    results["run_manifest_latest"] = sql_rows(f"SELECT * FROM {fq('storage_collection_runs_latest')} WHERE scope_id = :scope OR startswith(scope_id, concat(:scope, ':')) ORDER BY run_started_at", {"scope": scope})
    results["snapshot_counts_by_run"] = sql_rows(f"SELECT scope_id, run_id, collection_mode, collection_status, COUNT(*) AS row_count, SUM(total_bytes) AS total_bytes FROM {fq('table_storage_snapshots')} WHERE scope_id = :scope OR startswith(scope_id, concat(:scope, ':')) GROUP BY ALL ORDER BY run_id, collection_status", {"scope": scope})
    results["monitoring_schema"] = {name: spark.table(fq(name)).schema.jsonValue() for name in ("storage_collection_runs", "table_storage_snapshots", "storage_collection_scope_health", "table_storage_current")}
except Exception as exc:
    record_error("final_evidence_query", exc, "FAIL")

if any(item["result"] == "FAIL" for item in checks):
    results["overall_result"] = "FAIL"
elif not full_available:
    results["overall_result"] = "INCONCLUSIVE_FULL_STORAGE"
elif any(item["result"] == "INCONCLUSIVE" for item in checks):
    results["overall_result"] = "PARTIAL"
else:
    results["overall_result"] = "PASS"
results["finished_at"] = datetime.now(timezone.utc).isoformat()
print(json.dumps(serializable(results), indent=2))
dbutils.notebook.exit(json.dumps(serializable(results), sort_keys=True))
