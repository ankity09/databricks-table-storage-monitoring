# F5 — Monitor table storage in Databricks

<!-- header F5 | Table storage monitoring -->
<!-- footer Implementation guide | 20 October 2026 -->
<!-- toc -->

Implementation guide · 20 October 2026

**Validated on the F5 demo workspace using isolated synthetic tables.**

Implementation: [repository and quick start](README.md). All collector, SQL, job template, dashboard, and validation resources are included in this repository.

## Recommended approach

Run `ANALYZE TABLE … COMPUTE STORAGE METRICS` in a scheduled Databricks notebook, save each collection to Delta tables, and use `SELECT` queries over reporting views in an AI/BI dashboard. This provides table-level bytes, a storage breakdown, collection status, and history.

`system.information_schema.tables` supplies the table inventory; its documented schema does not contain a table-size column. The storage command computes metrics when invoked and does not persist them in Unity Catalog. Databricks explicitly recommends periodically collecting and saving its results. [Storage metrics documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-compute-storage-metrics) · [Information schema reference](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/tables)

The dashboard reads the saved results. Its refresh reruns those queries; the collection job is responsible for measuring storage. AI/BI datasets cannot use `DESCRIBE` commands directly. [Dashboard datasets](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/datasets)

```text
Unity Catalog table inventory
    → scheduled storage-metrics notebook
    → Delta observations + collection-run records
    → reporting views
    → AI/BI dashboard
```

## 1. Agree on the storage measure

For the PM question “How many bytes does each table occupy?”, use `total_bytes` as the headline and show the breakdown alongside it.

| Field | Meaning |
|---|---|
| `total_bytes` | Active files, retained files, and transaction logs attributed to the table. |
| `active_bytes` | Data files in the current table snapshot. |
| `time_travel_bytes` | Retained files needed for time travel under the table’s retention settings. |
| `vacuumable_bytes` | Files eligible for removal by supported cleanup operations. |
| `num_*_files` | File counts corresponding to each storage category. |
| `observed_at` | When the collector finished the measurement for that table. |

Transaction-log bytes are included in `total_bytes`. They are the residual after subtracting the other three byte categories; there is no separate log-byte metric in the command’s eight returned metrics.

F5’s existing `DESCRIBE DETAIL.sizeInBytes` answers the current-snapshot question. It does not provide the full retained-storage figure. The supplied collector has an explicit `active_snapshot` mode for that use case; in that mode, total and retained bytes remain `NULL`. It never silently substitutes active bytes for total bytes. [Table details](https://docs.databricks.com/aws/en/tables/operations/table-details)

Keep byte values as `BIGINT`. Display GiB as `bytes / 1073741824.0`; one GiB is 1,073,741,824 bytes. These measurements describe table storage in cloud object storage. They are not worker-local disk usage or a complete cloud storage invoice.

## 2. Prerequisites and scope

Use an AWS Databricks workspace with Unity Catalog and notebook compute that supports the command (**Databricks Runtime 18.0 or later**). The implementation uses serverless jobs by default. Validate the command on one table using F5’s selected serverless environment before scheduling the whole scope. Classic Runtime 18.0+ is an alternative when serverless is unavailable or F5 requires a specific runtime.

The supplied implementation initially discovers **Unity Catalog managed and external Delta tables**. It excludes views, foreign tables, materialized views, streaming tables, and shallow clones. The underlying storage command supports additional table types, but those require separate rollout validation. `system.information_schema` does not inventory the legacy Hive metastore.

Use an existing catalog and a dedicated output schema, for example `operations.table_storage_monitoring`. Replace those example names with F5-approved names throughout the deployment.

The collecting principal needs:

- Access to the chosen job compute and permission to execute the imported notebook.
- `USE CATALOG` and `USE SCHEMA` on the source objects, visibility of the expected inventory, and permission to execute the storage command on every selected table.
- Access to the output catalog/schema and permission to create the output tables/views initially and read/write the collector tables on later runs. Creating views requires `CREATE TABLE` on the output schema.

Run the first pilot as a table owner or an appropriately authorized platform principal, then validate F5’s production service principal with the same single-table command and inventory query. A successful owner-run test does not establish minimum reader privileges. Configure job failure notifications for the production owner.

Dashboard viewers need the appropriate dashboard access and access to its reporting data under F5’s selected dashboard credential model. Grant access only to the reporting schema/views required for their role. [Dashboard sharing and permissions](https://docs.databricks.com/aws/en/dashboards/share/)

For F5’s multiple accounts, regions, and metastores, deploy a collector in each accessible governance boundary. Assign a stable, unique `scope_id`, such as `aws-prod-us-east-1-metastoreA-prod-security`, and retain the actual `account_id` and `metastore_id` in the configuration. For consolidation, set those identifiers explicitly using one consistent format. Do not use the same scope identifier for independent inventories or change an existing scope’s definition without assigning a new identifier.

## 3. Validate one table first

Run this on a small representative table using the same identity and compute planned for the scheduled job:

```sql
ANALYZE TABLE `source_catalog`.`source_schema`.`example_table`
COMPUTE STORAGE METRICS;
```

Expect eight metric rows: `total_bytes`, `active_bytes`, `time_travel_bytes`, `vacuumable_bytes`, and the four corresponding `num_*_files` metrics. The result columns are `metric_name`, `metric_value`, and `metric_description`.

Compare the current-snapshot size for a normal Delta table:

```sql
DESCRIBE DETAIL `source_catalog`.`source_schema`.`example_table`;
```

For a stable table without concurrent writes, its `sizeInBytes` should agree with the storage command’s `active_bytes`. `total_bytes` can be larger because retained files and transaction logs occupy storage too. Do not require those two different measures to match.

If the storage command is unavailable, verify the runtime and table support before rollout. Choose `active_snapshot` only if the PMs explicitly accept active-snapshot size; keep its reporting separate from full storage.

## 4. Import and configure the collector

Import `collector.py` from this repository as a Python notebook. Create the approved output schema first. The notebook creates the required tables and reporting views; `setup.sql` contains the same definitions for review or a separate bootstrap process.

Bootstrap uses `CREATE … IF NOT EXISTS`. Recurring collections do not replace the reporting views. Apply future view-definition upgrades with reviewed `ALTER VIEW … AS` statements so existing grants and view identity are preserved. [CREATE VIEW behavior](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-view)

In the Workspace browser, create a dedicated folder, select **Import**, and choose the source file. For a CLI installation, run these commands from the cloned repository directory with F5’s intended profile:

```bash
databricks workspace mkdirs /Workspace/Shared/table-storage-monitoring --profile F5_PROFILE
databricks workspace import /Workspace/Shared/table-storage-monitoring/collector \
  --file collector.py --format SOURCE --language PYTHON --profile F5_PROFILE
```

Set the notebook widgets/job parameters:

| Parameter | Example / purpose |
|---|---|
| `scope_id` | A stable, unique scope identifier. Required. |
| `source_catalog` | One source catalog, for example `prod`. Required. |
| `source_schema` | One source schema for the pilot. Use `*` only for a deliberate catalog-wide rollout. |
| `target_catalog` | Existing catalog for monitoring output, for example `operations`. |
| `target_schema` | Dedicated output schema, for example `table_storage_monitoring`. |
| `collection_mode` | `full_storage` for total bytes; `active_snapshot` is an explicit alternative. |
| `account_id`, `metastore_id` | Actual governance identifiers for attribution and later consolidation. |
| `max_tables` | Safety ceiling, default `10000`. Exceeding it fails discovery rather than truncating the inventory. |
| `batch_size` | Number of observations per Delta write, default `100`. |
| `explicit_tables_json` | Optional JSON array of table names within one source schema, for example `["table_a", "table_b"]`. Leave blank for discovery. |
| `exit_on_error` | `true` for production so collection errors are visible as job failures. |

Review the discovered inventory before the first catalog-wide run:

```sql
SELECT table_catalog, table_schema, table_name,
       table_type, data_source_format
FROM system.information_schema.tables
WHERE table_catalog = 'source_catalog'
  AND table_schema = 'source_schema'
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND upper(data_source_format) = 'DELTA'
ORDER BY table_schema, table_name;
```

The inventory is filtered by the caller’s permissions. Reconcile its count with F5’s expected table list; a successful query alone does not prove estate-wide coverage. The collector excludes its own monitoring tables and views from discovery.

Start with a small allowlist, run the notebook manually, inspect the output, then expand the scope. An allowlist is still restricted to the configured source catalog/schema.

## 5. Understand the saved results

| Object | Purpose |
|---|---|
| `table_storage_snapshots` | Append-only table observations, raw metrics, identities, timestamps, and per-table errors. |
| `storage_collection_runs` | Run events, discovered/succeeded/failed counts, method, and run status. |
| `storage_collection_runs_latest` | The latest event for each collection run. |
| `storage_collection_scope_latest` | The latest terminal run for each scope and collection mode. |
| `table_storage_current` | Observations from that exact latest terminal run, including failed table measurements. |
| `table_storage_collection_errors` | Per-table measurement errors. |
| `storage_collection_scope_health` | Latest-attempt and reporting-run health, including failed discovery and collection freshness. |

A failed measurement has `NULL` metrics and an explicit error; it never becomes zero bytes. A partially successful run is `PARTIAL`, an all-error run is `FAILED`, and successful collection is `COMPLETE`. A failed discovery cannot make an old successful run appear current. Batches written by an unfinished run are retained for diagnosis but do not become the current reporting snapshot.

Empty inventory needs investigation against the expected scope and privileges. Do not interpret an empty result as proof that F5 stores zero bytes.

Each run produces a new observation set, so retrying the job retains history. Current reporting selects one run; it does not add multiple retries or snapshot dates together. Collection observes tables sequentially, so a run is not one atomic, estate-wide storage snapshot.

The saved `table_id` is the Delta table ID returned by `DESCRIBE DETAIL`, rather than the Unity Catalog REST object ID. Keep catalog/schema/name and governance IDs alongside it. Re-registering the same external Delta location can retain that Delta ID.

## 6. Create the AI/BI dashboard

Create a dashboard backed by the output catalog/schema. Use only read-only `SELECT` datasets. The package includes `dashboard_queries.sql` and a dashboard builder/template for the following views:

- Table detail: table name, readable total storage, raw total bytes, active bytes, retained bytes, observation time, and collection status. Numeric GiB is retained in the dataset.
- Largest tables: the ten largest successfully measured tables in the current scope.
- Collection health: latest attempt, run status, inventory count, errors, and measurement age.
- History: one total per completed collection run, with no summing across observations.

Use one explicit scope and collection mode per dashboard. Show the health section with the storage figures, including partial coverage. Do not present a partial sum as a complete estate total. Display unknown totals as unavailable.

The supplied dashboard headline uses total **bytes**. Its table formats positive values with binary units (B through EiB), while retaining numeric bytes and GiB in the dataset for calculations. For example, 393,072 bytes displays as `383.86 KiB`; rounded to two decimal places in GiB it would appear as `0.00`. NULL measurements display as unavailable.

The supplied dashboard builder and deployment helper target `full_storage` only. If F5 chooses `active_snapshot`, build separate datasets showing `active_bytes` and `num_active_files`, and adapt coverage checks to those fields; the full-storage headline intentionally remains unavailable for that mode.

The simplest table dataset is:

```sql
SELECT table_catalog, table_schema, table_name,
       total_bytes,
       total_bytes / 1073741824.0 AS total_gib,
       active_bytes, time_travel_bytes, vacuumable_bytes,
       observed_at, collection_status, error_message
FROM operations.table_storage_monitoring.table_storage_current
WHERE scope_id = 'YOUR_UNIQUE_SCOPE_ID'
  AND collection_mode = 'full_storage'
ORDER BY total_bytes DESC NULLS LAST;
```

Use the complete package queries for aggregate figures and health. When importing the portable dashboard template, set its dataset catalog/schema to the output schema and its scope to the configured `scope_id`.

The package provides two deployment options. To import through the UI, first generate the file for the exact collector scope, then import that file from the Dashboards page and select the reporting catalog/schema and SQL warehouse:

```bash
python3 build_dashboard.py --scope-id YOUR_UNIQUE_SCOPE_ID \
  --output table_storage.local.lvdash.json
```

For repeatable CLI deployment, use Python 3.9 or later and the current Databricks CLI. This command executes all five datasets on the specified warehouse before creating a draft; it stops if any query fails:

```bash
python3 deploy_dashboard.py \
  --profile F5_PROFILE --warehouse-id YOUR_SQL_WAREHOUSE_ID \
  --catalog operations --schema table_storage_monitoring \
  --scope-id YOUR_UNIQUE_SCOPE_ID --create
```

Omit `--create` for SQL validation only. Use `--update EXISTING_DASHBOARD_ID` to update the same draft later. Review the dashboard in the UI, verify the health and table figures, then publish it under F5’s chosen sharing/credential settings. The deployment helper writes `dashboard_validation.json` containing each statement’s outcome and the dashboard ID.

Dashboard refresh reads the most recently saved observations. Schedule it after the collection normally completes and expose freshness so an overrun remains visible. A scheduled dashboard refresh by itself does not run the storage collector. [Dashboard schedules](https://docs.databricks.com/aws/en/dashboards/share/schedule-subscribe)

## 7. Schedule and operate

Create a Lakeflow Job for the collector notebook using the reviewed runtime and production service principal. Set maximum concurrent runs to `1`. Start with a daily off-peak schedule and notifications on failures; tune the cadence after measuring the pilot’s duration. Keep schedules paused during setup and enable them only after the acceptance checks pass.

Alternatively, copy `job.template.json` to the ignored `job.local.json`, edit every `REPLACE_WITH_…` value, confirm the imported notebook path, and create the job with:

```bash
databricks jobs create --json @job.local.json --profile F5_PROFILE
```

The template selects serverless compute with environment client `4`, matching the live test configuration, and keeps its daily schedule paused. Its `STANDARD` performance target suits a daily batch without a tight startup deadline. A serverless environment client is not a pinned Databricks Runtime version; confirm command support in F5’s workspace. Review the environment in the Jobs UI, and configure the production run-as principal and failure notifications before enabling the schedule.

If using approved classic Runtime 18.0+ instead, remove `environment_key` from the task and remove the top-level `environments` and `performance_target` settings, then add `existing_cluster_id` for that compatible compute or select approved job compute in the UI. The inventory-backed option below requires a separate classic configuration.

The default command recursively lists table files. Large tables can take minutes to hours. Restrict the initial scope, avoid launching many collectors against the same storage simultaneously, and set a job timeout appropriate to the measured runtime. Persisted metrics keep those storage listings out of interactive dashboard refreshes.

Keep observation history according to F5’s monitoring retention policy. Do not run `VACUUM` simply because the dashboard reports vacuumable bytes; cleanup must respect F5’s retention and recovery requirements.

## 8. Optional scaling with cloud inventory

For tables with at least 100,000 files or heavy multi-key partitioning, Databricks recommends the inventory-backed command. This is a separate rollout path requiring **Databricks Runtime 19 or later on classic compute**; the inventory clause is not supported on serverless compute or SQL warehouses.

```sql
ANALYZE TABLE `source_catalog`.`source_schema`.`example_table`
COMPUTE STORAGE METRICS
USING INVENTORY LOCATION 's3://inventory-destination/prefix/'
CONF 'inventory-configuration-name';
```

Configure a daily Parquet S3 Inventory report using current object versions and including size and last-modified fields. Register its destination under an accessible external location and grant the collecting principal `READ FILES` there, in addition to the required table access. The inventory configuration name and destination must match the source bucket’s inventory configuration.

Metrics reflect the inventory’s observation time, often up to 24 hours old; report that age separately from the notebook execution time. The first S3 Inventory report can take up to 48 hours. Source-bucket versioning has correctness limitations documented by Databricks. Check the public prerequisites before adopting this option. [Inventory requirements and limitations](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-compute-storage-metrics#use-an-inventory-report)

The supplied collector uses direct file listing. Inventory-backed collection must be configured and validated separately before replacing that command.

## 9. Acceptance checklist

Before handing the dashboard to PMs, confirm:

1. The production job identity sees the expected source inventory and can measure a representative table.
2. Raw byte and file-count columns are `BIGINT`, and no failed collection produces zero metrics.
3. On a stable ordinary Delta table, `active_bytes` agrees with `DESCRIBE DETAIL.sizeInBytes`.
4. After an overwrite, full storage captures retained storage as well as the new active snapshot.
5. A retry adds history while the current view still returns one observation per selected table.
6. A missing/inaccessible table is visible as an error, with partial/failed collection status and honest coverage.
7. Failed discovery, unfinished runs, and empty inventory do not produce a healthy zero-byte estate report.
8. Dashboard datasets execute as `SELECT` queries over saved results, and refreshes do not launch storage listings.
9. Scope identifiers and governance IDs prevent collisions when regional/metastore outputs are consolidated.

## Validation record

Tested on 20 October 2026 Pacific time (21 October UTC) in the F5 demo AWS workspace with Unity Catalog and serverless notebook jobs. The observed runtime was **19.11 Photon**, with **Spark 4.2.0** and environment client `4`. The main harness used `PERFORMANCE_OPTIMIZED`; the independent external-file check used `STANDARD`. The collecting identity owned the test fixtures. The documented minimum is Runtime 18.0; this test did not execute an 18.0 runtime.

All **20 live collector checks passed**, together with six local collector/dashboard contract checks covering large integers, invalid metric results, identifier quoting, small-value display bindings, unusual scope strings, and the portable dashboard artifact.

| Scenario | Observed result |
|---|---|
| Managed and external Delta tables | All eight native metrics returned; active size agreed with `DESCRIBE DETAIL` on stable fixtures. |
| Retained data after overwrite | Positive time-travel bytes and files; full storage exceeded current-snapshot size. |
| Saved metric types | All eight metric columns were `BIGINT`. |
| Repeated runs and retries | History appended; current reporting selected exactly one terminal run without duplicate table rows. |
| Missing table, all-error run, and discovery guard | Errors and unknown metrics remained visible; discovery was not silently truncated. |
| Empty inventory and interrupted attempt | Old measurements did not reappear as current; latest-attempt health remained visible. The interruption check used a labelled synthetic run event and no fabricated storage observations. |
| Explicit `active_snapshot` mode | Only active bytes/file counts populated; full-storage measurements stayed separate. |
| Dashboard datasets | All five queries succeeded for the complete scope and six negative/test scopes: 35 dataset executions. Partial, failed, empty, and missing runs did not produce a complete zero-byte total. |

The final full-storage collection measured **four tables, with zero errors and 725,782 total bytes**. The history dataset returned three complete, nonempty collection points. These are synthetic test values, not measurements of F5’s production estate.

For example, one saved observation of the overwritten fixture contained:

| Storage category | Bytes |
|---|---:|
| Active snapshot | 45,441 |
| Time travel | 156,821 |
| Vacuumable | 0 |
| Transaction logs, derived residual | 35,874 |
| **Total** | **238,136** |

An independent check of the new external fixture recursively listed its files with a 1,000-file bound. The listing was unchanged before and after the native command: both methods returned **60,565 bytes across 10 files**, with zero byte or file-count difference. This separate check also confirmed command execution in serverless `STANDARD` mode. It covered one owned external fixture, without establishing bucket-wide or cloud-invoice accounting.

The draft AI/BI dashboard was created and read back through the API; its saved dataset contexts and widget-to-column bindings were checked. For the repository handoff, all five datasets were revalidated across the seven scopes after adding readable storage labels. Sixteen SQL display cases passed, covering NULL, zero, one byte, binary-unit thresholds, the synthetic table sizes, and the maximum BIGINT. Raw numeric bytes were preserved. Browser rendering was not verified because the available browser session was unauthenticated. Review the imported dashboard in F5’s authenticated UI before publishing.

The test did not validate inventory-backed collection, large-estate performance/listing cost, minimum privileges for a reader identity, positive vacuumable-byte scenarios, scheduled execution, or additional table formats. No recurring schedule was enabled. Test fixtures and the draft dashboard were retained for review.

To repeat the synthetic validation in F5’s environment, import `test_harness.py` as a separate notebook. Set `catalog` to an existing writable UC catalog and `collector_notebook_path` to the imported collector’s absolute workspace path. Leave `test_suffix` blank to generate a unique test schema. Optionally provide `external_base_url` under an existing permitted external location to include the external-table check. Run it on compatible notebook compute and inspect the returned JSON’s `overall_result` and individual checks. The harness creates new fixtures and retains them for review; it does not use the production source scope.

`external_reconciliation.py` is a separate bounded diagnostic for that new external fixture. It requires the exact expected fixture location and refuses managed tables. It is not part of the production collector.

## Accounting limits

The default inventory and collector intentionally cover a defined subset of table types. Validate Iceberg, materialized views, streaming tables, clones, and legacy Hive tables separately if F5 needs them. The public command treats shallow-clone storage specially: total bytes cover the clone’s own metadata/logs, and active bytes are zero because source data belongs to the source table.

Table-level totals also need review before estate aggregation where external tables share storage paths or inventories overlap. Across workspaces sharing one metastore, collect each logical scope once. These metrics do not replace cloud billing reconciliation for noncurrent object versions, replicas, or unrelated objects.

## Public references

[1] [Storage-metrics command, definitions, runtime requirements, and inventory option](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-compute-storage-metrics)

[2] [Databricks example multi-table collection notebook](https://docs.databricks.com/aws/en/notebooks/source/delta/analyze-compute.html)

[3] [Information schema TABLES](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/tables)

[4] [DESCRIBE DETAIL](https://docs.databricks.com/aws/en/tables/operations/table-details)

[5] [AI/BI dashboard datasets](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/datasets)

[6] [AI/BI dashboard schedules](https://docs.databricks.com/aws/en/dashboards/share/schedule-subscribe)
