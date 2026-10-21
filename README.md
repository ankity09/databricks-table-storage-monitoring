# Databricks table storage monitoring

Collect the bytes occupied by each Unity Catalog Delta table, retain measurements in Delta, and display them in an AI/BI dashboard. Prepared for F5 engineers; all workspace, catalog, and identity settings belong to your deployment.

The recommended collector uses `ANALYZE TABLE … COMPUTE STORAGE METRICS` on supported notebook compute. `information_schema` supplies the table inventory. The dashboard runs `SELECT` queries against saved observations.

Start with the [implementation guide](implementation_guide.md) for metric definitions, permissions, operating guidance, and acceptance checks.

## Requirements

- An AWS Databricks workspace with Unity Catalog and supported notebook compute. The public storage command requires Databricks Runtime 18.0+. The supplied job uses serverless; validate command support in your workspace first.
- An existing source catalog/schema, an approved output catalog, and a dedicated output schema. The collecting identity must see and measure the intended tables and create/read/write the output objects.
- A SQL warehouse for dashboard queries.
- Python 3.9+ and the current Databricks CLI for the deployment helper. It uses the Python standard library; no local Spark installation is needed to deploy or run the notebooks. CLI commands and flags were checked with CLI 1.21.0.

Coverage is limited to visible UC **managed/external Delta tables**. Views, shallow clones, materialized views, streaming tables, Iceberg, and the Hive metastore need separate validation. Inventory is filtered by the collecting identity's privileges.

## Quick start

### 1. Clone and select your workspace

```bash
git clone https://github.com/ankity09/databricks-table-storage-monitoring.git
cd databricks-table-storage-monitoring
databricks --version
databricks auth login --host https://YOUR_WORKSPACE_HOST --profile F5_PROFILE
databricks current-user me --profile F5_PROFILE
```

Replace `YOUR_…` values in the commands below. `F5_PROFILE` is the local profile name you choose for your workspace. Every workspace command requires it explicitly.

If authentication behaves differently in the helper, check `command -v databricks` and `databricks --version` in that same shell. Use one current CLI installation consistently for login and deployment; older installations can use different credential storage.

Using the intended job identity and compute, run the [single-table pilot](implementation_guide.md#3-validate-one-table-first). Confirm that `ANALYZE TABLE … COMPUTE STORAGE METRICS` returns all eight metrics before continuing.

### 2. Import the collector

```bash
databricks workspace mkdirs /Workspace/Shared/table-storage-monitoring --profile F5_PROFILE
databricks workspace import /Workspace/Shared/table-storage-monitoring/collector \
  --file collector.py --format SOURCE --language PYTHON --profile F5_PROFILE
```

For an existing installation, review the changes before updating its notebook. You can choose a different folder; set the same absolute notebook path in the job configuration.

### 3. Configure a paused job

```bash
cp job.template.json job.local.json
```

Edit `job.local.json`:

- Replace **every** `REPLACE_WITH_…` value: unique scope, source catalog/schema, output catalog/schema, account ID, and metastore ID. Use real governance IDs for consolidated reporting. `databricks metastores current --profile F5_PROFILE` returns the workspace's metastore assignment; obtain the account ID from your platform administrator.
- Confirm `notebook_path` matches the imported notebook.
- For the pilot, set `explicit_tables_json` to a JSON-encoded small allowlist such as `"[\"table_a\", \"table_b\"]"`. Leave it `""` for automatic discovery after acceptance. Each entry is a table name within the configured source schema.
- Keep `collection_mode` as `full_storage` and the schedule `PAUSED`. Set the production run-as service principal and failure notifications before enabling recurring collection; the initial job otherwise runs as its creator.

The template uses serverless environment client `4`, maximum concurrent runs `1`, and a six-hour job timeout. Review compute availability, permissions, runtime support, and timeout for your environment. For classic compute, follow the [job configuration notes](implementation_guide.md#7-schedule-and-operate).

### 4. Run once and inspect the saved metrics

```bash
databricks jobs create --json @job.local.json --profile F5_PROFILE
databricks jobs run-now YOUR_JOB_ID --no-wait --profile F5_PROFILE
databricks jobs get-run YOUR_RUN_ID --profile F5_PROFILE
```

Use the job ID from `jobs create` and the run ID from `jobs run-now`. Wait for a successful terminal run in the Jobs UI or with `get-run`; job creation alone does not collect metrics. The collector bootstraps its two Delta tables and five reporting views. `setup.sql` is the equivalent optional bootstrap, not a second required step.

In the SQL editor, select the output catalog/schema and check:

```sql
SELECT scope_id, current_run_status, current_inventory_count,
       current_success_count, current_error_count, current_run_finished_at
FROM storage_collection_scope_health
WHERE scope_id = 'YOUR_UNIQUE_SCOPE_ID' AND collection_mode = 'full_storage';

SELECT table_catalog, table_schema, table_name, total_bytes, active_bytes,
       time_travel_bytes, vacuumable_bytes, collection_status, error_message
FROM table_storage_current
WHERE scope_id = 'YOUR_UNIQUE_SCOPE_ID' AND collection_mode = 'full_storage'
ORDER BY total_bytes DESC NULLS LAST;
```

Require `COMPLETE`, the expected table count, and zero errors. An empty or privilege-limited inventory is not proof of zero storage.

### 5. Validate and create the dashboard

```bash
python3 deploy_dashboard.py \
  --profile F5_PROFILE --warehouse-id YOUR_SQL_WAREHOUSE_ID \
  --catalog YOUR_OUTPUT_CATALOG --schema YOUR_OUTPUT_SCHEMA \
  --scope-id YOUR_UNIQUE_SCOPE_ID --create
```

The helper executes all five datasets and creates a **draft** only when they succeed. Omit `--create` to validate SQL without creating a dashboard. Later use `--update YOUR_DASHBOARD_ID` to update the same draft. Results are written to the ignored local `dashboard_validation.json`.

Review coverage, freshness, and figures in your authenticated UI, then publish the dashboard with your chosen data credentials and sharing settings. Enable the collection job's paused schedule only after the [acceptance checks](implementation_guide.md#9-acceptance-checklist) pass. Schedule dashboard refresh after collection normally completes; refreshing a dashboard does not measure storage.

For UI import instead, generate your scope-specific JSON and import it from the Dashboards page. Select the output catalog/schema and your SQL warehouse:

```bash
python3 build_dashboard.py --scope-id YOUR_UNIQUE_SCOPE_ID \
  --output table_storage.local.lvdash.json
```

## Reading the dashboard

The headline shows raw total **bytes** for a nonempty, complete reporting run. Table rows also show readable binary units such as `383.86 KiB`; raw byte values remain available. Numeric GiB columns remain in the SQL datasets for further analysis.

This avoids the earlier `0.00 GiB` display for small positive tables: 393,072 bytes is only 0.000366077 GiB. Unknown measurements display as unavailable. Partial/failed/empty runs never produce a complete zero-byte total, and measurement age is visible.

The default freshness threshold is 26 hours, defined in `dashboard_queries.sql`. Adjust it to your collection cadence and regenerate/redeploy the dashboard after changing the query.

`DESCRIBE DETAIL.sizeInBytes` measures the active snapshot. The optional `active_snapshot` collector mode leaves full-storage fields `NULL`; it needs separate dashboard datasets. Do not mix it into the supplied full-storage dashboard or sum snapshots across dates.

## Files

| Resource | Purpose |
|---|---|
| [implementation_guide.md](implementation_guide.md) | Full implementation and operating guide, validation record, limitations, public references |
| [collector.py](collector.py) | Importable production notebook; bounded discovery, storage metrics, errors, append-only history |
| [setup.sql](setup.sql) | Optional bootstrap definitions matching the collector |
| [job.template.json](job.template.json) | Paused daily serverless job template |
| [dashboard_queries.sql](dashboard_queries.sql) | Five read-only datasets |
| [build_dashboard.py](build_dashboard.py) | Offline dashboard generator |
| [deploy_dashboard.py](deploy_dashboard.py) | SQL validation and optional draft dashboard create/update |
| [table_storage.lvdash.json](table_storage.lvdash.json) | Portable dashboard template; generate your exact scope before import |
| [TEST_PLAN.md](TEST_PLAN.md) | Synthetic test steps and actual validation scope |
| [test_harness.py](test_harness.py) | Separate notebook creating isolated fixtures in a new test schema |
| [external_reconciliation.py](external_reconciliation.py) | Optional bounded file listing for the owned external fixture only |
| [test_local_contract.py](test_local_contract.py), [test_local_dashboard.py](test_local_dashboard.py) | Offline collector and dashboard contract tests |

## Validation

The isolated demo test passed **20/20 live collector checks** on observed serverless Runtime 19.11 Photon / Spark 4.2.0. An external fixture independently reconciled to 60,565 bytes and 10 files. All five dashboard datasets were exercised across complete and negative scopes. A separate run of the supplied paused job template bootstrapped a new monitoring schema and completed a one-table collection with zero errors; the temporary job was then deleted. See the guide for exact coverage and untested cases.

Offline tests run with Python and PySpark; they do not start Spark or contact a workspace:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
python3 -m unittest discover -p 'test_local_*.py' -v
```

GitHub Actions runs these offline checks without Databricks credentials. The notebooks `test_harness.py` and `external_reconciliation.py` run inside Databricks, not as local tests. Operational evidence, local job configuration, credentials, and validation output are ignored by Git.

This is a reference implementation. Validate production identity permissions, runtime support, large-estate collection duration, overlapping external paths, and operational ownership in your environment. The measurements describe table files in cloud object storage; they do not replace cloud-invoice reconciliation.

## License

[MIT](LICENSE).
