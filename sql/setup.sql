-- Replace YOUR_TARGET_CATALOG and YOUR_TARGET_SCHEMA with your target identifiers.
-- The collector executes this same bootstrap DDL automatically. Catalog must already exist.
-- Existing tables/views are preserved, including reporting-view identities and grants.
-- Apply reviewed view-definition upgrades explicitly with ALTER VIEW ... AS; recurring collection never replaces them.
-- Run one statement at a time if your SQL editor does not support multiple statements.

CREATE SCHEMA IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`;

CREATE TABLE IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.`storage_collection_runs` (
  `scope_id` string,
  `run_id` string,
  `event_sequence` int,
  `event_at` timestamp,
  `run_started_at` timestamp,
  `run_finished_at` timestamp,
  `run_status` string,
  `collection_mode` string,
  `collection_method` string,
  `source_catalog` string,
  `source_schema` string,
  `account_id` string,
  `metastore_id` string,
  `workspace_id` string,
  `run_as_principal` string,
  `runtime_json` string,
  `scope_definition_json` string,
  `inventory_mode` string,
  `inventory_completed` boolean,
  `visible_object_count` bigint,
  `inventory_count` bigint,
  `success_count` bigint,
  `error_count` bigint,
  `error_type` string,
  `error_message` string
) USING DELTA COMMENT 'Append-only storage-collection run events; RUNNING followed by a terminal event.';

CREATE TABLE IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.`table_storage_snapshots` (
  `scope_id` string,
  `run_id` string,
  `collection_mode` string,
  `collection_method` string,
  `account_id` string,
  `metastore_id` string,
  `table_catalog` string,
  `table_schema` string,
  `table_name` string,
  `table_type` string,
  `data_source_format` string,
  `table_id` string,
  `collection_started_at` timestamp,
  `collection_finished_at` timestamp,
  `observed_at` timestamp,
  `collection_status` string,
  `total_bytes` bigint,
  `active_bytes` bigint,
  `time_travel_bytes` bigint,
  `vacuumable_bytes` bigint,
  `num_total_files` bigint,
  `num_active_files` bigint,
  `num_time_travel_files` bigint,
  `num_vacuumable_files` bigint,
  `raw_metrics_json` string,
  `metadata_warning` string,
  `error_type` string,
  `error_message` string
) USING DELTA COMMENT 'Append-only per-table observations; failed metrics are NULL, never zero.';

CREATE VIEW IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_runs_latest AS
SELECT * EXCEPT (_rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY scope_id, collection_mode, run_id
    ORDER BY event_sequence DESC, event_at DESC
  ) AS _rn
  FROM `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_runs
) WHERE _rn = 1;

CREATE VIEW IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_scope_latest AS
SELECT * EXCEPT (_rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY scope_id, collection_mode
    ORDER BY run_started_at DESC, run_id DESC
  ) AS _rn
  FROM `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_runs_latest
  WHERE run_status IN ('COMPLETE', 'PARTIAL', 'FAILED')
) WHERE _rn = 1;

CREATE VIEW IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_scope_health AS
WITH attempts AS (
  SELECT * EXCEPT (_rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY scope_id, collection_mode
      ORDER BY run_started_at DESC, run_id DESC
    ) AS _rn
    FROM `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_runs_latest
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
LEFT JOIN `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_scope_latest c
  ON a.scope_id = c.scope_id AND a.collection_mode = c.collection_mode;

CREATE VIEW IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.table_storage_current AS
SELECT s.*, r.run_status, r.run_started_at, r.run_finished_at,
       r.inventory_count, r.success_count, r.error_count
FROM `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.table_storage_snapshots s
JOIN `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.storage_collection_scope_latest r
  ON s.scope_id = r.scope_id AND s.collection_mode = r.collection_mode AND s.run_id = r.run_id;

CREATE VIEW IF NOT EXISTS `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.table_storage_collection_errors AS
SELECT * FROM `YOUR_TARGET_CATALOG`.`YOUR_TARGET_SCHEMA`.table_storage_current WHERE collection_status = 'ERROR';
