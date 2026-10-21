-- Use the monitoring catalog/schema as the SQL editor or dashboard dataset context.
-- Bind :scope_id to the stable scope configured on the collector.
-- These datasets deliberately report full_storage only.

-- dataset: storage_summary | Current storage summary
WITH health AS (
  SELECT * FROM storage_collection_scope_health
  WHERE scope_id = :scope_id AND collection_mode = 'full_storage'
), observed AS (
  SELECT run_id, COUNT(*) AS recorded_count,
         COUNT_IF(collection_status = 'SUCCESS' AND total_bytes IS NOT NULL) AS measured_count,
         SUM(CASE WHEN collection_status = 'SUCCESS' THEN total_bytes END) AS measured_total_bytes
  FROM table_storage_current
  WHERE scope_id = :scope_id AND collection_mode = 'full_storage'
  GROUP BY run_id
)
SELECT requested.scope_id,
       h.current_run_id,
       h.current_run_status,
       h.current_inventory_count AS inventory_count,
       COALESCE(o.measured_count, 0) AS measured_count,
       h.current_error_count AS error_count,
       CASE WHEN h.current_run_status = 'COMPLETE'
                  AND h.current_inventory_count > 0
                  AND h.current_error_count = 0
                  AND h.current_success_count = h.current_inventory_count
                  AND o.recorded_count = h.current_inventory_count
                  AND o.measured_count = h.current_inventory_count
            THEN o.measured_total_bytes END AS complete_total_bytes,
       CASE WHEN h.current_run_status = 'COMPLETE'
                  AND h.current_inventory_count > 0
                  AND h.current_error_count = 0
                  AND h.current_success_count = h.current_inventory_count
                  AND o.recorded_count = h.current_inventory_count
                  AND o.measured_count = h.current_inventory_count
            THEN o.measured_total_bytes / 1073741824.0 END AS complete_total_gib,
       o.measured_total_bytes AS measured_subset_bytes,
       h.current_run_finished_at
FROM (SELECT :scope_id AS scope_id) requested
LEFT JOIN health h ON requested.scope_id = h.scope_id
LEFT JOIN observed o ON h.current_run_id = o.run_id;

-- dataset: storage_health | Collection health and coverage
WITH observed AS (
  SELECT run_id, MIN(observed_at) AS oldest_observation_at
  FROM table_storage_current
  WHERE scope_id = :scope_id AND collection_mode = 'full_storage'
    AND collection_status = 'SUCCESS'
  GROUP BY run_id
)
SELECT requested.scope_id,
       CASE WHEN h.run_id IS NULL THEN 'NO_COLLECTION'
            WHEN h.run_status = 'RUNNING' THEN 'RUNNING'
            WHEN h.current_run_status = 'FAILED' THEN 'FAILED'
            WHEN h.current_inventory_count = 0 THEN 'EMPTY_INVENTORY'
            WHEN h.current_run_status = 'PARTIAL' THEN 'PARTIAL'
            ELSE h.current_run_status END AS collection_state,
       h.run_id AS latest_attempt_id,
       h.run_status AS latest_attempt_status,
       h.run_started_at AS latest_attempt_started_at,
       h.current_run_id,
       h.current_run_status,
       h.current_run_finished_at,
       h.current_inventory_count AS inventory_count,
       h.current_success_count AS success_count,
       h.current_error_count AS error_count,
       CASE WHEN h.current_inventory_count > 0
            THEN h.current_success_count * 1.0 / h.current_inventory_count END AS measured_fraction,
       CASE WHEN h.current_run_status IN ('COMPLETE', 'PARTIAL')
            THEN TIMESTAMPDIFF(SECOND, o.oldest_observation_at, CURRENT_TIMESTAMP()) / 3600.0 END AS age_hours,
       CASE WHEN h.current_run_finished_at IS NULL THEN 'NO_REPORTING_RUN'
            WHEN h.current_run_status = 'FAILED' THEN 'FAILED_COLLECTION'
            WHEN o.oldest_observation_at IS NULL THEN 'NO_MEASUREMENTS'
            WHEN o.oldest_observation_at < TIMESTAMPADD(HOUR, -26, CURRENT_TIMESTAMP()) THEN 'STALE'
            ELSE 'FRESH' END AS freshness_state,
       o.oldest_observation_at,
       h.inventory_completed AS latest_inventory_completed,
       h.account_id, h.metastore_id,
       h.error_type, h.error_message
FROM (SELECT :scope_id AS scope_id) requested
LEFT JOIN storage_collection_scope_health h
  ON requested.scope_id = h.scope_id AND h.collection_mode = 'full_storage'
LEFT JOIN observed o ON h.current_run_id = o.run_id;

-- dataset: storage_details | Latest table observations
SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_name,
       table_id, table_type, total_bytes,
       total_bytes / 1073741824.0 AS total_gib,
       CASE WHEN total_bytes IS NULL THEN 'Unavailable'
            WHEN total_bytes >= 1152921504606846976 THEN CONCAT(FORMAT_NUMBER(total_bytes / 1152921504606846976.0, 2), ' EiB')
            WHEN total_bytes >= 1125899906842624 THEN CONCAT(FORMAT_NUMBER(total_bytes / 1125899906842624.0, 2), ' PiB')
            WHEN total_bytes >= 1099511627776 THEN CONCAT(FORMAT_NUMBER(total_bytes / 1099511627776.0, 2), ' TiB')
            WHEN total_bytes >= 1073741824 THEN CONCAT(FORMAT_NUMBER(total_bytes / 1073741824.0, 2), ' GiB')
            WHEN total_bytes >= 1048576 THEN CONCAT(FORMAT_NUMBER(total_bytes / 1048576.0, 2), ' MiB')
            WHEN total_bytes >= 1024 THEN CONCAT(FORMAT_NUMBER(total_bytes / 1024.0, 2), ' KiB')
            ELSE CONCAT(CAST(total_bytes AS STRING), ' B') END AS total_storage,
       active_bytes, time_travel_bytes, vacuumable_bytes,
       total_bytes - active_bytes - time_travel_bytes - vacuumable_bytes AS log_bytes,
       num_total_files, observed_at, collection_status,
       run_status, run_id, error_type, error_message
FROM table_storage_current
WHERE scope_id = :scope_id AND collection_mode = 'full_storage'
ORDER BY total_bytes DESC NULLS LAST, table_name;

-- dataset: storage_top_tables | Ten largest measured tables
SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_name,
       total_bytes / 1073741824.0 AS total_gib,
       active_bytes / 1073741824.0 AS active_gib,
       observed_at
FROM table_storage_current
WHERE scope_id = :scope_id AND collection_mode = 'full_storage'
  AND collection_status = 'SUCCESS' AND total_bytes IS NOT NULL
ORDER BY total_bytes DESC, table_name
LIMIT 10;

-- dataset: storage_history | Storage per complete collection run
WITH completed_runs AS (
  SELECT * FROM storage_collection_runs_latest
  WHERE scope_id = :scope_id AND collection_mode = 'full_storage'
    AND run_status = 'COMPLETE' AND inventory_count > 0
    AND error_count = 0 AND success_count = inventory_count
), measured_runs AS (
  SELECT r.run_id, r.run_finished_at, r.inventory_count,
         COUNT(*) AS recorded_count,
         COUNT_IF(s.collection_status = 'SUCCESS' AND s.total_bytes IS NOT NULL) AS measured_count,
         SUM(CASE WHEN s.collection_status = 'SUCCESS' THEN s.total_bytes END) AS total_bytes,
         SUM(CASE WHEN s.collection_status = 'SUCCESS' THEN s.active_bytes END) AS active_bytes
  FROM completed_runs r
  JOIN table_storage_snapshots s
    ON r.scope_id = s.scope_id AND r.collection_mode = s.collection_mode AND r.run_id = s.run_id
  GROUP BY r.run_id, r.run_finished_at, r.inventory_count
)
SELECT run_id, run_finished_at,
       total_bytes / 1073741824.0 AS total_gib,
       active_bytes / 1073741824.0 AS active_gib,
       inventory_count
FROM measured_runs
WHERE recorded_count = inventory_count AND measured_count = inventory_count
ORDER BY run_finished_at;
