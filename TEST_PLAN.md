---
customer: "F5 Inc"
type: feature-test-plan
created: "2026-10-20"
updated: "2026-10-20"
---

# Public table-storage command and dashboard collection test

The target profile must be explicitly selected by the user before any workspace operation. No preview or private system-table feature is part of this test. No AWS resources, cloud inventory, schedules, or VACUUM operations are created.

## Intended environment

- AWS workspace with Unity Catalog, a writable existing catalog, and supported compute.
- `full_storage`: Databricks Runtime 18.0 or above, validated by executing the public command. An older or incompatible serverless environment is a setup limitation, not evidence of a feature defect.
- `active_snapshot`: separately selected `DESCRIBE DETAIL` mode. It cannot establish that full-storage collection works.
- Collector identity owns the isolated test fixtures. This does not establish the minimum privileges needed by a read-only production principal.
- UC managed/external Delta tables only. Broader table formats, clones, materialized views, streaming tables, and Hive-metastore objects are outside this implementation's validation scope.

## Isolation

Create a unique `storage_metrics_test_<timestamp>_<random>` schema under an existing permitted catalog. Create only small deterministic fixtures and collector output within that schema. If a permitted external location exists, use a new unique child prefix and create a new external Delta table there. Do not access or overwrite any existing object at that prefix. Never change shared-compute settings. Only compute created for this test may be stopped or removed.

## Required checks

| Scenario | Success criterion | Failure classification |
|---|---|---|
| Environment/principal | Record observed runtime/version, Spark version, UC metastore, principal, compute type and run URL | Missing permissions/runtime/compute is setup failure |
| Baseline managed table | `DESCRIBE DETAIL` succeeds; full command returns its documented three-column schema and all eight numeric metrics | Unsupported runtime is setup failure; correct supported environment with unexpected behavior is feature failure |
| Active/full comparison | Fresh unmodified Delta fixture has `active_bytes = sizeInBytes` and matching active file count | Mismatch needs investigation; fixture excludes clone semantics |
| Retained history | After overwrite, full command reports positive time-travel bytes/files; total is at least active plus retained categories | No history or concurrent optimization must be investigated before calling feature failure |
| Metric types | Persisted eight metric columns are BIGINT; unknown/unmeasured/failed values remain NULL | Collector failure if wrong types or zero substitution |
| Repeat collection | Two terminal runs append history; current view contains exactly one latest-run row per table | Collector failure if snapshots are double counted |
| Partial errors | Existing fixtures plus a nonexistent table produce PARTIAL; missing table appears with ERROR and NULL metrics | Collector failure if silently missing or marked complete |
| All errors | Nonexistent-only scope produces FAILED and visible NULL error rows | Collector failure if successful or zero-byte total |
| Empty inventory | Explicit empty allowlist produces COMPLETE with inventory_count=0; current view is empty | Collector failure if an older scope observation reappears |
| Discovery failure/guard | A source inventory exceeding max_tables produces FAILED/inventory_completed=false; current is empty; latest attempt error is visible | Collector failure if truncated or stale data silently reused |
| Interrupted attempt | A labelled synthetic RUNNING manifest in a dedicated QA scope is visible in health while current stays on that scope's real terminal observation; closing it FAILED removes current rows | Collector failure if RUNNING hides health or incomplete rows become current |
| Output exclusion | Automatic source enumeration excludes both collector tables | Collector failure if recursive monitoring measurements appear |
| Optional external table | A new external Delta fixture returns the same full metric structure and active comparison | Unavailable external location/privileges is setup limitation |
| Bounded external reconciliation | Two unchanged listings of only the exact new external fixture prefix have total bytes and file count equal to fresh native full-storage metrics | Changed inventory is INCONCLUSIVE; a stable mismatch needs investigation; never list managed paths or a bucket root |
| Optional active mode | Only active bytes/files are populated, with full-storage/retained fields NULL; modes remain separate | Collector failure if active values are presented as total storage |

## Evidence

Store local evidence in `evidence/`:

- `environment.json`: selected profile, workspace, metastore, principal, catalog, compute/runtime metadata and validation results.
- `submit*.json`, `run*.json`, `output*.json`: exact submitted task configuration, run/task IDs, run URL, terminal state and notebook output/error.
- `results.json`: scenario results, command-result schemas, raw metric rows, retained-history comparison, current/history/health counts and fixture identities.
- `external_reconciliation_results.json`: bounded before/after listings of the isolated external fixture, fresh native metrics, differences and observed runtime.
- `cleanup.json` when cleanup occurs: only resources owned by this test, actions taken and anything deliberately retained.

Evidence must distinguish PASS, FAIL and NOT_TESTED/INCONCLUSIVE. An infrastructure/setup failure never becomes a passing full-storage test. Inventory-backed Runtime 19 classic mode remains NOT_TESTED unless a compliant inventory already exists and is explicitly in scope.

For the dashboard, execute all five datasets for complete, partial, all-error, empty, discovery-failed, interrupted, and never-run scopes. Complete totals must be unavailable outside a nonempty complete run. Check readable size labels with NULL, zero, one byte, unit boundaries (KiB through EiB), and the maximum BIGINT; positive sizes must not appear as a rounded zero. Retain raw numeric bytes alongside the display label.

The final primary scope is restored to COMPLETE using a real discovery/measurement run. Separate actual collection scopes retain PARTIAL, all-error FAILED, empty, and discovery-failure states for dashboard query checks. Only the interruption manifest is synthetic, clearly labelled, and closed after its check; no artificial table-storage observations are written.

## Observed result — 2026-10-20 PDT

The F5 AWS demo workspace completed the main serverless test with all **20/20 checks PASS** on observed Runtime `19.11` Photon and Spark `4.2.0`; its performance target was `PERFORMANCE_OPTIMIZED`. The independent external reconciliation completed successfully using serverless environment client `4` and performance target `STANDARD`: unchanged before/after listings contained **60,565 bytes across 10 files**, exactly matching fresh native metrics, with zero byte or file-count difference. This reconciliation is limited to one small owned external fixture.

The primary full-storage scope remains **COMPLETE**, with 4 successful tables, 0 errors, 725,782 total bytes and 386,544 active bytes. Test assets and outputs are deliberately retained for review and the working draft dashboard. Both ephemeral submit runs are terminal; no scheduled collector job was created. The customer package includes this plan and the guide’s validation record; environment-specific evidence and resource identifiers are retained separately. Minimum reader privileges, inventory-backed classic compute, and large-estate scaling remain untested.

For the repository handoff, six offline contract checks passed. The updated dashboard passed all 35 dataset executions across seven scopes and all 16 size-display SQL cases. The draft dashboard was updated after SQL validation. Authenticated browser rendering remains a customer acceptance step.
