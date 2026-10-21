#!/usr/bin/env python3
"""Validate all dashboard SQL via an explicit Databricks profile; optionally deploy a draft."""

import argparse
import json
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from build_dashboard import build


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("profile", "warehouse-id", "catalog", "schema", "scope-id"):
        parser.add_argument("--" + option, required=True)
    parser.add_argument("--display-name", default="Table storage monitoring")
    parser.add_argument("--folder", help="Existing workspace folder for a new dashboard.")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--create", action="store_true", help="Create a new draft after all SQL succeeds.")
    action.add_argument("--update", metavar="DASHBOARD_ID", help="Update this draft after all SQL succeeds.")
    parser.add_argument("--result-file", type=Path, default=Path("dashboard_validation.json"))
    args = parser.parse_args()
    if any(not getattr(args, option).strip() for option in ("profile", "warehouse_id", "catalog", "schema", "scope_id")):
        parser.error("Connection and scope arguments must be nonempty.")

    report = {"profile": args.profile, "warehouse_id": args.warehouse_id,
              "catalog": args.catalog, "schema": args.schema, "scope_id": args.scope_id,
              "started_at": datetime.now(timezone.utc).isoformat(), "queries": []}
    dashboard = build(args.scope_id)

    with tempfile.TemporaryDirectory(prefix="table_storage_dashboard_") as temporary:
        payload_path = Path(temporary) / "request.json"

        def cli(arguments, payload=None):
            command = ["databricks", *arguments]
            if payload is not None:
                payload_path.write_text(json.dumps(payload))
                command.extend(["--json", "@" + str(payload_path)])
            command.extend(["--profile", args.profile, "-o", "json"])
            completed = subprocess.run(command, capture_output=True, text=True)
            if completed.returncode:
                raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
            try:
                return json.loads(completed.stdout)
            except json.JSONDecodeError as exc:
                raise RuntimeError("Databricks CLI returned non-JSON output: " + completed.stdout[:2000]) from exc

        def validate(dataset):
            sql = "".join(dataset["queryLines"])
            response = cli(["api", "post", "/api/2.0/sql/statements"], {
                "warehouse_id": args.warehouse_id, "catalog": args.catalog, "schema": args.schema,
                "statement": sql, "wait_timeout": "30s", "on_wait_timeout": "CONTINUE",
                "disposition": "INLINE", "format": "JSON_ARRAY", "row_limit": 10000,
            })
            statement_id = response["statement_id"]
            deadline = time.monotonic() + 600
            while response.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
                if time.monotonic() > deadline:
                    cli(["api", "post", "/api/2.0/sql/statements/" + statement_id + "/cancel"], {})
                    raise TimeoutError("Dashboard validation timed out; its statement was cancelled: " + dataset["name"])
                time.sleep(3)
                response = cli(["api", "get", "/api/2.0/sql/statements/" + statement_id])
            entry = {"dataset": dataset["name"], "statement_id": statement_id,
                     "state": response.get("status", {}).get("state"), "sql": sql,
                     "manifest": response.get("manifest"),
                     "sample_rows": (response.get("result") or {}).get("data_array", [])[:10]}
            if response.get("status", {}).get("error"):
                entry["error"] = response["status"]["error"]
            report["queries"].append(entry)
            print(dataset["name"] + ": " + str(entry["state"]), flush=True)
            if entry["state"] != "SUCCEEDED":
                raise RuntimeError("Dashboard SQL failed: " + json.dumps(entry.get("error", entry["state"])))

        try:
            for dataset in dashboard["datasets"]:
                validate(dataset)
            report["sql_validation"] = "PASS"
            if args.create or args.update:
                arguments = ["lakeview", "update", args.update] if args.update else ["lakeview", "create"]
                arguments.extend([
                    "--display-name", args.display_name, "--warehouse-id", args.warehouse_id,
                    "--dataset-catalog", args.catalog, "--dataset-schema", args.schema,
                    "--serialized-dashboard", json.dumps(dashboard),
                ])
                payload = {"parent_path": args.folder} if args.folder and args.create else None
                deployed = cli(arguments, payload)
                report["dashboard_id"] = deployed.get("dashboard_id", args.update)
                report["dashboard_path"] = deployed.get("path")
                report["dashboard_state"] = "DRAFT"
                print("Draft dashboard: " + str(report["dashboard_id"]), flush=True)
        except Exception as exc:
            report["result"] = "FAIL"
            report["error"] = str(exc)
            raise
        else:
            report["result"] = "PASS"
        finally:
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            args.result_file.parent.mkdir(parents=True, exist_ok=True)
            args.result_file.write_text(json.dumps(report, indent=2) + "\n")
            print("Validation record: " + str(args.result_file), flush=True)


if __name__ == "__main__":
    main()
