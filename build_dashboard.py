#!/usr/bin/env python3
"""Build one portable AI/BI dashboard. This script makes no network calls."""

import argparse
import json
import re
from pathlib import Path


PACKAGE = Path(__file__).resolve().parent
DATASET_FIELDS = {
    "storage_summary": ["complete_total_bytes", "measured_count", "error_count"],
    "storage_health": [
        "collection_state", "freshness_state", "inventory_count", "success_count",
        "error_count", "measured_fraction", "age_hours", "current_run_finished_at",
        "latest_attempt_status", "error_message",
    ],
    "storage_details": [
        "table_name", "total_storage", "total_bytes", "active_bytes", "time_travel_bytes",
        "vacuumable_bytes", "log_bytes", "num_total_files", "observed_at",
        "collection_status", "error_message",
    ],
    "storage_top_tables": ["table_name", "total_gib"],
    "storage_history": ["run_finished_at", "total_gib", "active_gib"],
}


def load_datasets(scope_id):
    if not isinstance(scope_id, str) or not scope_id.strip() or any(ord(char) < 32 for char in scope_id):
        raise ValueError("scope_id must be a nonempty string without control characters")
    # Use a binary literal for unusual identifiers so SQL parsing cannot reinterpret their contents.
    scope_literal = ("'" + scope_id + "'" if re.fullmatch(r"[A-Za-z0-9_.:/@=-]+", scope_id)
                     else "CAST(X'" + scope_id.encode("utf-8").hex() + "' AS STRING)")
    source = (PACKAGE / "dashboard_queries.sql").read_text()
    markers = list(re.finditer(r"(?m)^-- dataset: ([a-z_]+) \| (.+)$", source))
    datasets = []
    for index, marker in enumerate(markers):
        end = markers[index + 1].start() if index + 1 < len(markers) else len(source)
        sql = source[marker.end():end].strip().removesuffix(";").rstrip()
        sql = sql.replace(":scope_id", scope_literal)
        datasets.append({
            "name": marker.group(1), "displayName": marker.group(2),
            "queryLines": sql.splitlines(keepends=True),
        })
    if {dataset["name"] for dataset in datasets} != set(DATASET_FIELDS):
        raise ValueError("Dataset declarations do not match the dashboard widget bindings.")
    return datasets


def query(dataset, columns):
    return [{"name": "main_query", "query": {
        "datasetName": dataset,
        "fields": [{"name": field, "expression": "`" + field + "`"} for field in columns],
        "disaggregated": True,
    }}]


def frame(title, description=None):
    result = {"title": title, "showTitle": True}
    if description:
        result.update(description=description, showDescription=True)
    return result


def position(widget, x, y, width, height):
    return {"widget": widget, "position": {"x": x, "y": y, "width": width, "height": height}}


def text_widget(name, content, y, height=1):
    return position({"name": name, "multilineTextboxSpec": {"lines": [content]}}, 0, y, 12, height)


def counter(name, field, title, x, description, places=0):
    return position({
        "name": name, "queries": query("storage_summary", [field]),
        "spec": {"version": 2, "widgetType": "counter", "frame": frame(title, description),
                 "encodings": {"value": {"fieldName": field, "displayName": title,
                     "format": {"type": "number", "decimalPlaces": {"type": "max", "places": places}}}}},
    }, x, 2, 4, 3)


def table_widget(name, dataset, columns, labels, y, height):
    encodings = []
    for column, label in zip(columns, labels):
        encoding = {"fieldName": column, "displayName": label}
        if column == "measured_fraction":
            encoding["format"] = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
        if column in {"total_gib", "age_hours"}:
            encoding["format"] = {"type": "number", "decimalPlaces": {"type": "max", "places": 4}}
        encodings.append(encoding)
    return position({
        "name": name, "queries": query(dataset, columns),
        "spec": {"version": 2, "widgetType": "table",
                 "frame": frame("Collection health" if dataset == "storage_health" else "Storage by table"),
                 "encodings": {"columns": encodings}},
    }, 0, y, 12, height)


def build(scope_id):
    layout = [
        text_widget("title", "# Table storage", 0),
        text_widget("scope", "Scope: **" + scope_id + "** · Full storage, including retained files and transaction logs. Review coverage and freshness below.", 1),
        counter("total-storage", "complete_total_bytes", "Total storage (bytes)", 0,
                "Available only for a nonempty, complete collection run. Readable units appear in the table below."),
        counter("measured-tables", "measured_count", "Tables measured", 4,
                "Successful measurements in the current reporting run."),
        counter("failed-tables", "error_count", "Table errors", 8,
                "Review collection health for inventory or job failures."),
        table_widget("health", "storage_health", DATASET_FIELDS["storage_health"],
                     ["Collection", "Freshness", "Discovered", "Measured", "Errors", "Coverage", "Age (hours)",
                      "Reporting run completed", "Latest attempt", "Error"], 5, 4),
        position({
            "name": "largest-tables", "queries": query("storage_top_tables", DATASET_FIELDS["storage_top_tables"]),
            "spec": {"version": 3, "widgetType": "bar",
                     "frame": frame("Largest measured tables", "Current run; incomplete coverage is shown above."),
                     "encodings": {
                         "x": {"fieldName": "total_gib", "displayName": "Total storage (GiB)", "scale": {"type": "quantitative"}},
                         "y": {"fieldName": "table_name", "displayName": "Table", "scale": {"type": "categorical"}},
                     }},
        }, 0, 9, 6, 6),
        position({
            "name": "storage-history", "queries": query("storage_history", DATASET_FIELDS["storage_history"]),
            "spec": {"version": 3, "widgetType": "line",
                     "frame": frame("Storage by complete run", "Each point is one complete collection. Snapshots are not added across time."),
                     "encodings": {
                         "x": {"fieldName": "run_finished_at", "displayName": "Collection finished", "scale": {"type": "temporal"}},
                         "y": {"scale": {"type": "quantitative"}, "fields": [
                             {"fieldName": "total_gib", "displayName": "Total GiB"},
                             {"fieldName": "active_gib", "displayName": "Active GiB"},
                         ]},
                     }},
        }, 6, 9, 6, 6),
        table_widget("table-storage", "storage_details", DATASET_FIELDS["storage_details"],
                     ["Table", "Total storage", "Total bytes", "Active bytes", "Time travel bytes", "Vacuumable bytes",
                      "Log bytes", "Total files", "Observed at", "Status", "Error"], 15, 8),
        text_widget("refresh-note", "Dashboard refresh reads saved observations. The collection job measures storage. Unknown totals remain unavailable; the default stale threshold is 26 hours.", 23, 2),
    ]
    return {
        "datasets": load_datasets(scope_id),
        "pages": [{"name": "table-storage", "displayName": "Table storage",
                   "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": layout}],
        "uiSettings": {"theme": {
            "canvasBackgroundColor": {"light": "#F5F7FA", "dark": "#19242D"},
            "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#111B23"},
            "widgetBorderColor": {"light": "#E3E8EE", "dark": "#253541"},
            "fontColor": {"light": "#172B3A", "dark": "#E5EEF5"},
            "selectionColor": {"light": "#136FA6", "dark": "#8BC9EC"},
            "visualizationColors": ["#136FA6", "#E67722", "#368B73", "#8C6BB1", "#C6454D", "#7A8D9B"],
            "widgetHeaderAlignment": "LEFT",
        }},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope-id", required=True, help="The collector's exact scope_id.")
    parser.add_argument("--output", type=Path, default=PACKAGE / "table_storage.local.lvdash.json")
    args = parser.parse_args()
    if not args.scope_id.strip() or any(ord(char) < 32 for char in args.scope_id):
        parser.error("scope-id must be nonempty and cannot contain control characters")
    result = build(args.scope_id)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(str(args.output))


if __name__ == "__main__":
    main()
