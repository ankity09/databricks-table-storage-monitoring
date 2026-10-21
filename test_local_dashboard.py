"""Offline checks for the customer dashboard artifact; no workspace access."""

import json
import unittest
from pathlib import Path

from build_dashboard import build


class DashboardDeliveryTests(unittest.TestCase):
    def test_small_positive_sizes_do_not_depend_on_gib_rounding(self):
        dashboard = build("test-scope")
        widgets = {item["widget"]["name"]: item["widget"]
                   for item in dashboard["pages"][0]["layout"]}
        headline = widgets["total-storage"]["spec"]["encodings"]["value"]
        self.assertEqual(headline["fieldName"], "complete_total_bytes")
        detail_fields = {column["fieldName"]
                         for column in widgets["table-storage"]["spec"]["encodings"]["columns"]}
        self.assertIn("total_storage", detail_fields)
        self.assertIn("total_bytes", detail_fields)
        self.assertNotIn("total_gib", detail_fields)

    def test_unusual_scope_is_encoded_as_data(self):
        scope = "F5's \\ example :scope_id"
        dashboard = build(scope)
        for dataset in dashboard["datasets"]:
            sql = "".join(dataset["queryLines"])
            self.assertNotIn(scope, sql)
            self.assertNotIn(":scope_id", sql)
            self.assertIn("CAST(X'" + scope.encode("utf-8").hex() + "' AS STRING)", sql)

    def test_checked_in_template_matches_generator(self):
        path = Path(__file__).with_name("table_storage.lvdash.json")
        self.assertEqual(json.loads(path.read_text()), build("REPLACE_WITH_UNIQUE_SCOPE_ID"))


if __name__ == "__main__":
    unittest.main()
