"""Small local checks of output interpretation and identifier safety; no workspace calls."""

import ast
import json
import unittest
from pathlib import Path


def definitions():
    source = Path(__file__).with_name("collector.py")
    nodes = []
    for node in ast.parse(source.read_text()).body:
        if isinstance(node, ast.For):
            break  # Notebook widget/entry-point cell is deliberately not executed.
        nodes.append(node)
    namespace = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    return namespace


class StorageContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lib = definitions()

    def rows(self, **overrides):
        values = dict(total_bytes=2**40, active_bytes=2**39, time_travel_bytes=100,
                      vacuumable_bytes=20, num_total_files=100, num_active_files=50,
                      num_time_travel_files=20, num_vacuumable_files=5)
        values.update(overrides)
        return [dict(metric_name=name, metric_value=value, metric_description=name) for name, value in values.items()]

    def test_preserves_values_larger_than_32_bit_and_raw_descriptions(self):
        values, raw = self.lib["parse_storage_metrics"](self.rows())
        self.assertEqual(values["total_bytes"], 2**40)
        self.assertEqual(len(json.loads(raw)), 8)

    def test_missing_null_duplicate_and_inconsistent_metrics_are_rejected(self):
        cases = [self.rows()[:-1], self.rows(active_bytes=None), self.rows() + self.rows()[:1],
                 self.rows(total_bytes=1), self.rows(active_bytes=-1), self.rows(num_active_files="1.5")]
        for rows in cases:
            with self.subTest(rows=rows):
                with self.assertRaises((ValueError, TypeError)):
                    self.lib["parse_storage_metrics"](rows)

    def test_identifier_components_are_quoted_individually(self):
        self.assertEqual(self.lib["qualified_name"]("my-catalog", "odd schema", "a`b"), "`my-catalog`.`odd schema`.`a``b`")
        with self.assertRaises(ValueError):
            self.lib["quote_identifier"]("")

if __name__ == "__main__":
    unittest.main()
