import unittest
from unittest.mock import patch

import clv_tracker


class _Cursor:
    def __init__(self, one=None, all_rows=None):
        self._one = one
        self._all = all_rows or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


class _Conn:
    def __init__(self, total_resolved, rows):
        self._total_resolved = total_resolved
        self._rows = rows

    def execute(self, sql, params=None):
        if "COUNT(*)" in sql:
            return _Cursor(one=(self._total_resolved,))
        return _Cursor(all_rows=self._rows)


class ClvTrackerTests(unittest.TestCase):
    def test_calculate_clv_aggregates_global_and_surface_stats(self):
        rows = [
            ("hard", 2.10, 2.00, "win"),   # +0.10
            ("hard", 1.80, 1.90, "loss"),  # -0.10
            ("clay", 2.00, 2.00, "push"),  #  0.00
        ]
        conn = _Conn(total_resolved=5, rows=rows)

        with patch("clv_tracker.get_conn", return_value=conn):
            stats = clv_tracker.calculate_clv()

        self.assertEqual(stats["total_resolved"], 5)
        self.assertEqual(stats["with_closing_odds"], 3)
        self.assertEqual(stats["coverage_pct"], 60.0)
        self.assertEqual(stats["positive"], 1)
        self.assertEqual(stats["negative"], 1)
        self.assertEqual(stats["zero"], 1)
        self.assertEqual(stats["avg_clv"], 0.0)
        self.assertEqual(len(stats["by_surface"]), 2)

    def test_format_report_contains_core_sections(self):
        report = clv_tracker.format_clv_report(
            {
                "total_resolved": 10,
                "with_closing_odds": 6,
                "coverage_pct": 60.0,
                "positive": 3,
                "negative": 2,
                "zero": 1,
                "avg_clv": 0.015,
                "by_surface": [
                    {
                        "surface": "hard",
                        "count": 4,
                        "positive": 2,
                        "negative": 1,
                        "zero": 1,
                        "avg_clv": 0.01,
                    }
                ],
            }
        )
        self.assertIn("CLV REPORT", report)
        self.assertIn("With Closing Odds: 6 (60.0%)", report)
        self.assertIn("By Surface:", report)
        self.assertIn("- hard:", report)

    def test_db_unavailable_returns_no_data_summary(self):
        with patch("clv_tracker.get_conn", side_effect=Exception("db down")), patch(
            "clv_tracker.os.path.exists", return_value=False
        ):
            stats = clv_tracker.calculate_clv()

        self.assertEqual(stats["source"], "none")
        self.assertEqual(stats["total_resolved"], 0)
        self.assertEqual(stats["with_closing_odds"], 0)
        self.assertIn("Database unavailable", stats["note"])


if __name__ == "__main__":
    unittest.main()
