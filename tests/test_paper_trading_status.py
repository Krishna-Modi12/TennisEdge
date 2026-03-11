import unittest
from unittest.mock import patch

import paper_trading_status as pts


class _Cursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, row):
        self._row = row

    def execute(self, _sql):
        return _Cursor(self._row)


class PaperTradingStatusTests(unittest.TestCase):
    def test_status_happy_path(self):
        row = (20, 5, 8, 6, 1, "2026-03-01", "2026-03-10")
        with patch("paper_trading_status.get_conn", return_value=_Conn(row)), patch(
            "paper_trading_status.get_signal_accuracy",
            return_value={"tracked_results": 14, "accuracy_pct": 57.1, "roi_pct": 5.4},
        ), patch(
            "paper_trading_status.calculate_clv",
            return_value={
                "source": "database",
                "coverage_pct": 60.0,
                "avg_clv": 0.01,
                "positive": 4,
                "negative": 2,
                "zero": 1,
            },
        ):
            status = pts.get_paper_trading_status(target_bets=100)

        self.assertTrue(status["ok"])
        self.assertEqual(status["generated_signals"], 20)
        self.assertEqual(status["tracked_bets"], 14)
        self.assertEqual(status["remaining_to_target"], 86)
        self.assertEqual(status["progress_pct"], 14.0)
        self.assertEqual(status["clv"]["source"], "database")

    def test_status_db_failure(self):
        with patch("paper_trading_status.get_conn", side_effect=Exception("db down")):
            status = pts.get_paper_trading_status()
        self.assertFalse(status["ok"])
        self.assertIn("Database unavailable", status["error"])


if __name__ == "__main__":
    unittest.main()
