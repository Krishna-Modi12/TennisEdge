import unittest
from unittest.mock import patch

from tennis_backtest.paper_trading.weekly_summary import generate_weekly_summary


class WeeklySummaryTests(unittest.TestCase):
    def test_generate_weekly_summary_includes_clv_and_accuracy(self):
        with patch(
            "tennis_backtest.paper_trading.weekly_summary.get_signal_accuracy",
            return_value={
                "tracked_results": 20,
                "wins": 12,
                "losses": 8,
                "accuracy_pct": 60.0,
                "roi_pct": 8.5,
                "avg_odds": 1.95,
            },
        ), patch(
            "tennis_backtest.paper_trading.weekly_summary.calculate_clv",
            return_value={
                "with_closing_odds": 15,
                "total_resolved": 20,
                "coverage_pct": 75.0,
                "positive": 8,
                "negative": 5,
                "zero": 2,
                "avg_clv": 0.021,
                "by_surface": [
                    {
                        "surface": "hard",
                        "count": 10,
                        "positive": 5,
                        "negative": 3,
                        "zero": 2,
                        "avg_clv": 0.014,
                    }
                ],
            },
        ):
            report = generate_weekly_summary()

        self.assertIn("WEEKLY PAPER TRADING SUMMARY", report)
        self.assertIn("Accuracy: 60.0%", report)
        self.assertIn("Coverage: 15/20 (75.0%)", report)
        self.assertIn("Average CLV: +0.0210", report)
        self.assertIn("- hard:", report)


if __name__ == "__main__":
    unittest.main()
