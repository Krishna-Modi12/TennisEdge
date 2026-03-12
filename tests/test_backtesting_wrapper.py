import unittest
from unittest.mock import patch

from backtesting.backtest_engine import run_historical_backtest


class BacktestingWrapperTests(unittest.TestCase):
    def test_wrapper_persists_backtest_result(self):
        core_result = {
            "roi": "5.00%",
            "win_rate": "52.0%",
            "total_staked": 1000.0,
            "total_profit": 50.0,
            "total_matches": 10,
            "total_bets": 10,
            "wins": 5,
            "losses": 5,
        }
        with patch("backtesting.backtest_engine.run_core_backtest", return_value=core_result), patch(
            "backtesting.backtest_engine.save_backtest_result", return_value=123
        ) as save_mock:
            result = run_historical_backtest(run_name="test", persist=True)

        self.assertEqual(result["backtest_result_id"], 123)
        save_mock.assert_called_once()

    def test_wrapper_returns_message_without_persist(self):
        with patch(
            "backtesting.backtest_engine.run_core_backtest",
            return_value={"message": "No valid signals generated"},
        ), patch("backtesting.backtest_engine.save_backtest_result") as save_mock:
            result = run_historical_backtest(run_name="test", persist=True)

        self.assertIn("message", result)
        save_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
