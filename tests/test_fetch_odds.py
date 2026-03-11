import unittest
from unittest.mock import patch

import ingestion.fetch_odds as fetch_odds


class FetchOddsRegressionTests(unittest.TestCase):
    def test_mock_mode_uses_mock_data_only(self):
        mock_rows = [{"match_id": "mock_1"}]
        with patch.object(fetch_odds, "MOCK_MODE", True), patch.object(
            fetch_odds, "_mock_odds", return_value=mock_rows
        ) as mock_fn, patch.object(
            fetch_odds, "_live_odds", side_effect=AssertionError("live path should not run")
        ):
            result = fetch_odds.fetch_odds()

        self.assertEqual(result, mock_rows)
        mock_fn.assert_called_once()

    def test_live_mode_without_api_key_returns_empty_list(self):
        with patch.object(fetch_odds, "MOCK_MODE", False), patch.object(
            fetch_odds, "ODDS_API_KEY", ""
        ), patch.object(
            fetch_odds, "_live_odds", side_effect=AssertionError("live path should not run")
        ):
            result = fetch_odds.fetch_odds()

        self.assertEqual(result, [])

    def test_live_mode_with_api_key_uses_live_data(self):
        live_rows = [{"match_id": "live_1"}]
        with patch.object(fetch_odds, "MOCK_MODE", False), patch.object(
            fetch_odds, "ODDS_API_KEY", "test_key"
        ), patch.object(fetch_odds, "_live_odds", return_value=live_rows) as live_fn:
            result = fetch_odds.fetch_odds()

        self.assertEqual(result, live_rows)
        live_fn.assert_called_once()


if __name__ == "__main__":
    unittest.main()
