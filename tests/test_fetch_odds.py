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

    def test_live_odds_extracts_best_offer_and_pinnacle_odds(self):
        fixtures = [
            {
                "event_key": "101",
                "event_first_player": "Player A",
                "event_second_player": "Player B",
                "league_name": "ATP 500 Vienna",
                "country_name": "ATP",
                "event_status": "",
                "event_date": "2026-03-11",
                "event_time": "14:00",
            }
        ]
        odds_map = {
            "101": {
                "Home/Away": {
                    "Home": {"Bet365": "1.95", "Pinnacle": "1.90", "Bad": "abc"},
                    "Away": {"Bet365": "2.00", "Pinnacle": "1.96", "Other": None},
                }
            }
        }

        with patch.object(fetch_odds, "_fetch_fixtures", return_value=fixtures), patch.object(
            fetch_odds, "_fetch_odds_map", return_value=odds_map
        ):
            result = fetch_odds._live_odds()

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row["odds_a"], 1.95)
        self.assertEqual(row["odds_b"], 2.00)
        self.assertEqual(row["pinny_odds_a"], 1.90)
        self.assertEqual(row["pinny_odds_b"], 1.96)

    def test_live_odds_extracts_ps_prefixed_pinnacle_labels(self):
        fixtures = [
            {
                "event_key": "202",
                "event_first_player": "Player C",
                "event_second_player": "Player D",
                "league_name": "WTA Dubai",
                "country_name": "WTA",
                "event_status": "",
            }
        ]
        odds_map = {
            "202": {
                "Home/Away": {
                    "Home": {"PS": "1.88", "Bet365": "1.92"},
                    "Away": {"PS3838": "2.02", "Bet365": "2.05"},
                }
            }
        }

        with patch.object(fetch_odds, "_fetch_fixtures", return_value=fixtures), patch.object(
            fetch_odds, "_fetch_odds_map", return_value=odds_map
        ):
            result = fetch_odds._live_odds()

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row["pinny_odds_a"], 1.88)
        self.assertEqual(row["pinny_odds_b"], 2.02)


if __name__ == "__main__":
    unittest.main()
