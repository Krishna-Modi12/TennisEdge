import unittest
from unittest.mock import MagicMock, patch

import ingestion.resolve_matches as resolve_matches


class ResolveMatchesRegressionTests(unittest.TestCase):
    def test_runtime_validation_does_not_exit_process(self):
        with patch("ingestion.resolve_matches.sys.exit", side_effect=AssertionError("sys.exit should not be called")):
            result = resolve_matches.resolve_pending_signals(
                force_signal_id="123",
                forced_outcome=None,
                cli_mode=False,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["resolved"], 0)
        self.assertIn("error", result)

    def test_runtime_missing_api_key_returns_failure_summary(self):
        with patch.object(resolve_matches, "ODDS_API_KEY", ""):
            result = resolve_matches.resolve_pending_signals(cli_mode=False)

        self.assertFalse(result["ok"])
        self.assertEqual(result["resolved"], 0)
        self.assertEqual(result["pending"], 0)
        self.assertEqual(result["error"], "ODDS_API_KEY not set.")

    def test_unsuccessful_fixtures_response_returns_failure_summary(self):
        pending_rows = [
            {
                "id": 1,
                "match_id": "1001",
                "bet_on": "Alice",
                "created_at": "2024-01-01T10:00:00",
            }
        ]
        fake_resp = MagicMock()
        fake_resp.raise_for_status.return_value = None
        fake_resp.json.return_value = {"success": 0, "result": []}

        with patch.object(resolve_matches, "ODDS_API_KEY", "test_key"), patch.object(
            resolve_matches, "get_pending_signals", return_value=pending_rows
        ), patch("ingestion.resolve_matches.requests.get", return_value=fake_resp):
            result = resolve_matches.resolve_pending_signals()

        self.assertFalse(result["ok"])
        self.assertEqual(result["resolved"], 0)
        self.assertEqual(result["pending"], 1)
        self.assertEqual(result["error"], "fixtures_api_unsuccessful_response")

    def test_fixtures_request_exception_returns_failure_summary(self):
        pending_rows = [
            {
                "id": 1,
                "match_id": "1001",
                "bet_on": "Alice",
                "created_at": "2024-01-01T10:00:00",
            }
        ]
        with patch.object(resolve_matches, "ODDS_API_KEY", "test_key"), patch.object(
            resolve_matches, "get_pending_signals", return_value=pending_rows
        ), patch("ingestion.resolve_matches.requests.get", side_effect=Exception("network down")):
            result = resolve_matches.resolve_pending_signals()

        self.assertFalse(result["ok"])
        self.assertEqual(result["resolved"], 0)
        self.assertEqual(result["pending"], 1)
        self.assertIn("fixtures_request_failed", result["error"])

    def test_finished_match_records_closing_odds(self):
        pending_rows = [
            {
                "id": 11,
                "match_id": "1001",
                "bet_on": "Alice",
                "created_at": "2024-01-01T10:00:00",
            }
        ]

        fixtures_resp = MagicMock()
        fixtures_resp.raise_for_status.return_value = None
        fixtures_resp.json.return_value = {
            "success": 1,
            "result": [
                {
                    "event_key": "1001",
                    "event_status": "Finished",
                    "event_winner": "First Player",
                    "event_first_player": "Alice",
                    "event_second_player": "Bob",
                }
            ],
        }

        odds_resp = MagicMock()
        odds_resp.raise_for_status.return_value = None
        odds_resp.json.return_value = {
            "success": 1,
            "result": {
                "1001": {
                    "Home/Away": {
                        "Home": {"book1": "2.10", "book2": "2.20"},
                        "Away": {"book1": "1.80"},
                    }
                }
            },
        }

        with patch.object(resolve_matches, "ODDS_API_KEY", "test_key"), patch.object(
            resolve_matches, "get_pending_signals", return_value=pending_rows
        ), patch(
            "ingestion.resolve_matches.requests.get", side_effect=[fixtures_resp, odds_resp]
        ), patch(
            "ingestion.resolve_matches.update_signal_result"
        ) as update_mock:
            result = resolve_matches.resolve_pending_signals()

        self.assertTrue(result["ok"])
        self.assertEqual(result["resolved"], 1)
        self.assertEqual(result["won"], 1)
        update_mock.assert_called_once_with(11, "win", "Alice", closing_odds=2.2)


if __name__ == "__main__":
    unittest.main()
