import datetime as dt
import unittest
from unittest.mock import patch

import scheduler.update_elo_job as updater


class EloUpdaterTests(unittest.TestCase):
    def test_rejects_today_or_future_date(self):
        today = dt.datetime.utcnow().date()
        with self.assertRaises(ValueError):
            updater.run_daily_elo_update(target_date=today, dry_run=True)
        with self.assertRaises(ValueError):
            updater.run_daily_elo_update(target_date=today + dt.timedelta(days=1), dry_run=True)

    def test_dry_run_without_api_key_skips_cleanly(self):
        target = dt.date(2024, 1, 1)
        with patch.object(updater, "ODDS_API_KEY", ""):
            result = updater.run_daily_elo_update(target_date=target, dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["fixtures_fetched"], 0)
        self.assertEqual(result["skipped_reason"], "ODDS_API_KEY not set")

    def test_dry_run_counts_updates_without_writing(self):
        target = dt.date(2024, 1, 1)
        fixtures = [
            {
                "event_key": "1001",
                "event_status": "Finished",
                "event_winner": "First Player",
                "event_first_player": "Alice",
                "event_second_player": "Bob",
                "country_name": "ATP",
                "league_name": "French Open",
                "event_date": "2024-01-01",
            }
        ]
        with patch.object(updater, "ODDS_API_KEY", "test_key"), patch.object(
            updater, "_fetch_fixtures_for_date", return_value=fixtures
        ), patch(
            "scheduler.update_elo_job.elo_history_event_exists", return_value=False
        ), patch("scheduler.update_elo_job.update_elo_after_match") as update_mock, patch(
            "scheduler.update_elo_job.record_elo_history_event"
        ) as record_mock:
            result = updater.run_daily_elo_update(target_date=target, dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["duplicates"], 0)
        self.assertEqual(result["errors"], 0)
        self.assertTrue(result["dry_run"])
        update_mock.assert_not_called()
        record_mock.assert_not_called()

    def test_duplicate_events_are_skipped(self):
        target = dt.date(2024, 1, 1)
        fixtures = [
            {
                "event_key": "1002",
                "event_status": "Finished",
                "event_winner": "Second Player",
                "event_first_player": "Alice",
                "event_second_player": "Bob",
                "country_name": "ATP",
                "league_name": "Wimbledon",
                "event_date": "2024-01-01",
            }
        ]
        with patch.object(updater, "_fetch_fixtures_for_date", return_value=fixtures), patch(
            "scheduler.update_elo_job.elo_history_event_exists", return_value=True
        ), patch("scheduler.update_elo_job.update_elo_after_match") as update_mock:
            result = updater.run_daily_elo_update(target_date=target, dry_run=False)

        self.assertTrue(result["ok"])
        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["duplicates"], 1)
        update_mock.assert_not_called()

    def test_successful_update_writes_elo_and_history(self):
        target = dt.date(2024, 1, 1)
        fixtures = [
            {
                "event_key": "1003",
                "event_status": "Finished",
                "event_winner": "First Player",
                "event_first_player": "Carlos Alcaraz",
                "event_second_player": "Jannik Sinner",
                "country_name": "ATP",
                "league_name": "French Open",
                "event_date": "2024-01-01",
            }
        ]
        with patch.object(updater, "_fetch_fixtures_for_date", return_value=fixtures), patch(
            "scheduler.update_elo_job.elo_history_event_exists", return_value=False
        ), patch("scheduler.update_elo_job.update_elo_after_match") as update_mock, patch(
            "scheduler.update_elo_job.record_elo_history_event"
        ) as record_mock:
            result = updater.run_daily_elo_update(target_date=target, dry_run=False)

        self.assertTrue(result["ok"])
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["duplicates"], 0)
        self.assertEqual(result["errors"], 0)
        update_mock.assert_called_once_with("Carlos Alcaraz", "Jannik Sinner", "clay")
        record_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
