import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import scheduler.job as job


class SchedulerJobTests(unittest.TestCase):
    def setUp(self):
        job._bot_app = None
        job._bot_loop = None

    def test_empty_odds_response(self):
        fake_loop = MagicMock()
        fake_loop.is_running.return_value = True
        job._bot_loop = fake_loop

        with patch("ingestion.resolve_matches.resolve_pending_signals", return_value={"ok": True}), patch.object(
            job, "_trigger_daily_elo_update_if_due", return_value=None
        ), patch.object(
            job, "fetch_odds", return_value=[]
        ), patch.object(
            job, "detect_edges", return_value=[]
        ) as detect_mock, patch(
            "scheduler.job.asyncio.run_coroutine_threadsafe"
        ) as schedule_mock:
            job.run_pipeline()

        detect_mock.assert_called_once_with([])
        schedule_mock.assert_not_called()

    def test_successful_signal_delivery(self):
        fake_loop = MagicMock()
        fake_loop.is_running.return_value = True
        job._bot_loop = fake_loop

        signal = {"signal_id": 42, "bet_on": "Alice", "player_a": "Alice", "player_b": "Bob"}
        future = MagicMock()
        future.result.return_value = None

        with patch("ingestion.resolve_matches.resolve_pending_signals", return_value={"ok": True}), patch.object(
            job, "_trigger_daily_elo_update_if_due", return_value=None
        ), patch.object(
            job, "fetch_odds", return_value=[{"match_id": "1"}]
        ), patch.object(
            job, "detect_edges", return_value=[signal]
        ), patch.object(
            job, "_send_signal_to_subscribers", new_callable=AsyncMock
        ) as send_mock, patch(
            "scheduler.job.asyncio.run_coroutine_threadsafe", return_value=future
        ) as schedule_mock:
            job.run_pipeline()

        send_mock.assert_called_once_with(signal)
        schedule_mock.assert_called_once()
        coro_arg, loop_arg = schedule_mock.call_args[0]
        self.assertTrue(asyncio.iscoroutine(coro_arg))
        self.assertEqual(loop_arg, fake_loop)
        coro_arg.close()
        future.result.assert_called_once_with(timeout=30)

    def test_send_message_exception_does_not_deduct_credit(self):
        signal = {
            "signal_id": 101,
            "bet_on": "Alice",
            "player_a": "Alice",
            "player_b": "Bob",
            "surface": "hard",
            "model_prob": 0.6,
            "odds": 2.0,
            "value_edge": 0.05,
        }
        send_message = AsyncMock(side_effect=Exception("telegram send failed"))
        job._bot_app = SimpleNamespace(bot=SimpleNamespace(send_message=send_message))

        with patch("database.db.get_all_subscribers", return_value=[12345]), patch(
            "database.db.get_user", return_value={"id": 7, "credits": 3}
        ), patch("database.db.deduct_credit") as deduct_mock, patch(
            "database.db.record_delivery"
        ) as delivery_mock, patch(
            "signals.formatter.format_signal", return_value="msg"
        ), patch(
            "signals.formatter.format_signal_with_ai", return_value="msg+ai"
        ), patch(
            "ai.analyzer.generate_match_analysis", new=AsyncMock(return_value="")
        ):
            asyncio.run(job._send_signal_to_subscribers(signal))

        deduct_mock.assert_not_called()
        delivery_mock.assert_not_called()

    def test_fetch_odds_exception_is_handled(self):
        with patch("ingestion.resolve_matches.resolve_pending_signals", return_value={"ok": True}), patch.object(
            job, "_trigger_daily_elo_update_if_due", return_value=None
        ), patch.object(
            job, "fetch_odds", side_effect=Exception("odds down")
        ), patch.object(
            job, "detect_edges"
        ) as detect_mock:
            job.run_pipeline()

        detect_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
