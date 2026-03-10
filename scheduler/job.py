"""
scheduler/job.py
APScheduler job that runs the full pipeline every 30 minutes.

CRITICAL BUG FIX (loop capture):
python-telegram-bot v20 calls asyncio.run() inside run_polling(), which creates
a BRAND NEW event loop. Any loop captured before run_polling() via
asyncio.get_event_loop() is a DEAD loop. Calling run_coroutine_threadsafe()
against a dead loop silently does nothing — signals detected but never delivered.

Fix: expose set_loop() which is called from a post_init hook that runs INSIDE
the live loop, guaranteeing we capture the correct running loop.
"""

import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from ingestion.fetch_odds import fetch_odds
from signals.edge_detector import detect_edges
from config import POLL_INTERVAL_MINUTES

_bot_app  = None
_bot_loop = None  # set via set_loop() from post_init, NOT before run_polling()


def set_bot(app):
    """Store bot app reference. Called from main() before run_polling()."""
    global _bot_app
    _bot_app = app


def set_loop(loop: asyncio.AbstractEventLoop):
    """
    Store the live event loop. MUST be called from inside an async context
    (post_init callback) so we capture the actual loop asyncio.run() created.
    """
    global _bot_loop
    _bot_loop = loop
    print("[Scheduler] Live event loop captured.")


async def _send_signal_to_subscribers(signal: dict):
    """Async: send one signal to every subscriber who has credits."""
    from database.db import get_all_subscribers, deduct_credit, get_user
    from signals.formatter import format_signal, format_signal_with_ai

    if _bot_app is None:
        print("[Scheduler] Bot app not set — cannot send signals.")
        return

    # Generate AI analysis (non-blocking, fails gracefully)
    ai_text = ""
    try:
        from ai.analyzer import generate_match_analysis
        ai_text = await generate_match_analysis(
            player=signal.get("bet_on", ""),
            opponent=signal.get("player_b", "") if signal.get("bet_on") == signal.get("player_a") else signal.get("player_a", ""),
            surface=signal.get("surface", "hard"),
            model_prob=signal.get("model_prob", 0.5),
            odds=signal.get("odds", 2.0),
            value_edge=signal.get("value_edge", 0),
            data_quality=signal.get("data_quality", "unknown"),
            elo_prob=signal.get("elo_prob_a"),
            form_prob=signal.get("form_prob_a"),
            surface_prob=signal.get("surface_prob_a"),
            h2h_prob=signal.get("h2h_prob_a"),
        )
    except Exception as e:
        print(f"[Scheduler] AI analysis skipped: {e}")

    subscribers = get_all_subscribers()
    msg = format_signal_with_ai(signal, ai_text) if ai_text else format_signal(signal)

    for telegram_id in subscribers:
        user = get_user(telegram_id)
        if not user:
            continue
        deducted = deduct_credit(user["id"])
        if not deducted:
            try:
                await _bot_app.bot.send_message(
                    chat_id=telegram_id,
                    text="⚠️ You have 0 credits remaining. Use /buy to top up.",
                )
            except Exception:
                pass
            continue
        try:
            await _bot_app.bot.send_message(
                chat_id=telegram_id,
                text=msg,
                parse_mode="Markdown",
            )
        except Exception as e:
            print(f"[Scheduler] Failed to send to {telegram_id}: {e}")


def run_pipeline():
    """
    Sync entry point — called by APScheduler in a background thread.
    Fetches odds, detects edges, schedules async delivery into the live bot loop.
    """
    print("[Scheduler] Running pipeline...")
    matches = fetch_odds()
    signals = detect_edges(matches)

    if not signals:
        print("[Scheduler] No new signals this run.")
        return

    if _bot_loop is None or not _bot_loop.is_running():
        print("[Scheduler] Event loop not ready — signals found but not delivered.")
        return

    for signal in signals:
        try:
            future = asyncio.run_coroutine_threadsafe(
                _send_signal_to_subscribers(signal),
                _bot_loop,
            )
            future.result(timeout=30)
        except Exception as e:
            print(f"[Scheduler] Signal delivery error: {e}")


def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger="interval",
        minutes=POLL_INTERVAL_MINUTES,
        id="pipeline_job",
        replace_existing=True,
    )
    scheduler.start()
    print(f"[Scheduler] Started – pipeline runs every {POLL_INTERVAL_MINUTES} minutes.")
    return scheduler
