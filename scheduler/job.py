"""
scheduler/job.py
APScheduler job that runs the full pipeline every 30 minutes,
plus a daily Elo update job at 2am.

Uses asyncio.run_coroutine_threadsafe() to safely dispatch async Telegram
sends from the APScheduler background thread into the bot's event loop.
Includes exponential-backoff retry for failed Telegram sends.
"""

import asyncio
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from ingestion.fetch_odds import fetch_odds
from signals.edge_detector import detect_edges
from scheduler.elo_update_job import run_daily_elo_update
from config import POLL_INTERVAL_MINUTES

logger = logging.getLogger(__name__)

_bot_app = None   # set from bot.py via post_init
_loop = None      # the bot's asyncio event loop

MAX_SEND_RETRIES = 3
RETRY_DELAYS = [2, 5, 15]  # seconds between retries

def set_bot(app):
    global _bot_app
    _bot_app = app

def set_loop(loop):
    """Store a reference to the running event loop (called from post_init)."""
    global _loop
    _loop = loop


async def _send_with_retry(telegram_id: int, msg: str) -> bool:
    """Send a Telegram message with exponential backoff retry."""
    for attempt in range(MAX_SEND_RETRIES):
        try:
            await _bot_app.bot.send_message(
                chat_id=telegram_id,
                text=msg,
                parse_mode="MarkdownV2"
            )
            return True
        except Exception as e:
            if attempt < MAX_SEND_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                logger.warning(
                    f"[Scheduler] Send retry {attempt+1}/{MAX_SEND_RETRIES} "
                    f"for {telegram_id} in {delay}s: {e}"
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"[Scheduler] All {MAX_SEND_RETRIES} retries failed "
                    f"for {telegram_id}: {e}"
                )
                return False


async def _send_signal_to_subscribers(signal: dict):
    from database.db import get_subscribers_with_info, deduct_credit, record_delivery, add_credits_manual, get_user_credits
    from signals.signal_formatter import format_rich_signal

    subscribers = get_subscribers_with_info()
    msg = format_rich_signal(signal)

    for user in subscribers:
        deducted = deduct_credit(user["id"])
        if not deducted:
            continue

        success = await _send_with_retry(user["telegram_id"], msg)
        if success:
            record_delivery(signal["signal_id"], user["id"])
            # Low credit notification
            try:
                remaining = get_user_credits(user["telegram_id"])
                if remaining == 0:
                    await _send_with_retry(
                        user["telegram_id"],
                        "⚠️ You have *0 credits* remaining\\.\nUse /buy to top up and keep receiving signals\\."
                    )
            except Exception as e:
                logger.warning(f"[Scheduler] Low credit check failed for {user['telegram_id']}: {e}")
        else:
            # Refund credit on permanent failure
            try:
                add_credits_manual(user["id"], 1)
                logger.info(f"[Scheduler] Refunded 1 credit to user {user['telegram_id']} after send failure.")
            except Exception as refund_err:
                logger.error(f"[Scheduler] Failed to refund credit for {user['telegram_id']}: {refund_err}")


def run_pipeline():
    """Fetch odds → track movement → detect edges → push signals."""
    try:
        logger.info("[Scheduler] Running pipeline...")
        matches = fetch_odds()

        # Track odds for movement analysis (first-seen → open, subsequent → close)
        from signals.odds_movement import track_odds
        for m in matches:
            track_odds(m["match_id"], m["player_a"], m["player_b"],
                       m["odds_a"], m["odds_b"])

        signals = detect_edges(matches)

        if not signals:
            logger.info("[Scheduler] No new signals this run.")
            return

        for signal in signals:
            try:
                if _loop is not None and _loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(
                        _send_signal_to_subscribers(signal), _loop
                    )
                    future.result(timeout=120)
                else:
                    asyncio.run(_send_signal_to_subscribers(signal))
            except Exception as e:
                logger.error(f"[Scheduler] Signal delivery error: {e}")

        logger.info(f"[Scheduler] Pipeline complete – {len(signals)} signal(s) sent.")
    except Exception as e:
        logger.error(f"[Scheduler] Pipeline failed: {e}", exc_info=True)


def start_scheduler():
    scheduler = BackgroundScheduler()
    # Pipeline job: every 30 minutes
    scheduler.add_job(
        run_pipeline,
        trigger="interval",
        minutes=POLL_INTERVAL_MINUTES,
        id="pipeline_job",
        replace_existing=True,
    )
    # Daily Elo update + auto result tracking: 2am every day
    scheduler.add_job(
        run_daily_elo_update,
        trigger=CronTrigger(hour=2, minute=0),
        id="daily_elo_job",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"[Scheduler] Started – pipeline every {POLL_INTERVAL_MINUTES}min, daily Elo at 2:00am.")
    return scheduler
