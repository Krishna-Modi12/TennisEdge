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
import datetime as dt
import hashlib
import os
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from ingestion.fetch_odds import fetch_odds
from signals.edge_detector import detect_edges
from config import POLL_INTERVAL_MINUTES

_bot_app  = None
_bot_loop = None  # set via set_loop() from post_init, NOT before run_polling()
_elo_update_lock = threading.Lock()
_elo_update_inflight = False
_last_elo_update_date = None
ELO_DAILY_UPDATE_HOUR_UTC = int(os.getenv("ELO_DAILY_UPDATE_HOUR_UTC", "6"))


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
    from database.db import (
        get_all_subscribers,
        deduct_credit,
        get_user,
        is_signal_alert_sent,
        record_signal_alert,
    )
    from signals.formatter import format_signal, format_signal_with_ai

    if _bot_app is None:
        print("[Scheduler] Bot app not set — cannot send signals.")
        return

    match_id = str(signal.get("match_id") or "").strip()
    market = str(signal.get("market") or "match_winner").strip()
    selection = str(signal.get("bet_on") or "").strip()
    normalized_selection = selection.lower().strip()
    signal_hash = ""
    if match_id and market and normalized_selection:
        signal_hash = hashlib.sha1(
            f"{match_id}:{market}:{normalized_selection}".encode()
        ).hexdigest()

    if signal_hash:
        try:
            if is_signal_alert_sent(signal_hash):
                print(
                    f"[Scheduler] Duplicate alert skipped for match_id={match_id}, "
                    f"hash={signal_hash}"
                )
                return
        except Exception as e:
            print(f"[Scheduler] Duplicate-check error (continuing): {e}")

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
    sent_any = False

    for telegram_id in subscribers:
        user = get_user(telegram_id)
        if not user:
            continue
            
        if user["credits"] <= 0:
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
            # Successfully sent! Now deduct credit and record delivery
            from database.db import record_delivery
            deduct_credit(user["id"])
            if signal.get("signal_id"):
                record_delivery(signal["signal_id"], user["id"])
            sent_any = True
            await asyncio.sleep(0.3)  # Telegram rate limit protection
        except Exception as e:
            print(f"[Scheduler] Failed to send to {telegram_id}: {e}")

    if sent_any and signal_hash:
        try:
            record_signal_alert(signal_hash, match_id)
        except Exception as e:
            print(f"[Scheduler] Failed to persist sent alert state: {e}")


def _run_daily_elo_update_worker(target_date: dt.date):
    """Background thread worker for daily Elo updates."""
    global _elo_update_inflight, _last_elo_update_date
    try:
        from scheduler.update_elo_job import run_daily_elo_update
        result = run_daily_elo_update(target_date=target_date, dry_run=False)
        if result.get("ok"):
            _last_elo_update_date = target_date
    except Exception as e:
        print(f"[Scheduler] Daily Elo update error: {e}")
    finally:
        with _elo_update_lock:
            _elo_update_inflight = False


def _trigger_daily_elo_update_if_due():
    """
    Start daily Elo update in a background thread once per UTC day.
    It updates yesterday's matches and never blocks signal delivery.
    """
    global _elo_update_inflight
    now = dt.datetime.utcnow()
    if now.hour < ELO_DAILY_UPDATE_HOUR_UTC:
        return
    target_date = now.date() - dt.timedelta(days=1)
    if _last_elo_update_date == target_date:
        return
    with _elo_update_lock:
        if _elo_update_inflight:
            return
        _elo_update_inflight = True
    threading.Thread(
        target=_run_daily_elo_update_worker,
        args=(target_date,),
        daemon=True,
    ).start()


def run_pipeline():
    """
    Sync entry point — called by APScheduler in a background thread.
    Fetches odds, detects edges, schedules async delivery into the live bot loop.
    Also resolves pending paper trades.
    """
    print("[Scheduler] Running pipeline...")
    
    # 0. Track closing line value for matches starting soon
    try:
        from signals.closing_line_tracker import track_closing_lines
        track_closing_lines()
    except Exception as e:
        print(f"[Scheduler] CLV tracking error: {e}")

    # 1. Resolve pending paper trades first
    try:
        from ingestion.resolve_matches import resolve_pending_signals
        resolve_pending_signals()
    except Exception as e:
        print(f"[Scheduler] Paper trade resolution error: {e}")
    # 1a. Sync ROI/CLV performance table for resolved signals
    try:
        from signals.result_tracker import sync_signal_performance
        sync_signal_performance(limit=500, stake=1.0)
    except Exception as e:
        print(f"[Scheduler] Result tracker sync error: {e}")
    # 1b. Daily Elo update (non-blocking secondary job)
    try:
        _trigger_daily_elo_update_if_due()
    except Exception as e:
        print(f"[Scheduler] Daily Elo trigger error: {e}")

    # 2. Fetch new matches and detect edges
    try:
        matches = fetch_odds()
    except Exception as e:
        print(f"[Scheduler] Odds fetch error: {e}")
        return

    try:
        signals = detect_edges(matches)
    except Exception as e:
        print(f"[Scheduler] Edge detection error: {e}")
        return

    signals = sorted(
        signals,
        key=lambda s: s.get("ev_score", float("-inf")),
        reverse=True,
    )

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
        next_run_time=dt.datetime.utcnow(),
        replace_existing=True,
    )
    try:
        from scheduler.model_monitor_job import run_model_monitor_job

        scheduler.add_job(
            run_model_monitor_job,
            trigger="cron",
            hour=4,
            minute=0,
            id="model_monitor_job",
            replace_existing=True,
        )
    except Exception as e:
        print(f"[Scheduler] Model monitor job setup skipped: {e}")
    scheduler.start()
    print(
        f"[Scheduler] Started – pipeline runs every {POLL_INTERVAL_MINUTES} minutes "
        f"(first run scheduled immediately)."
    )
    return scheduler
