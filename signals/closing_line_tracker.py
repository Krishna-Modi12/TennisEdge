"""
signals/closing_line_tracker.py
Captures closing odds 10 minutes before match start.
"""

import logging
import datetime as dt
from database.db import get_conn
from ingestion.fetch_odds import fetch_odds

logger = logging.getLogger(__name__)

def track_closing_lines():
    """
    Finds signals starting soon and captures current market odds as 'closing' price.
    """
    logger.info("[CLV] Checking for matches starting soon...")
    
    conn = get_conn()
    # 1. Get signals needing closing odds
    # We look for signal_performance rows where closing_odds is null
    cur = conn.execute(
        "SELECT id, match_id, taken_odds FROM signal_performance WHERE closing_odds IS NULL"
    )
    unresolved = cur.fetchall()
    
    if not unresolved:
        return

    # 2. Fetch latest odds from API
    try:
        all_matches = fetch_odds()
    except Exception as e:
        logger.error("[CLV] Failed to fetch latest odds: %s", e)
        return

    odds_lookup = {m['match_id']: m for m in all_matches}
    now = dt.datetime.now(dt.timezone.utc)

    for perf_id, match_id, taken_odds in unresolved:
        match_data = odds_lookup.get(match_id)
        if not match_data:
            continue

        # Check start time
        start_str = f"{match_data['event_date']} {match_data['event_time']}"
        try:
            start_time = dt.datetime.strptime(start_str, "%Y-%m-%d %H:%M").replace(tzinfo=dt.timezone.utc)
        except Exception:
            continue

        diff = (start_time - now).total_seconds() / 60
        
        # If match starts within 10 minutes (and hasn't started more than 5 mins ago)
        if -5 < diff <= 10:
            # closing_odds is odds of the side we bet on
            # We need to know WHICH side was bet on. 
            # Taken from signals table or we can store bet_on in performance table
            # For simplicity, we query the signals table for the side
            cur_sig = conn.execute(
                "SELECT bet_on, player_a, player_b FROM signals WHERE match_id = %s",
                (match_id,)
            )
            sig_data = cur_sig.fetchone()
            if not sig_data:
                continue
            
            bet_on, p_a, p_b = sig_data
            
            closing_odds = None
            if bet_on == p_a:
                closing_odds = match_data.get('pinny_odds_a') or match_data.get('odds_a')
            else:
                closing_odds = match_data.get('pinny_odds_b') or match_data.get('odds_b')

            if closing_odds:
                clv_ratio = float(taken_odds) / float(closing_odds)
                conn.execute(
                    "UPDATE signal_performance SET closing_odds = %s, clv_ratio = %s WHERE id = %s",
                    (closing_odds, clv_ratio, perf_id)
                )
                logger.info("[CLV] Captured closing odds for %s: %.2f (CLV: %.3f)", 
                            match_id, closing_odds, clv_ratio)

    conn.commit()
