import time
import requests
import datetime
from config import ODDS_API_KEY
from database.db import get_pending_signals, update_signal_result, get_conn, get_signal_accuracy
import logging

logger = logging.getLogger(__name__)
ALLSPORTS_BASE = "https://apiv2.allsportsapi.com/tennis/"

def resolve_pending_signals():
    """Fetch pending signals and resolve them if their match is finished."""
    if not ODDS_API_KEY:
        return
        
    pending = get_pending_signals(max_age_days=7)
    if not pending:
        return

    logger.info(f"Checking results for {len(pending)} pending signals...")
    
    # We calculate date range covering the pending signals
    dates = []
    for p in pending:
        # Created at is a datetime object
        dt = p['created_at'].strftime("%Y-%m-%d")
        if dt not in dates:
            dates.append(dt)
            
    if not dates:
        dates = [datetime.date.today().strftime("%Y-%m-%d")]
        
    min_date = min(dates)
    max_date = datetime.date.today().strftime("%Y-%m-%d")

    try:
        resp = requests.get(ALLSPORTS_BASE, params={
            "met": "Fixtures",
            "APIkey": ODDS_API_KEY,
            "from": min_date,
            "to": max_date,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") == 1:
            fixtures = data.get("result", [])
        else:
            return
    except Exception as e:
        logger.error(f"Error fetching fixtures for resolution: {e}")
        return

    # Map match_id to fixture
    fixture_map = {str(f["event_key"]): f for f in fixtures}
    resolved_count = 0

    for sig in pending:
        match_id = str(sig["match_id"])
        if match_id in fixture_map:
            f = fixture_map[match_id]
            status = f.get("event_status", "")
            if status == "Finished":
                winner_code = f.get("event_winner", "")
                actual_winner = None
                if winner_code == "First Player":
                    actual_winner = f.get("event_first_player")
                elif winner_code == "Second Player":
                    actual_winner = f.get("event_second_player")
                
                if actual_winner:
                    bet_on = sig["bet_on"]
                    result = "win" if actual_winner == bet_on else "loss"
                    update_signal_result(sig["id"], result, actual_winner)
                    resolved_count += 1
            elif status == "Cancelled" or status == "Retired":
                # Assuming push for retired/cancelled matches
                update_signal_result(sig["id"], "push", "None")
                resolved_count += 1

    if resolved_count > 0:
        logger.info(f"Successfully resolved {resolved_count} matches via Paper Trading.")
        stats = get_signal_accuracy()
        logger.info(f"Updated Paper Trading ROI: {stats['roi_pct']}%")

if __name__ == "__main__":
    resolve_pending_signals()
