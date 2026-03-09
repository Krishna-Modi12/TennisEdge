"""
Daily Elo Update Job
- Fetches completed match scores from AllSportsAPI
- Updates Elo ratings for winners/losers
- Auto-resolves pending signals against completed results
"""
import logging
import time
import requests
from datetime import date, timedelta

from config import ODDS_API_KEY, MOCK_MODE
from database.db import (
    get_pending_signals, update_signal_result, get_signal_by_match_id,
)
from models.elo_model import update_elo_after_match
from utils import normalize_player_name
from ingestion.fetch_odds import infer_surface, BASE_URL

logger = logging.getLogger(__name__)

_MIN_CALL_INTERVAL = 2  # seconds between API calls
_last_api_call = 0.0

def fetch_completed_scores(days_from: int = 1) -> list:
    """Fetch completed match scores from AllSportsAPI.

    Returns list of dicts: {match_id, home, away, winner, loser, score, surface, sport_key}
    """
    if MOCK_MODE or not ODDS_API_KEY:
        return _mock_scores()

    all_results = []
    global _last_api_call

    today = date.today()
    past_date = today - timedelta(days=days_from)
    
    date_from = past_date.strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")
    
    url = f"{BASE_URL}?met=Fixtures&APIkey={ODDS_API_KEY}&from={date_from}&to={date_to}"

    elapsed = time.time() - _last_api_call
    if elapsed < _MIN_CALL_INTERVAL:
        time.sleep(_MIN_CALL_INTERVAL - elapsed)

    try:
        resp = requests.get(url, timeout=15)
        _last_api_call = time.time()
        resp.raise_for_status()
        events = resp.json().get("result", [])
    except Exception as e:
        logger.error(f"[scores] Failed to fetch completed scores: {e}")
        return []

    for event in events:
        if event.get("event_status") != "Finished":
            continue
        result = _parse_score_event(event)
        if result:
            all_results.append(result)

    logger.info(f"[scores] {len(all_results)} completed events found between {date_from} and {date_to}.")
    return all_results


def _parse_score_event(event: dict) -> dict:
    """Parse a single score event from AllSportsAPI into a result dict."""
    p_a_raw = event.get("event_first_player", "")
    p_b_raw = event.get("event_second_player", "")
    
    if " / " in p_a_raw or " / " in p_b_raw:
        return None # skip doubles
        
    winner_str = event.get("event_winner")
    if not winner_str:
        return None
        
    if "First" in winner_str:
        winner = p_a_raw
        loser = p_b_raw
    elif "Second" in winner_str:
        winner = p_b_raw
        loser = p_a_raw
    else:
        return None
        
    score_str = event.get("event_final_result", "")
    tournament = event.get("league_name", "ATP/WTA")
    surface = infer_surface(tournament)

    return {
        "match_id": str(event.get("event_key", "")),
        "home": normalize_player_name(p_a_raw),
        "away": normalize_player_name(p_b_raw),
        "winner": normalize_player_name(winner),
        "loser": normalize_player_name(loser),
        "score": score_str,
        "surface": surface,
        "sport_key": "tennis",
    }


def run_daily_elo_update():
    """Main daily job: fetch scores → update Elo → resolve signals."""
    logger.info("[daily_elo] Starting daily Elo update...")

    results = fetch_completed_scores(days_from=1)
    elo_updated = 0
    signals_resolved = 0

    for match in results:
        # Update Elo ratings
        try:
            update_elo_after_match(match["winner"], match["loser"], match["surface"])
            elo_updated += 1
        except Exception as e:
            logger.error(f"[daily_elo] Elo update failed for {match['match_id']}: {e}")

        # Auto-resolve matching signals
        try:
            signal = get_signal_by_match_id(match["match_id"])
            if signal and signal["result"] == "pending":
                bet_on_norm = normalize_player_name(signal["bet_on"])
                winner_norm = match["winner"]
                result = "win" if bet_on_norm == winner_norm else "loss"
                update_signal_result(signal["id"], result, match["winner"])
                signals_resolved += 1
                logger.info(f"[daily_elo] Signal #{signal['id']} resolved: {result} "
                           f"(bet={bet_on_norm}, winner={winner_norm})")
        except Exception as e:
            logger.error(f"[daily_elo] Signal resolve failed for {match['match_id']}: {e}")

    logger.info(f"[daily_elo] Done: {elo_updated} Elo updates, {signals_resolved} signals resolved.")
    return {"elo_updated": elo_updated, "signals_resolved": signals_resolved}


def _mock_scores() -> list:
    """Return mock completed match results for testing."""
    return [
        {
            "match_id": "mock_completed_001",
            "home": normalize_player_name("Novak Djokovic"),
            "away": normalize_player_name("Carlos Alcaraz"),
            "winner": normalize_player_name("Novak Djokovic"),
            "loser": normalize_player_name("Carlos Alcaraz"),
            "score": "6-3, 7-5",
            "surface": "hard",
            "sport_key": "tennis",
        },
        {
            "match_id": "mock_completed_002",
            "home": normalize_player_name("Jannik Sinner"),
            "away": normalize_player_name("Daniil Medvedev"),
            "winner": normalize_player_name("Jannik Sinner"),
            "loser": normalize_player_name("Daniil Medvedev"),
            "score": "6-4, 6-7, 7-6",
            "surface": "clay",
            "sport_key": "tennis",
        },
        {
            "match_id": "mock_completed_003",
            "home": normalize_player_name("Iga Swiatek"),
            "away": normalize_player_name("Aryna Sabalenka"),
            "winner": normalize_player_name("Iga Swiatek"),
            "loser": normalize_player_name("Aryna Sabalenka"),
            "score": "6-2, 6-4",
            "surface": "clay",
            "sport_key": "tennis",
        },
    ]
