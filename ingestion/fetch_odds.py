"""
ingestion/fetch_odds.py
Fetches live tennis odds from AllSportsAPI.
Falls back to mock data when MOCK_MODE=true.
"""

import logging
import re
import time
import requests
from datetime import date, timedelta
from config import ODDS_API_KEY, MOCK_MODE
from utils import normalize_player_name

logger = logging.getLogger(__name__)

# Rate limiting state
_last_api_call = 0.0
_MIN_CALL_INTERVAL = 2  # seconds between API calls

BASE_URL = "https://apiv2.allsportsapi.com/tennis/"

TOURNAMENT_SURFACE_MAP = {
    # Clay
    "roland garros": "clay", "french open": "clay",
    "rome": "clay", "madrid": "clay", "barcelona": "clay",
    "monte carlo": "clay", "monte-carlo": "clay",
    "hamburg": "clay", "buenos aires": "clay", "rio": "clay",
    "lyon": "clay", "bastad": "clay", "umag": "clay",
    "kitzbuhel": "clay", "gstaad": "clay", "bucharest": "clay",
    "marrakech": "clay", "cordoba": "clay", "santiago": "clay",
    "sao paulo": "clay", "parma": "clay", "strasbourg": "clay",
    "rabat": "clay", "palermo": "clay", "bogota": "clay",
    "prague": "clay", "budapest": "clay", "geneva": "clay",
    # Grass
    "wimbledon": "grass", "queens": "grass", "queen's": "grass",
    "halle": "grass", "eastbourne": "grass", "stuttgart": "grass",
    "s-hertogenbosch": "grass", "'s-hertogenbosch": "grass",
    "mallorca": "grass", "newport": "grass", "berlin": "grass",
    "bad homburg": "grass", "nottingham": "grass",
    # Hard (explicit)
    "australian open": "hard", "us open": "hard",
    "indian wells": "hard", "miami": "hard",
    "cincinnati": "hard", "toronto": "hard", "montreal": "hard",
    "shanghai": "hard", "beijing": "hard", "tokyo": "hard",
    "dubai": "hard", "doha": "hard", "atp finals": "hard",
    "wta finals": "hard", "brisbane": "hard", "adelaide": "hard",
    "auckland": "hard", "acapulco": "hard", "washington": "hard",
    "san diego": "hard", "seoul": "hard", "astana": "hard",
    "vienna": "hard", "basel": "hard", "paris": "hard",
}

def infer_surface(tournament: str) -> str:
    """Infer court surface from tournament name. Falls back to 'hard'.

    Uses word-boundary matching to avoid false positives where a keyword
    appears as a substring of an unrelated word (e.g. "halle" inside
    "challenger").
    """
    name = tournament.lower()
    for keyword, surface in TOURNAMENT_SURFACE_MAP.items():
        # Build a pattern that requires the keyword to be surrounded by
        # non-alpha characters (start/end of string, space, punctuation).
        pattern = r'(?<![a-z])' + re.escape(keyword) + r'(?![a-z])'
        if re.search(pattern, name):
            return surface
    return "hard"


def fetch_odds() -> list:
    """
    Returns list of match dicts:
    {
        "match_id":  str,
        "player_a":  str,
        "player_b":  str,
        "tournament": str,
        "surface":   str,
        "odds_a":    float,
        "odds_b":    float,
    }
    """
    if MOCK_MODE or not ODDS_API_KEY:
        return _mock_odds()
    return _live_odds()


def _live_odds() -> list:
    global _last_api_call
    all_matches = []

    today = date.today()
    tomorrow = today + timedelta(days=1)
    date_from = today.strftime("%Y-%m-%d")
    date_to = tomorrow.strftime("%Y-%m-%d")

    # Fetch Fixtures
    fixtures_url = f"{BASE_URL}?met=Fixtures&APIkey={ODDS_API_KEY}&from={date_from}&to={date_to}"
    # Fetch Odds
    odds_url = f"{BASE_URL}?met=Odds&APIkey={ODDS_API_KEY}&from={date_from}&to={date_to}"

    def safe_get(url):
        global _last_api_call
        elapsed = time.time() - _last_api_call
        if elapsed < _MIN_CALL_INTERVAL:
            time.sleep(_MIN_CALL_INTERVAL - elapsed)
        resp = requests.get(url, timeout=15)
        _last_api_call = time.time()
        resp.raise_for_status()
        return resp.json()

    try:
        f_data = safe_get(fixtures_url)
        o_data = safe_get(odds_url)
    except Exception as e:
        logger.error(f"[fetch_odds] Error fetching from AllSportsAPI: {e}")
        return []

    fixtures = f_data.get("result", [])
    odds = o_data.get("result", {}) or {}
    
    if not fixtures:
        logger.info("[fetch_odds] No fixtures found.")
        return []

    for m in fixtures:
        status = m.get("event_status", "")
        # skip doubles or finished/cancelled matches
        p_a_raw = m.get("event_first_player", "")
        p_b_raw = m.get("event_second_player", "")
        
        if " / " in p_a_raw or " / " in p_b_raw:
            continue
        if status in ["Finished", "Cancelled", "Postponed", "Interrupted", "Abandoned"]:
            continue
        
        match_id = str(m.get("event_key"))
        odd_data = odds.get(match_id)
        if not odd_data:
            continue
            
        # extract best odds
        # odd_data typically format: [{"Home/Away": {"Home": {"bet365": "1.9"}, "Away": {"bet365": "1.9"}}}] 
        # or dict format: {"Home/Away": {"Home": {...}, "Away": {...}}}
        best_a, best_b = None, None
        
        if isinstance(odd_data, list):
            for odd_item in odd_data:
                if "Home/Away" in odd_item:
                    best_a, best_b = _extract_best_odds(odd_item["Home/Away"])
                    break
        elif isinstance(odd_data, dict):
            if "Home/Away" in odd_data:
                 best_a, best_b = _extract_best_odds(odd_data["Home/Away"])
                 
        if not best_a or not best_b:
            continue
            
        tournament = m.get("league_name", "ATP/WTA")
        surface = infer_surface(tournament)

        all_matches.append({
            "match_id": match_id,
            "player_a": normalize_player_name(p_a_raw),
            "player_b": normalize_player_name(p_b_raw),
            "tournament": tournament,
            "surface": surface,
            "odds_a": round(best_a, 2),
            "odds_b": round(best_b, 2),
        })

    logger.info(f"[fetch_odds] Total: {len(all_matches)} matches with odds.")
    return all_matches


def _extract_best_odds(home_away_markets: dict):
    best_a = None
    best_b = None
    home_books = home_away_markets.get("Home", {})
    away_books = home_away_markets.get("Away", {})
    
    for bookmaker, price_str in home_books.items():
        try:
            p = float(price_str)
            if best_a is None or p > best_a:
                best_a = p
        except ValueError:
            pass
            
    for bookmaker, price_str in away_books.items():
        try:
            p = float(price_str)
            if best_b is None or p > best_b:
                best_b = p
        except ValueError:
            pass
            
    return best_a, best_b


def _mock_odds() -> list:
    """Realistic mock data for testing without an API key."""
    return [
        {
            "match_id":   "mock_001",
            "player_a":   "Carlos Alcaraz",
            "player_b":   "Jannik Sinner",
            "tournament": "ATP Masters 1000 – Madrid",
            "surface":    "clay",
            "odds_a":     1.85,
            "odds_b":     2.05,
        },
        {
            "match_id":   "mock_002",
            "player_a":   "Iga Swiatek",
            "player_b":   "Aryna Sabalenka",
            "tournament": "WTA 1000 – Madrid",
            "surface":    "clay",
            "odds_a":     1.60,
            "odds_b":     2.40,
        },
        {
            "match_id":   "mock_003",
            "player_a":   "Novak Djokovic",
            "player_b":   "Daniil Medvedev",
            "tournament": "ATP 500 – Vienna",
            "surface":    "hard",
            "odds_a":     1.75,
            "odds_b":     2.10,
        },
        {
            "match_id":   "mock_004",
            "player_a":   "Rafael Nadal",
            "player_b":   "Alexander Zverev",
            "tournament": "ATP Masters 1000 – Rome",
            "surface":    "clay",
            "odds_a":     2.20,
            "odds_b":     1.70,
        },
        {
            "match_id":   "mock_005",
            "player_a":   "Elena Rybakina",
            "player_b":   "Coco Gauff",
            "tournament": "WTA 500 – Berlin",
            "surface":    "grass",
            "odds_a":     1.90,
            "odds_b":     1.95,
        },
    ]
