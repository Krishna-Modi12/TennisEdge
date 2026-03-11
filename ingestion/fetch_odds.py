"""
ingestion/fetch_odds.py
Fetches live tennis fixtures and odds from AllSportsAPI.
Falls back to mock data when MOCK_MODE=true or API fails.
"""

import datetime
import requests
from config import ODDS_API_KEY, MOCK_MODE

ALLSPORTS_BASE = "https://apiv2.allsportsapi.com/tennis/"

# Surface lookup by tournament name keyword
TOURNAMENT_SURFACE_MAP = {
    # Grand Slams
    "australian open":    "hard",
    "french open":        "clay",
    "roland garros":      "clay",
    "wimbledon":          "grass",
    "us open":            "hard",
    # Masters / WTA 1000
    "indian wells":       "hard",
    "miami":              "hard",
    "monte carlo":        "clay",
    "madrid":             "clay",
    "italian open":       "clay",
    "rome":               "clay",
    "canada":             "hard",
    "montreal":           "hard",
    "toronto":            "hard",
    "cincinnati":         "hard",
    "shanghai":           "hard",
    "paris":              "hard",
    "vienna":             "hard",
    # ATP 500
    "halle":              "grass",
    "queens":             "grass",
    "queen's club":       "grass",
    "barcelona":          "clay",
    "hamburg":            "clay",
    "washington":         "hard",
    "beijing":            "hard",
    "tokyo":              "hard",
    "basel":              "hard",
    # WTA
    "berlin":             "grass",
    "eastbourne":         "grass",
    "birmingham":         "grass",
    "nottingham":         "grass",
    "charleston":         "clay",
    "stuttgart":          "clay",
    "dubai":              "hard",
    "doha":               "hard",
    # Challengers
    "cherbourg":          "hard",
    "kolkata":            "hard",
    "heraklion":          "clay",
}


def _surface_from_tournament(tournament_name: str) -> str:
    name_lower = tournament_name.lower()
    for keyword, surface in TOURNAMENT_SURFACE_MAP.items():
        if keyword in name_lower:
            return surface
    return "hard"


def fetch_odds() -> list:
    """
    Returns list of match dicts with odds from AllSportsAPI.
    Falls back to mock data if MOCK_MODE or API fails.
    """
    if MOCK_MODE or not ODDS_API_KEY:
        return _mock_odds()
    result = _live_odds()
    if not result:
        print("[fetch_odds] Live API returned no matches.")
        return []
    return result


def _live_odds() -> list:
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    date_from = today.strftime("%Y-%m-%d")
    date_to = tomorrow.strftime("%Y-%m-%d")

    # Step 1: Fetch fixtures (today + tomorrow)
    fixtures = _fetch_fixtures(date_from, date_to)
    if not fixtures:
        return []

    # Filter: singles only, upcoming (not finished), ATP/WTA/Challenger
    upcoming = []
    for f in fixtures:
        country = f.get("country_name", "")
        if "Doubles" in country:
            continue
        status = f.get("event_status", "")
        if status == "Finished" or status == "Cancelled":
            continue
        upcoming.append(f)

    if not upcoming:
        print("[fetch_odds] No upcoming singles fixtures found.")
        return []

    # Step 2: Fetch odds for today+tomorrow
    odds_map = _fetch_odds_map(date_from, date_to)

    # Step 3: Combine fixtures + odds
    all_matches = []
    for f in upcoming:
        event_key = str(f["event_key"])
        player_a = f.get("event_first_player", "Unknown")
        player_b = f.get("event_second_player", "Unknown")
        tournament = f.get("league_name", "Unknown")
        surface = _surface_from_tournament(tournament)
        event_time = f.get("event_time", "")
        event_date = f.get("event_date", "")

        odds_a = None
        odds_b = None

        if event_key in odds_map:
            ha = odds_map[event_key].get("Home/Away", {})
            home_odds = ha.get("Home", {})
            away_odds = ha.get("Away", {})
            # Pick best (highest) odds across bookmakers
            if home_odds:
                odds_a = max(float(v) for v in home_odds.values())
            if away_odds:
                odds_b = max(float(v) for v in away_odds.values())

        # If no odds available, use placeholder
        if not odds_a or not odds_b:
            odds_a = odds_a or 0.0
            odds_b = odds_b or 0.0

        all_matches.append({
            "match_id":   event_key,
            "player_a":   player_a,
            "player_b":   player_b,
            "tournament": tournament,
            "surface":    surface,
            "odds_a":     round(odds_a, 2),
            "odds_b":     round(odds_b, 2),
            "event_date": event_date,
            "event_time": event_time,
            "status":     f.get("event_status", ""),
        })

    print(f"[fetch_odds] Fetched {len(all_matches)} upcoming matches.")
    return all_matches


def _fetch_fixtures(date_from: str, date_to: str) -> list:
    try:
        resp = requests.get(ALLSPORTS_BASE, params={
            "met": "Fixtures",
            "APIkey": ODDS_API_KEY,
            "from": date_from,
            "to": date_to,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") == 1:
            return data.get("result", [])
        print(f"[fetch_odds] Fixtures API error: {data}")
        return []
    except Exception as e:
        print(f"[fetch_odds] Fixtures request failed: {e}")
        return []


def _fetch_odds_map(date_from: str, date_to: str) -> dict:
    try:
        resp = requests.get(ALLSPORTS_BASE, params={
            "met": "Odds",
            "APIkey": ODDS_API_KEY,
            "from": date_from,
            "to": date_to,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") == 1:
            return data.get("result", {})
        return {}
    except Exception as e:
        print(f"[fetch_odds] Odds request failed: {e}")
        return {}


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
