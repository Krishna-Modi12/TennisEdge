"""
ingestion/fetch_odds.py
Fetches live tennis fixtures and odds from AllSportsAPI.
Falls back to mock data when MOCK_MODE=true or API fails.
"""

import os
import sys
import datetime
import requests
from config import ODDS_API_KEY, MOCK_MODE

# ── Production safety guard ───────────────────────────────────────────────
# MOCK_MODE=true on a live Render instance almost always means someone
# forgot to unset it after local testing. This guard prevents fake signals
# from reaching real users.
_MOCK_MODE  = os.getenv("MOCK_MODE", "false").lower() == "true"
_IS_RENDER  = os.getenv("RENDER", "") != ""   # Render sets this automatically

if _MOCK_MODE and _IS_RENDER:
    _ALLOW_OVERRIDE = os.getenv("ALLOW_MOCK_ON_RENDER", "false").lower() == "true"
    if not _ALLOW_OVERRIDE:
        print(
            "FATAL: MOCK_MODE=true detected on Render production instance.\n"
            "This would cause fake signals to be sent to real paying users.\n"
            "To allow this intentionally, set ALLOW_MOCK_ON_RENDER=true.\n"
            "Exiting."
        )
        sys.exit(1)
# ─────────────────────────────────────────────────────────────────────────

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
    Uses mock data only when MOCK_MODE=true.
    """
    if MOCK_MODE:
        return _mock_odds()
    if not ODDS_API_KEY:
        print("[fetch_odds] ODDS_API_KEY missing and MOCK_MODE=false. Returning no live matches.")
        return []
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
        pinny_a = None
        pinny_b = None

        if event_key in odds_map:
            ha = odds_map[event_key].get("Home/Away", {})
            home_odds = ha.get("Home", {})
            away_odds = ha.get("Away", {})
            # Pick best (highest) odds across bookmakers
            if home_odds:
                odds_a = max(float(v) for v in home_odds.values())
                # Extract Pinnacle odds if available (Pinnacle or PS or Pinny)
                for k, v in home_odds.items():
                    if "pinnacle" in str(k).lower():
                        pinny_a = float(v)
                        break
            if away_odds:
                odds_b = max(float(v) for v in away_odds.values())
                for k, v in away_odds.items():
                    if "pinnacle" in str(k).lower():
                        pinny_b = float(v)
                        break

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
            "pinny_odds_a": round(pinny_a, 2) if pinny_a else None,
            "pinny_odds_b": round(pinny_b, 2) if pinny_b else None,
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
            "pinny_odds_a": 1.83,
            "pinny_odds_b": 2.01,
        },
        {
            "match_id":   "mock_002",
            "player_a":   "Iga Swiatek",
            "player_b":   "Aryna Sabalenka",
            "tournament": "WTA 1000 – Madrid",
            "surface":    "clay",
            "odds_a":     1.60,
            "odds_b":     2.40,
            "pinny_odds_a": 1.55,
            "pinny_odds_b": 2.35,
        },
        {
            "match_id":   "mock_003",
            "player_a":   "Novak Djokovic",
            "player_b":   "Daniil Medvedev",
            "tournament": "ATP 500 – Vienna",
            "surface":    "hard",
            "odds_a":     1.75,
            "odds_b":     2.10,
            "pinny_odds_a": 1.71,
            "pinny_odds_b": 2.05,
        },
        {
            "match_id":   "mock_004",
            "player_a":   "Rafael Nadal",
            "player_b":   "Alexander Zverev",
            "tournament": "ATP Masters 1000 – Rome",
            "surface":    "clay",
            "odds_a":     2.20,
            "odds_b":     1.70,
            "pinny_odds_a": 2.15,
            "pinny_odds_b": 1.66,
        },
        {
            "match_id":   "mock_005",
            "player_a":   "Elena Rybakina",
            "player_b":   "Coco Gauff",
            "tournament": "WTA 500 – Berlin",
            "surface":    "grass",
            "odds_a":     1.90,
            "odds_b":     1.95,
            "pinny_odds_a": 1.88,
            "pinny_odds_b": 1.91,
        },
    ]
