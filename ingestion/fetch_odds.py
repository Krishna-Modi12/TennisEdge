"""
ingestion/fetch_odds.py
Fetches live tennis odds from TheOddsAPI.
Falls back to mock data when MOCK_MODE=true.
"""

import requests
from config import ODDS_API_KEY, ODDS_API_BASE, MOCK_MODE

# BUG FIX: "tennis" is not a valid TheOddsAPI sport key.
# Real keys are "tennis_atp" and "tennis_wta". Using "tennis" returns a 404
# and silently falls through to an empty list every single run.
TENNIS_SPORT_KEYS = ["tennis_atp", "tennis_wta"]

# BUG FIX: TheOddsAPI does not return surface in its response.
# Without this map everything defaults to "hard", meaning clay/grass
# tournaments get the wrong Elo blend and produce inaccurate edges.
TOURNAMENT_SURFACE_MAP = {
    # Grand Slams
    "australian open":    "hard",
    "french open":        "clay",
    "roland garros":      "clay",
    "wimbledon":          "grass",
    "us open":            "hard",
    # Masters / WTA 1000
    "indian wells":       "hard",
    "miami open":         "hard",
    "monte carlo":        "clay",
    "madrid open":        "clay",
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
    "rome":               "clay",
    "dubai":              "hard",
    "doha":               "hard",
    "miami":              "hard",
}


def _surface_from_tournament(tournament_name: str) -> str:
    """Look up surface by tournament name substring match."""
    name_lower = tournament_name.lower()
    for keyword, surface in TOURNAMENT_SURFACE_MAP.items():
        if keyword in name_lower:
            return surface
    return "hard"  # safe default


def fetch_odds() -> list:
    """
    Returns list of match dicts:
    {
        "match_id":   str,
        "player_a":   str,
        "player_b":   str,
        "tournament": str,
        "surface":    str,
        "odds_a":     float,
        "odds_b":     float,
    }
    """
    if MOCK_MODE or not ODDS_API_KEY:
        return _mock_odds()
    result = _live_odds()
    if not result:
        print("[fetch_odds] Live API returned no matches, falling back to mock data.")
        return _mock_odds()
    return result


def _live_odds() -> list:
    all_matches = []
    for sport_key in TENNIS_SPORT_KEYS:
        url = f"{ODDS_API_BASE}/sports/{sport_key}/odds"
        params = {
            "apiKey":     ODDS_API_KEY,
            "regions":    "uk,eu",
            "markets":    "h2h",
            "oddsFormat": "decimal",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            events = resp.json()
        except Exception as e:
            print(f"[fetch_odds] API error for {sport_key}: {e}")
            continue

        for event in events:
            match = _parse_event(event)
            if match:
                all_matches.append(match)

    print(f"[fetch_odds] Fetched {len(all_matches)} matches with odds.")
    return all_matches


def _parse_event(event: dict) -> dict:
    try:
        player_a   = event["home_team"]
        player_b   = event["away_team"]
        match_id   = event["id"]
        tournament = event.get("sport_title", "ATP/WTA")

        best_a = None
        best_b = None

        for bookmaker in event.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                if market["key"] != "h2h":
                    continue
                for outcome in market["outcomes"]:
                    if outcome["name"] == player_a:
                        if best_a is None or outcome["price"] > best_a:
                            best_a = outcome["price"]
                    elif outcome["name"] == player_b:
                        if best_b is None or outcome["price"] > best_b:
                            best_b = outcome["price"]

        if not best_a or not best_b:
            return None

        surface = _surface_from_tournament(tournament)

        return {
            "match_id":   match_id,
            "player_a":   player_a,
            "player_b":   player_b,
            "tournament": tournament,
            "surface":    surface,
            "odds_a":     round(best_a, 2),
            "odds_b":     round(best_b, 2),
        }
    except Exception as e:
        print(f"[fetch_odds] Parse error: {e}")
        return None


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
