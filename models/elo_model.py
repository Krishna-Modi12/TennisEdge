"""
models/elo_model.py
Surface-adjusted Elo win probability model.

Win probability formula (Elo):
    E(A) = 1 / (1 + 10^((Rb - Ra) / 400))

We blend overall Elo with surface-specific Elo:
    blended = SURFACE_ELO_WEIGHT * surface_elo + (1 - SURFACE_ELO_WEIGHT) * overall_elo
"""

import logging
import math
from config import ELO_DEFAULT_RATING, ELO_K_FACTOR, SURFACE_ELO_WEIGHT
from database.db import get_elo, upsert_elo
from utils import normalize_player_name, resolve_player_name

logger = logging.getLogger(__name__)


def win_probability(rating_a: float, rating_b: float) -> float:
    """Expected win probability for player A given Elo ratings."""
    return 1.0 / (1.0 + math.pow(10, (rating_b - rating_a) / 400.0))


def blended_rating(player: str, surface: str) -> float:
    """60% surface Elo + 40% overall Elo."""
    overall = get_elo(player, "overall")
    surface_r = get_elo(player, surface)
    return SURFACE_ELO_WEIGHT * surface_r + (1 - SURFACE_ELO_WEIGHT) * overall


def predict(player_a: str, player_b: str, surface: str) -> dict:
    """
    Returns:
        {
            "prob_a": float,   # win probability for player_a
            "prob_b": float,
            "elo_a":  float,   # blended Elo for player_a
            "elo_b":  float,
        }
    """
    player_a = resolve_player_name(player_a)
    player_b = resolve_player_name(player_b)
    elo_a = blended_rating(player_a, surface)
    elo_b = blended_rating(player_b, surface)
    prob_a = win_probability(elo_a, elo_b)
    return {
        "prob_a": round(prob_a, 4),
        "prob_b": round(1 - prob_a, 4),
        "elo_a":  round(elo_a, 1),
        "elo_b":  round(elo_b, 1),
    }


def update_elo_after_match(winner: str, loser: str, surface: str):
    """
    Call this after a match result is known.
    Updates both overall and surface Elo for both players.
    """
    winner = normalize_player_name(winner)
    loser = normalize_player_name(loser)
    for surf in ["overall", surface]:
        ra = get_elo(winner, surf)
        rb = get_elo(loser, surf)
        ea = win_probability(ra, rb)
        new_ra = ra + ELO_K_FACTOR * (1 - ea)   # winner gained
        new_rb = rb + ELO_K_FACTOR * (0 - (1 - ea))  # loser lost
        upsert_elo(winner, surf, new_ra)
        upsert_elo(loser,  surf, new_rb)


# ── Seed ratings for top players ──────────────────────────────────────────────
# Run once with:  from models.elo_model import seed_top_players; seed_top_players()

TOP_PLAYERS = {
    # (player_name): {surface: rating}
    "Carlos Alcaraz":   {"overall": 2180, "clay": 2210, "hard": 2160, "grass": 2100},
    "Jannik Sinner":    {"overall": 2175, "clay": 2120, "hard": 2200, "grass": 2050},
    "Novak Djokovic":   {"overall": 2150, "clay": 2130, "hard": 2140, "grass": 2200},
    "Rafael Nadal":     {"overall": 2050, "clay": 2250, "hard": 1980, "grass": 1950},
    "Daniil Medvedev":  {"overall": 2090, "clay": 1980, "hard": 2150, "grass": 1990},
    "Alexander Zverev": {"overall": 2060, "clay": 2080, "hard": 2050, "grass": 1960},
    "Aryna Sabalenka":  {"overall": 2100, "clay": 2050, "hard": 2130, "grass": 2000},
    "Iga Swiatek":      {"overall": 2130, "clay": 2250, "hard": 2080, "grass": 1970},
    "Coco Gauff":       {"overall": 2000, "clay": 1980, "hard": 2020, "grass": 1950},
    "Elena Rybakina":   {"overall": 2020, "clay": 1960, "hard": 2000, "grass": 2100},
}

def seed_top_players():
    """Seed the DB with realistic starting Elo ratings for top players."""
    for player, surfaces in TOP_PLAYERS.items():
        for surface, rating in surfaces.items():
            upsert_elo(player, surface, rating)
    logger.info(f"[Elo] Seeded {len(TOP_PLAYERS)} players.")
