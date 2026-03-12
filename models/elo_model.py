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
from config import ELO_DEFAULT_RATING, SURFACE_ELO_WEIGHT
from database.db import (
    get_elo,
    get_recent_match_count,
    get_recent_player_matches_for_form,
    upsert_elo,
)

logger = logging.getLogger(__name__)

RECENT_MATCH_LIMIT = 5
FATIGUE_WINDOW_DAYS = 3
FATIGUE_PER_EXTRA_MATCH = 8.0
MAX_FATIGUE_PENALTY = 25.0


def win_probability(rating_a: float, rating_b: float) -> float:
    """Expected win probability for player A given Elo ratings."""
    return 1.0 / (1.0 + math.pow(10, (rating_b - rating_a) / 400.0))


def blended_rating(player: str, surface: str) -> float:
    """60% surface Elo + 40% overall Elo."""
    overall = get_elo(player, "overall")
    surface_r = get_elo(player, surface)
    return SURFACE_ELO_WEIGHT * surface_r + (1 - SURFACE_ELO_WEIGHT) * overall


def compute_form_adjustment(recent_matches: list) -> float:
    """
    Compute small Elo adjustment from recent match over/under-performance.

    recent_matches: list of dicts with keys:
      - expected_prob: float
      - actual_result: 1 for win, 0 for loss
    """
    if not recent_matches:
        return 0.0

    deltas = []
    for match in recent_matches:
        expected_prob = float(match.get("expected_prob", 0.5))
        actual_result = 1 if match.get("actual_result", 0) else 0
        deltas.append(actual_result - expected_prob)

    if not deltas:
        return 0.0

    avg_delta = sum(deltas) / len(deltas)
    adjustment = avg_delta * 100.0
    adjustment = max(-40.0, min(40.0, adjustment))
    return adjustment


def compute_fatigue_penalty(matches: int) -> float:
    """
    Compute Elo fatigue penalty from recent match volume.
    2+ matches in 3 days triggers a penalty:
      penalty = 8 Elo per match above 1, capped at 25.
    """
    match_count = max(0, int(matches))
    if match_count <= 1:
        return 0.0
    penalty = (match_count - 1) * FATIGUE_PER_EXTRA_MATCH
    return min(MAX_FATIGUE_PENALTY, penalty)


def _recent_form_inputs(player: str, fallback_surface: str) -> list:
    """Build recent match inputs expected by compute_form_adjustment()."""
    recent_rows = get_recent_player_matches_for_form(player, limit=RECENT_MATCH_LIMIT)
    recent_matches = []
    for row in recent_rows:
        opponent = row.get("opponent")
        if not opponent:
            continue
        match_surface = row.get("surface") or fallback_surface
        expected_prob = win_probability(
            blended_rating(player, match_surface),
            blended_rating(opponent, match_surface),
        )
        recent_matches.append(
            {
                "expected_prob": expected_prob,
                "actual_result": 1 if row.get("actual_result", 0) else 0,
            }
        )
    return recent_matches


def _player_form_adjustment(player: str, fallback_surface: str) -> float:
    """Compute robust per-player form adjustment, defaulting to 0 on lookup failures."""
    try:
        form_adjustment = compute_form_adjustment(_recent_form_inputs(player, fallback_surface))
        logger.debug("Form adjustment for %s: %+d Elo", player, int(round(form_adjustment)))
        return form_adjustment
    except Exception:
        logger.debug(
            "Failed to compute form adjustment for %s; defaulting to 0 Elo.",
            player,
            exc_info=True,
        )
        return 0.0


def _player_fatigue_penalty(player: str) -> float:
    """Compute robust per-player fatigue penalty, defaulting to 0 on lookup failures."""
    try:
        recent_matches = get_recent_match_count(player, days=FATIGUE_WINDOW_DAYS)
        penalty = compute_fatigue_penalty(recent_matches)
        if penalty > 0:
            logger.debug(
                "Fatigue penalty applied for %s: -%d Elo (%d matches/%dd)",
                player,
                int(round(penalty)),
                recent_matches,
                FATIGUE_WINDOW_DAYS,
            )
        return penalty
    except Exception:
        logger.debug(
            "Failed to compute fatigue penalty for %s; defaulting to 0 Elo.",
            player,
            exc_info=True,
        )
        return 0.0


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
    base_elo_a = blended_rating(player_a, surface)
    base_elo_b = blended_rating(player_b, surface)

    form_adjustment_a = _player_form_adjustment(player_a, surface)
    form_adjustment_b = _player_form_adjustment(player_b, surface)
    fatigue_penalty_a = _player_fatigue_penalty(player_a)
    fatigue_penalty_b = _player_fatigue_penalty(player_b)

    # Apply recent-form adjustment only for this prediction.
    elo_a = base_elo_a + form_adjustment_a - fatigue_penalty_a
    elo_b = base_elo_b + form_adjustment_b - fatigue_penalty_b
    prob_a = win_probability(elo_a, elo_b)
    return {
        "prob_a": round(prob_a, 4),
        "prob_b": round(1 - prob_a, 4),
        "elo_a":  round(elo_a, 1),
        "elo_b":  round(elo_b, 1),
    }


SURFACE_K = {
    "hard": 32,
    "clay": 28,
    "grass": 24
}


def update_elo_after_match(winner: str, loser: str, surface: str):
    """
    Call this after a match result is known.
    Updates both overall and surface Elo for both players using surface-specific K-factors.
    """
    # Use surface-specific K-factor, default to 32 (standard)
    k = SURFACE_K.get(surface.lower(), 32)
    
    for surf in ["overall", surface]:
        ra = get_elo(winner, surf)
        rb = get_elo(loser, surf)
        ea = win_probability(ra, rb)
        
        # New rating = old_rating + k * (actual - expected)
        # For winner: actual = 1, expected = ea
        # For loser: actual = 0, expected = (1 - ea)
        new_ra = ra + k * (1 - ea)
        new_rb = rb + k * (0 - (1 - ea))
        
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
    """
    Seed the DB with realistic starting Elo ratings for top players.

    BUG FIX: Previously this ran unconditionally on every bot start, silently
    overwriting Elo ratings that were built from real historical data
    (build_elo_from_history.py). Now it only seeds players who have NO existing
    entry in the DB, so historical ratings are never clobbered.
    """
    seeded = 0
    for player, surfaces in TOP_PLAYERS.items():
        for surface, rating in surfaces.items():
            existing = get_elo(player, surface)
            if existing == ELO_DEFAULT_RATING:
                # Only write if still at the uninitialised default
                upsert_elo(player, surface, rating)
                seeded += 1
    if seeded:
        print(f"[Elo] Seeded {seeded} missing player-surface ratings.")
    else:
        print("[Elo] All top players already have Elo ratings — skipping seed.")
