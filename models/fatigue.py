"""
models/fatigue.py
Tournament fatigue model for TennisEdge.

Estimates accumulated fatigue based on recent match density and
tournament depth. Players deep in a draw who've played many matches
in the last 7-14 days are penalized.

The key insight: two players meeting in a QF may have different fatigue
levels. One may have had an easy draw (straight-sets wins), while
the other ground through 3 tough three-setters. We approximate this
via match count in the recent window.

Round labels in DB (exact values):
    "1st Round", "2nd Round", "3rd Round", "4th Round",
    "Quarterfinals", "Semifinals", "The Final", "Round Robin"
"""

import logging

logger = logging.getLogger(__name__)

# ── Round depth mapping ───────────────────────────────────────────────────────
# Higher = deeper into tournament = more accumulated fatigue.
# Values represent approximate cumulative match load at that stage.

ROUND_DEPTH = {
    "1st Round":      1,
    "2nd Round":      2,
    "3rd Round":      3,
    "4th Round":      4,
    "Quarterfinals":  4,   # same depth as 4th round in smaller draws
    "Semifinals":     5,
    "The Final":      6,
    "Round Robin":    2,   # round robin = early-stage in ATP Finals format
}


def round_factor(round_label: str) -> float:
    """
    Convert a round label to a fatigue multiplier (0–1 scale).
    Later rounds produce higher fatigue factors.

    Returns 0.0 for 1st round (fresh), up to 1.0 for Finals.
    """
    if not round_label:
        return 0.0
    depth = ROUND_DEPTH.get(round_label, 0)
    # Normalize: 1st round = 0.0, Final = 1.0
    return max(0.0, min(1.0, (depth - 1) / 5.0))


# ── Match density ─────────────────────────────────────────────────────────────

def match_density(player: str, current_date: str, matches_so_far: list,
                  window_days: int = 14) -> dict:
    """
    Count recent matches and compute fatigue score from in-memory match list.

    Parameters
    ----------
    player : str — player name (DB format: "LastName F.")
    current_date : str — "YYYY-MM-DD" of the current match
    matches_so_far : list — [(date, player1, player2, round, surface), ...]
                     already processed chronologically
    window_days : int — lookback window

    Returns
    -------
    dict with:
        matches_7d  — matches in last 7 days
        matches_14d — matches in last 14 days
        fatigue     — 0–1 score (0 = fresh, 1 = exhausted)
    """
    try:
        from datetime import datetime, timedelta
        cur = datetime.strptime(current_date, "%Y-%m-%d")
        cutoff_7 = (cur - timedelta(days=7)).strftime("%Y-%m-%d")
        cutoff_14 = (cur - timedelta(days=window_days)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return {"matches_7d": 0, "matches_14d": 0, "fatigue": 0.0}

    m7 = 0
    m14 = 0
    round_load = 0.0

    for date_str, p1, p2, rnd, _surf in matches_so_far:
        if date_str >= current_date:
            break  # only look at past matches
        if player not in (p1, p2):
            continue
        if date_str >= cutoff_7:
            m7 += 1
        if date_str >= cutoff_14:
            m14 += 1
            round_load += round_factor(rnd)

    # Fatigue score: blend of density + round depth
    # Max expected: ~7 matches in 14 days (deep run at a Slam)
    density_score = min(1.0, m14 / 7.0)
    # Round load: deeper rounds add more fatigue
    load_score = min(1.0, round_load / 4.0)

    fatigue = 0.6 * density_score + 0.4 * load_score
    return {"matches_7d": m7, "matches_14d": m14, "fatigue": round(fatigue, 4)}


# ── Fatigue adjustment ────────────────────────────────────────────────────────

def fatigue_adjustment(p1: str, p2: str, current_date: str,
                       matches_so_far: list) -> float:
    """
    Compute a probability adjustment based on relative fatigue.

    Returns a value to ADD to p1's win probability:
      - Positive if p2 is more fatigued (advantage p1)
      - Negative if p1 is more fatigued (advantage p2)
      - Zero if equal fatigue

    The adjustment is capped at ±0.05 (5 percentage points) to avoid
    overpowering the core model.
    """
    f1 = match_density(p1, current_date, matches_so_far)
    f2 = match_density(p2, current_date, matches_so_far)

    # Fatigue differential: positive = p1 more fatigued
    diff = f1["fatigue"] - f2["fatigue"]

    # Convert to probability adjustment: opponent's fatigue helps you
    # Scale: max fatigue diff of 1.0 → ±0.05 probability shift
    MAX_ADJ = 0.05
    adj = -diff * MAX_ADJ  # negative diff (p1 less tired) → positive adj for p1

    return round(max(-MAX_ADJ, min(MAX_ADJ, adj)), 4)
