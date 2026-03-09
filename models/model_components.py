"""
model_components.py — Supplementary prediction components for TennisEdge.

Combines with the existing Surface Elo probability to produce a blended
final probability for edge detection.

Blend weights:
    0.35  Surface Elo  (provided externally as elo_prob)
    0.25  Recent Form  (last 10 matches, decay-weighted)
    0.20  H2H Edge     (head-to-head on specific surface)
    0.20  Surface Win%  (win rate on surface over last 2 years)

All functions accept a raw sqlite3.Connection to data/tennis.db and handle
missing data gracefully (return 0.5 as a neutral prior).
"""

import logging
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

NEUTRAL = 0.5
DECAY = 0.9          # exponential decay per match for recent form
MIN_H2H = 3          # minimum head-to-head meetings to use H2H data
FORM_N = 10           # number of recent matches to consider


# ── 1. Recent Form ───────────────────────────────────────────────────────────

def recent_form(player: str, surface: str, conn: sqlite3.Connection,
                n: int = FORM_N) -> float:
    """Decay-weighted win probability from last *n* matches on any surface.

    Weight of match i (0 = most recent): DECAY^i
    Returns weighted_wins / weighted_total, or 0.5 if no data.
    Gives a small bonus (+0.05 weight) when the match surface matches
    the target surface, so surface-relevant form counts slightly more.
    """
    rows = conn.execute(
        """
        SELECT winner, surface
        FROM (
            SELECT winner, surface, date FROM matches
            WHERE player1 = ? OR player2 = ?
            ORDER BY date DESC
            LIMIT ?
        ) sub
        """,
        (player, player, n),
    ).fetchall()

    if not rows:
        return NEUTRAL

    weighted_wins = 0.0
    weighted_total = 0.0

    for i, (winner, match_surface) in enumerate(rows):
        w = DECAY ** i
        # Small bonus when surface matches
        if match_surface and match_surface.lower() == surface.lower():
            w += 0.05
        weighted_total += w
        if winner == player:
            weighted_wins += w

    if weighted_total == 0:
        return NEUTRAL

    return weighted_wins / weighted_total


# ── 2. H2H Edge ─────────────────────────────────────────────────────────────

def h2h_edge(p1: str, p2: str, surface: str,
             conn: sqlite3.Connection) -> float:
    """Head-to-head win rate of p1 vs p2 on the given surface.

    Players are stored alphabetically in the h2h table (via ordered_pair),
    so we query with the sorted pair and check the winner column.

    Returns 0.5 (neutral) if fewer than MIN_H2H meetings on that surface.
    """
    a, b = (p1, p2) if p1 <= p2 else (p2, p1)

    rows = conn.execute(
        "SELECT winner FROM h2h WHERE player1 = ? AND player2 = ? AND surface = ?",
        (a, b, surface),
    ).fetchall()

    if len(rows) < MIN_H2H:
        return NEUTRAL

    p1_wins = sum(1 for (w,) in rows if w == p1)
    return p1_wins / len(rows)


# ── 3. Surface Win Rate ─────────────────────────────────────────────────────

def surface_winrate(player: str, surface: str, conn: sqlite3.Connection,
                    years: int = 2) -> float:
    """Win percentage on *surface* over the last *years* calendar years.

    Returns 0.5 if no matches found.
    """
    cutoff = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    row = conn.execute(
        """
        SELECT
            COUNT(*)                                          AS total,
            SUM(CASE WHEN winner = ? THEN 1 ELSE 0 END)      AS wins
        FROM matches
        WHERE (player1 = ? OR player2 = ?)
          AND surface = ?
          AND date >= ?
        """,
        (player, player, player, surface, cutoff),
    ).fetchone()

    total, wins = row if row else (0, 0)
    if not total:
        return NEUTRAL

    return wins / total


# ── 4. Final Blended Probability ─────────────────────────────────────────────

# Blend weights (must sum to 1.0)
W_ELO = 0.35
W_FORM = 0.25
W_H2H = 0.20
W_SURFACE = 0.20


def final_probability(p1: str, p2: str, surface: str,
                      conn: sqlite3.Connection,
                      elo_prob: float) -> float:
    """Blend all four components into a single p1-win probability.

    Parameters
    ----------
    p1, p2 : str   — player names (any format — will be resolved to DB format)
    surface : str   — "hard", "clay", or "grass"
    conn : Connection — open connection to data/tennis.db
    elo_prob : float — pre-computed Surface Elo probability for p1

    Returns
    -------
    float  — blended probability that p1 wins (0–1)
    """
    from utils import resolve_player_name
    p1 = resolve_player_name(p1)
    p2 = resolve_player_name(p2)

    form_p1 = recent_form(p1, surface, conn)
    form_p2 = recent_form(p2, surface, conn)
    # Convert two independent form scores into a relative probability
    form_total = form_p1 + form_p2
    form_prob = (form_p1 / form_total) if form_total > 0 else NEUTRAL

    h2h_prob = h2h_edge(p1, p2, surface, conn)

    sw_p1 = surface_winrate(p1, surface, conn)
    sw_p2 = surface_winrate(p2, surface, conn)
    sw_total = sw_p1 + sw_p2
    surface_prob = (sw_p1 / sw_total) if sw_total > 0 else NEUTRAL

    blended = (
        W_ELO * elo_prob
        + W_FORM * form_prob
        + W_H2H * h2h_prob
        + W_SURFACE * surface_prob
    )

    # Clamp to [0.01, 0.99] to avoid degenerate odds
    blended = max(0.01, min(0.99, blended))

    logger.debug(
        "[model] %s v %s (%s): elo=%.3f form=%.3f h2h=%.3f sw=%.3f → %.3f",
        p1, p2, surface, elo_prob, form_prob, h2h_prob, surface_prob, blended,
    )

    return blended
