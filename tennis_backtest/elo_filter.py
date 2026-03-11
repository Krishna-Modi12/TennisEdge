"""
tennis_backtest/elo_filter.py
Secondary Elo agreement filter for Pinnacle-first signal generation.
"""

from __future__ import annotations

from typing import Iterable


def elo_agrees(
    pinnacle_prob_a: float,
    elo_prob_a: float,
    max_gap: float = 0.15,
) -> bool:
    """
    Return True when Elo confirms Pinnacle direction and the probability gap is acceptable.

    Rules:
    - |pinnacle_prob_a - elo_prob_a| <= max_gap
    - Both models point to the same winner side (>0.5 for player A, <0.5 for player B)
    """
    try:
        p = float(pinnacle_prob_a)
        e = float(elo_prob_a)
    except (TypeError, ValueError):
        return False

    if p <= 0.0 or p >= 1.0 or e <= 0.0 or e >= 1.0:
        return False
    if abs(p - e) > max_gap:
        return False

    p_side = 1 if p > 0.5 else (-1 if p < 0.5 else 0)
    e_side = 1 if e > 0.5 else (-1 if e < 0.5 else 0)
    if p_side == 0 or e_side == 0:
        return False
    return p_side == e_side


def apply_elo_filter(
    rows: Iterable[dict],
    max_gap: float = 0.15,
    pinnacle_key: str = "pinnacle_prob_a",
    elo_key: str = "elo_prob_a",
) -> list[dict]:
    """Filter iterable of row dicts by `elo_agrees`."""
    out = []
    for row in rows:
        if elo_agrees(row.get(pinnacle_key), row.get(elo_key), max_gap=max_gap):
            out.append(row)
    return out
