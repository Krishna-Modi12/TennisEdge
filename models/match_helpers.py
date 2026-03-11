"""
models/match_helpers.py
Utility helpers for match cards and market-side recommendations.
"""

from __future__ import annotations


def fair_odds(model_prob: float):
    if not model_prob or model_prob <= 0:
        return None
    return round(1 / model_prob, 2)


def kelly_stake(model_prob: float, market_odds: float, fraction: float = 0.25) -> float:
    if not model_prob or not market_odds or market_odds <= 1:
        return 0.0
    edge = (model_prob * market_odds) - 1
    kelly = edge / (market_odds - 1)
    return round(max(0, kelly * fraction) * 100, 1)


def confidence_tier(true_edge_score: float, model_prob: float) -> str:
    # Uses true_edge_score not raw value_edge to match edge_detector edge naming.
    if true_edge_score >= 0.15 and model_prob >= 0.60:
        return "💎 HIGH"
    elif true_edge_score >= 0.08 and model_prob >= 0.45:
        return "💙 MEDIUM"
    else:
        return " LOW"


def format_prob(prob: float) -> str:
    # Converts 0.623 -> "62.3%"
    if prob is None:
        return "N/A"
    return f"{round(prob * 100, 1)}%"


def total_games_probability(
    p1_first_serve_won: float,
    p2_first_serve_won: float,
    p1_second_serve_won: float,
    p2_second_serve_won: float,
) -> dict | None:
    if None in [p1_first_serve_won, p2_first_serve_won, p1_second_serve_won, p2_second_serve_won]:
        return None
    p1_hold = (p1_first_serve_won * 0.6) + (p1_second_serve_won * 0.4)
    p2_hold = (p2_first_serve_won * 0.6) + (p2_second_serve_won * 0.4)
    avg_hold = (p1_hold + p2_hold) / 2
    over_prob = (
        0.75
        if avg_hold >= 0.75
        else 0.60
        if avg_hold >= 0.65
        else 0.45
        if avg_hold >= 0.55
        else 0.35
    )
    under_prob = round(1.0 - over_prob, 2)
    return {"over": over_prob, "under": under_prob}
