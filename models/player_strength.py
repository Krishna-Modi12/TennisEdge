"""
models/player_strength.py
Serve/return strength modeling utilities.
"""

from database.db import get_player_stats, get_player_surface_stats

MIN_STRENGTH_MATCHES = 8


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _proxy_strength_from_win_rate(surface_win_rate: float) -> tuple[float, float]:
    """
    Conservative transform from surface win rate to serve/return strength
    when direct point-level stats are unavailable.
    """
    wr = _clamp(float(surface_win_rate), 0.0, 1.0)
    serve_strength = _clamp(0.62 + ((wr - 0.5) * 0.30), 0.45, 0.80)
    return_strength = _clamp(0.38 + ((wr - 0.5) * 0.24), 0.25, 0.55)
    return serve_strength, return_strength


def _normalize_strength_stats(stats: dict) -> dict | None:
    if not stats:
        return None
    serve_strength = stats.get("serve_strength")
    return_strength = stats.get("return_strength")
    if serve_strength is None or return_strength is None:
        return None
    return {
        "serve_strength": float(serve_strength),
        "return_strength": float(return_strength),
        "matches_counted": int(stats.get("matches_counted") or 0),
    }


def _fallback_strength_from_player_stats(player: str, surface: str) -> dict | None:
    stats = get_player_stats(player, surface)
    if not stats:
        return None
    wr = stats.get("surface_win_rate")
    if wr is None:
        return None
    serve_strength, return_strength = _proxy_strength_from_win_rate(wr)
    return {
        "serve_strength": serve_strength,
        "return_strength": return_strength,
        "matches_counted": int(stats.get("matches_counted") or 0),
    }


def _get_strength_stats(player: str, surface: str) -> dict | None:
    explicit = _normalize_strength_stats(get_player_surface_stats(player, surface))
    if explicit:
        return explicit
    return _fallback_strength_from_player_stats(player, surface)


def compute_strength_probability(stats_a: dict, stats_b: dict) -> float:
    """
    Approximate fair strength probability:
      fair_strength_prob = (S_A + R_A - S_B - R_B + 1) / 2
    """
    s_a = float(stats_a["serve_strength"])
    r_a = float(stats_a["return_strength"])
    s_b = float(stats_b["serve_strength"])
    r_b = float(stats_b["return_strength"])
    return _clamp((s_a + r_a - s_b - r_b + 1.0) / 2.0, 0.01, 0.99)


def predict_strength_prob(player_a: str, player_b: str, surface: str) -> dict | None:
    """
    Returns strength probability package for player_a, or None when data is weak.
    """
    stats_a = _get_strength_stats(player_a, surface)
    stats_b = _get_strength_stats(player_b, surface)
    if not stats_a or not stats_b:
        return None
    if (
        stats_a["matches_counted"] < MIN_STRENGTH_MATCHES
        or stats_b["matches_counted"] < MIN_STRENGTH_MATCHES
    ):
        return None

    prob_a = compute_strength_probability(stats_a, stats_b)
    return {
        "prob_a": prob_a,
        "prob_b": 1.0 - prob_a,
        "serve_a": stats_a["serve_strength"],
        "return_a": stats_a["return_strength"],
        "serve_b": stats_b["serve_strength"],
        "return_b": stats_b["return_strength"],
        "matches_a": stats_a["matches_counted"],
        "matches_b": stats_b["matches_counted"],
    }
