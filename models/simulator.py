"""
models/simulator.py
Monte Carlo tennis match simulator (best-of-3 sets).
"""

from __future__ import annotations

import random


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _point_win_prob(
    serve_strength_a: float,
    return_strength_a: float,
    serve_strength_b: float,
    return_strength_b: float,
) -> float:
    raw = (serve_strength_a + return_strength_a - serve_strength_b - return_strength_b + 1.0) / 2.0
    return _clamp(raw, 0.05, 0.95)


def _service_hold_prob(server_serve: float, returner_return: float) -> float:
    raw = (float(server_serve) + (1.0 - float(returner_return))) / 2.0
    return _clamp(raw, 0.35, 0.95)


def _simulate_game(server_is_a: bool, hold_a: float, hold_b: float, rng: random.Random) -> bool:
    """Returns True if player A wins the game."""
    if server_is_a:
        return rng.random() < hold_a
    return rng.random() >= hold_b


def _simulate_tiebreak(p_point_a: float, rng: random.Random) -> bool:
    points_a = 0
    points_b = 0
    while True:
        if rng.random() < p_point_a:
            points_a += 1
        else:
            points_b += 1
        if (points_a >= 7 or points_b >= 7) and abs(points_a - points_b) >= 2:
            return points_a > points_b


def _simulate_set(server_a_starts: bool, hold_a: float, hold_b: float, p_point_a: float, rng: random.Random) -> bool:
    games_a = 0
    games_b = 0
    server_is_a = server_a_starts
    while True:
        if _simulate_game(server_is_a, hold_a, hold_b, rng):
            games_a += 1
        else:
            games_b += 1
        server_is_a = not server_is_a

        if (games_a >= 6 or games_b >= 6) and abs(games_a - games_b) >= 2:
            return games_a > games_b
        if games_a == 6 and games_b == 6:
            return _simulate_tiebreak(p_point_a, rng)


def simulate_match_probability(
    serve_strength_a: float,
    return_strength_a: float,
    serve_strength_b: float,
    return_strength_b: float,
    simulations: int = 5000,
    seed: int | None = None,
) -> float:
    """
    Monte Carlo estimate for player A match win probability in best-of-3.
    """
    sims = max(100, int(simulations))
    rng = random.Random(seed)

    hold_a = _service_hold_prob(serve_strength_a, return_strength_b)
    hold_b = _service_hold_prob(serve_strength_b, return_strength_a)
    p_point_a = _point_win_prob(
        serve_strength_a, return_strength_a, serve_strength_b, return_strength_b
    )

    wins_a = 0
    for _ in range(sims):
        sets_a = 0
        sets_b = 0
        server_a_starts = rng.random() < 0.5
        while sets_a < 2 and sets_b < 2:
            if _simulate_set(server_a_starts, hold_a, hold_b, p_point_a, rng):
                sets_a += 1
            else:
                sets_b += 1
            server_a_starts = not server_a_starts
        if sets_a > sets_b:
            wins_a += 1

    return _clamp(wins_a / sims, 0.01, 0.99)
