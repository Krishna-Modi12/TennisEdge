"""
tests/test_elo_model.py
Unit tests for models/elo_model.py — surface-adjusted Elo model.
"""

import pytest
import math
from models.elo_model import win_probability, blended_rating, predict, TOP_PLAYERS


# ── win_probability ───────────────────────────────────────────────────────────

def test_win_probability_equal_ratings():
    """Equal Elo ratings → 50% win probability."""
    prob = win_probability(1500.0, 1500.0)
    assert abs(prob - 0.5) < 1e-6


def test_win_probability_higher_rating_wins():
    """Higher-rated player should have > 50% probability."""
    prob = win_probability(1600.0, 1400.0)
    assert prob > 0.5


def test_win_probability_lower_rating_loses():
    """Lower-rated player should have < 50% probability."""
    prob = win_probability(1400.0, 1600.0)
    assert prob < 0.5


def test_win_probability_symmetry():
    """P(A beats B) + P(B beats A) == 1."""
    pa = win_probability(1550.0, 1450.0)
    pb = win_probability(1450.0, 1550.0)
    assert abs(pa + pb - 1.0) < 1e-6


def test_win_probability_range():
    """Probability is always in (0, 1)."""
    for diff in [-1000, -200, 0, 200, 1000]:
        prob = win_probability(1500.0 + diff, 1500.0)
        assert 0.0 < prob < 1.0


def test_win_probability_standard_formula():
    """Check one known value: 200-point gap → ~76% for higher-rated player."""
    prob = win_probability(1700.0, 1500.0)
    expected = 1.0 / (1.0 + math.pow(10, (1500.0 - 1700.0) / 400.0))
    assert abs(prob - expected) < 1e-9


# ── predict ───────────────────────────────────────────────────────────────────

def test_predict_returns_required_keys():
    result = predict("Carlos Alcaraz", "Jannik Sinner", "clay")
    assert set(result.keys()) >= {"prob_a", "prob_b", "elo_a", "elo_b"}


def test_predict_probs_sum_to_one():
    result = predict("Carlos Alcaraz", "Jannik Sinner", "clay")
    assert abs(result["prob_a"] + result["prob_b"] - 1.0) < 1e-3


def test_predict_prob_range():
    result = predict("Iga Swiatek", "Aryna Sabalenka", "clay")
    assert 0.0 < result["prob_a"] < 1.0
    assert 0.0 < result["prob_b"] < 1.0


def test_predict_known_player_clay():
    """Swiatek (clay Elo 2250) should beat Sabalenka (clay Elo 2050) on clay."""
    result = predict("Iga Swiatek", "Aryna Sabalenka", "clay")
    assert result["prob_a"] > 0.5


def test_predict_known_player_hard():
    """Sinner (hard Elo 2200) should beat Nadal (hard Elo 1980) on hard."""
    result = predict("Jannik Sinner", "Rafael Nadal", "hard")
    assert result["prob_a"] > 0.5


def test_predict_elo_values_positive():
    result = predict("Carlos Alcaraz", "Novak Djokovic", "grass")
    assert result["elo_a"] > 0
    assert result["elo_b"] > 0


def test_predict_surface_matters():
    """Clay vs hard results should differ for surface-specific players."""
    clay_result = predict("Rafael Nadal", "Novak Djokovic", "clay")
    hard_result = predict("Rafael Nadal", "Novak Djokovic", "hard")
    # Nadal should do better relative to Djokovic on clay than on hard
    assert clay_result["prob_a"] > hard_result["prob_a"]


# ── blended_rating ────────────────────────────────────────────────────────────

def test_blended_rating_known_player():
    """Carlos Alcaraz is seeded — blended clay rating should be non-default."""
    from config import ELO_DEFAULT_RATING
    rating = blended_rating("Carlos Alcaraz", "clay")
    assert rating != ELO_DEFAULT_RATING


def test_blended_rating_unknown_player():
    """Unknown player gets default Elo (1500) blended with itself → 1500."""
    from config import ELO_DEFAULT_RATING
    rating = blended_rating("Unknown Player Xyz123", "clay")
    assert abs(rating - ELO_DEFAULT_RATING) < 1e-3
