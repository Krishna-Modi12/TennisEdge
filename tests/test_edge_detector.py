"""
tests/test_edge_detector.py
Unit tests for signals/edge_detector.py — odds conversion and validation.
"""

import pytest
from signals.edge_detector import odds_to_prob, is_valid_odds


# ── odds_to_prob ──────────────────────────────────────────────────────────────

def test_odds_to_prob_evens():
    """2.0 odds → 0.5 implied probability."""
    assert abs(odds_to_prob(2.0) - 0.5) < 1e-4


def test_odds_to_prob_heavy_favourite():
    """1.25 odds → 0.8 implied probability."""
    assert abs(odds_to_prob(1.25) - 0.8) < 1e-4


def test_odds_to_prob_underdog():
    """4.0 odds → 0.25 implied probability."""
    assert abs(odds_to_prob(4.0) - 0.25) < 1e-4


def test_odds_to_prob_zero_odds():
    """Zero odds → 0.0 (guard against division by zero)."""
    assert odds_to_prob(0.0) == 0.0


def test_odds_to_prob_negative_odds():
    """Negative odds → 0.0 (invalid input)."""
    assert odds_to_prob(-1.5) == 0.0


def test_odds_to_prob_returns_float():
    assert isinstance(odds_to_prob(2.0), float)


def test_odds_to_prob_rounds_to_4dp():
    """Result should be rounded to 4 decimal places."""
    result = odds_to_prob(3.0)
    assert result == round(result, 4)


# ── is_valid_odds ─────────────────────────────────────────────────────────────

def test_is_valid_odds_normal_market():
    """A normal market with ~5% overround should be valid."""
    # 1.90 / 1.90 → overround ≈ 1.053
    assert is_valid_odds(1.90, 1.90) is True


def test_is_valid_odds_typical_market():
    """Favourite/underdog market within normal margin."""
    assert is_valid_odds(1.65, 2.30) is True


def test_is_valid_odds_zero_odds_a():
    """Zero odds_a is invalid."""
    assert is_valid_odds(0.0, 1.90) is False


def test_is_valid_odds_zero_odds_b():
    """Zero odds_b is invalid."""
    assert is_valid_odds(1.90, 0.0) is False


def test_is_valid_odds_negative_odds():
    """Negative odds are invalid."""
    assert is_valid_odds(-1.5, 2.0) is False
    assert is_valid_odds(2.0, -1.5) is False


def test_is_valid_odds_absurd_overround():
    """Very low odds (overround >> 1.30) should be flagged as invalid."""
    # odds of 1.01 / 1.01 → overround ≈ 1.98
    assert is_valid_odds(1.01, 1.01) is False


def test_is_valid_odds_implied_arbitrage():
    """Odds summing to implied < 0.95 (impossible in normal markets) are invalid."""
    # odds of 100 / 100 → overround ≈ 0.02
    assert is_valid_odds(100.0, 100.0) is False


def test_is_valid_odds_borderline_low():
    """Overround just above MIN_OVERROUND should pass."""
    # 1.50 / 3.00 → overround = 1/1.5 + 1/3.0 ≈ 1.0
    assert is_valid_odds(1.50, 3.00) is True
