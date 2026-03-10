"""
tests/test_odds_movement.py
Unit tests for signals/odds_movement.py — line movement classification.
"""

import pytest
from signals.odds_movement import line_movement, MOVEMENT_THRESHOLD


# ── line_movement ─────────────────────────────────────────────────────────────

def test_line_movement_with():
    """Odds shortening significantly → sharp money WITH our pick."""
    # 1.80 → 1.60: pct_change = (1.60 - 1.80) / 1.80 ≈ -11% < -3%
    assert line_movement(1.80, 1.60) == "with"


def test_line_movement_against():
    """Odds drifting significantly → sharp money AGAINST our pick."""
    # 1.80 → 2.10: pct_change = (2.10 - 1.80) / 1.80 ≈ +17% > +3%
    assert line_movement(1.80, 2.10) == "against"


def test_line_movement_neutral_small_move():
    """Odds change within ±3% → neutral."""
    # 1.80 → 1.82: pct_change ≈ +1.1% — within threshold
    assert line_movement(1.80, 1.82) == "neutral"


def test_line_movement_neutral_no_change():
    """No change → neutral."""
    assert line_movement(2.00, 2.00) == "neutral"


def test_line_movement_none_open():
    """None odds_open → neutral (missing data)."""
    assert line_movement(None, 1.80) == "neutral"


def test_line_movement_none_close():
    """None odds_close → neutral (missing data)."""
    assert line_movement(1.80, None) == "neutral"


def test_line_movement_both_none():
    """Both None → neutral."""
    assert line_movement(None, None) == "neutral"


def test_line_movement_zero_open():
    """Zero odds_open → neutral (guard against division by zero)."""
    assert line_movement(0, 1.80) == "neutral"
    assert line_movement(0.0, 1.80) == "neutral"


def test_line_movement_at_threshold():
    """Change within ±MOVEMENT_THRESHOLD is neutral."""
    # ~2.5% drop and rise — both well within the 3% threshold
    assert line_movement(2.00, 1.95) == "neutral"
    assert line_movement(2.00, 2.05) == "neutral"


def test_line_movement_just_beyond_threshold_with():
    """Change just beyond -MOVEMENT_THRESHOLD → 'with'."""
    odds_open = 2.0
    odds_close = odds_open * (1 - MOVEMENT_THRESHOLD - 0.001)
    assert line_movement(odds_open, odds_close) == "with"


def test_line_movement_just_beyond_threshold_against():
    """Change just beyond +MOVEMENT_THRESHOLD → 'against'."""
    odds_open = 2.0
    odds_close = odds_open * (1 + MOVEMENT_THRESHOLD + 0.001)
    assert line_movement(odds_open, odds_close) == "against"


def test_line_movement_high_odds():
    """Large-odds movements should also classify correctly."""
    # 5.00 → 4.00: pct_change = -20% → 'with'
    assert line_movement(5.00, 4.00) == "with"
    # 5.00 → 6.00: pct_change = +20% → 'against'
    assert line_movement(5.00, 6.00) == "against"
