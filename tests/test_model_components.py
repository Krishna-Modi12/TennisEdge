"""
tests/test_model_components.py
Unit tests for models/model_components.py — blended probability components.
"""

import sqlite3
import pytest
from models.model_components import (
    recent_form,
    h2h_edge,
    surface_winrate,
    final_probability,
    NEUTRAL,
    MIN_H2H,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def empty_db():
    """In-memory SQLite database with the required schema but no data."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE matches (
            id         INTEGER PRIMARY KEY,
            date       TEXT,
            player1    TEXT,
            player2    TEXT,
            surface    TEXT,
            winner     TEXT,
            odds_p1    REAL,
            odds_p2    REAL,
            tournament TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE h2h (
            id      INTEGER PRIMARY KEY,
            player1 TEXT,
            player2 TEXT,
            surface TEXT,
            winner  TEXT,
            date    TEXT
        )
    """)
    conn.commit()
    return conn


@pytest.fixture
def populated_db(empty_db):
    """In-memory DB with sample match and H2H data."""
    conn = empty_db

    # Carlos Alcaraz: 2W on clay, 1L on hard (vs Sinner)
    conn.executemany(
        "INSERT INTO matches (id, date, player1, player2, surface, winner) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "2025-01-01", "Carlos Alcaraz", "Jannik Sinner",    "clay",  "Carlos Alcaraz"),
            (2, "2025-02-01", "Carlos Alcaraz", "Novak Djokovic",   "clay",  "Carlos Alcaraz"),
            (3, "2025-03-01", "Jannik Sinner",  "Carlos Alcaraz",   "hard",  "Jannik Sinner"),
        ],
    )

    # H2H on clay: Alcaraz 2 wins, Sinner 1 win (total 3 = MIN_H2H)
    conn.executemany(
        "INSERT INTO h2h (id, player1, player2, surface, winner, date) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "Carlos Alcaraz", "Jannik Sinner", "clay", "Carlos Alcaraz", "2024-01-01"),
            (2, "Carlos Alcaraz", "Jannik Sinner", "clay", "Carlos Alcaraz", "2023-05-15"),
            (3, "Carlos Alcaraz", "Jannik Sinner", "clay", "Jannik Sinner",  "2023-06-20"),
        ],
    )
    conn.commit()
    return conn


# ── recent_form ───────────────────────────────────────────────────────────────

def test_recent_form_no_data_returns_neutral(empty_db):
    result = recent_form("Carlos Alcaraz", "clay", empty_db)
    assert result == NEUTRAL


def test_recent_form_all_wins(populated_db):
    """Player with all wins should have form > 0.5."""
    result = recent_form("Carlos Alcaraz", "clay", populated_db)
    assert result > NEUTRAL


def test_recent_form_range(populated_db):
    result = recent_form("Carlos Alcaraz", "clay", populated_db)
    assert 0.0 <= result <= 1.0


def test_recent_form_opponent_lower(populated_db):
    """Sinner won fewer matches in the sample — should have lower form than Alcaraz on clay."""
    alcaraz_form = recent_form("Carlos Alcaraz", "clay", populated_db)
    sinner_form = recent_form("Jannik Sinner", "clay", populated_db)
    assert alcaraz_form >= sinner_form


# ── h2h_edge ──────────────────────────────────────────────────────────────────

def test_h2h_edge_no_data_returns_neutral(empty_db):
    result = h2h_edge("Carlos Alcaraz", "Jannik Sinner", "clay", empty_db)
    assert result == NEUTRAL


def test_h2h_edge_insufficient_meetings_returns_neutral(populated_db):
    """Fewer than MIN_H2H meetings → neutral. Use grass where there are no meetings."""
    result = h2h_edge("Carlos Alcaraz", "Jannik Sinner", "grass", populated_db)
    assert result == NEUTRAL


def test_h2h_edge_with_data(populated_db):
    """3 meetings, Alcaraz wins 2 → 2/3 ≈ 0.667."""
    result = h2h_edge("Carlos Alcaraz", "Jannik Sinner", "clay", populated_db)
    assert abs(result - 2 / 3) < 1e-6


def test_h2h_edge_range(populated_db):
    result = h2h_edge("Carlos Alcaraz", "Jannik Sinner", "clay", populated_db)
    assert 0.0 <= result <= 1.0


def test_h2h_edge_symmetric_pair_order(populated_db):
    """h2h_edge(A, B) + h2h_edge(B, A) should equal 1 when using same surface data."""
    ab = h2h_edge("Carlos Alcaraz", "Jannik Sinner", "clay", populated_db)
    ba = h2h_edge("Jannik Sinner", "Carlos Alcaraz", "clay", populated_db)
    assert abs(ab + ba - 1.0) < 1e-6


# ── surface_winrate ───────────────────────────────────────────────────────────

def test_surface_winrate_no_data_returns_neutral(empty_db):
    result = surface_winrate("Carlos Alcaraz", "clay", empty_db)
    assert result == NEUTRAL


def test_surface_winrate_all_wins(populated_db):
    """Alcaraz won both clay matches → winrate should be 1.0."""
    result = surface_winrate("Carlos Alcaraz", "clay", populated_db)
    assert abs(result - 1.0) < 1e-6


def test_surface_winrate_range(populated_db):
    result = surface_winrate("Carlos Alcaraz", "clay", populated_db)
    assert 0.0 <= result <= 1.0


def test_surface_winrate_no_matches_on_surface(populated_db):
    """No matches for a player on that surface → neutral."""
    result = surface_winrate("Carlos Alcaraz", "grass", populated_db)
    assert result == NEUTRAL


# ── final_probability ─────────────────────────────────────────────────────────

def test_final_probability_is_clamped(populated_db):
    """Result must always be within [0.01, 0.99]."""
    result = final_probability("Carlos Alcaraz", "Jannik Sinner", "clay", populated_db, 0.55)
    assert 0.01 <= result <= 0.99


def test_final_probability_range_with_extreme_elo(populated_db):
    """Even with extreme elo_prob, result is clamped."""
    result_high = final_probability("Carlos Alcaraz", "Jannik Sinner", "clay", populated_db, 0.99)
    result_low  = final_probability("Carlos Alcaraz", "Jannik Sinner", "clay", populated_db, 0.01)
    assert 0.01 <= result_high <= 0.99
    assert 0.01 <= result_low  <= 0.99


def test_final_probability_empty_db_returns_neutral_elo(empty_db):
    """With no historical data all components are 0.5, result driven by elo_prob."""
    elo_prob = 0.65
    result = final_probability("Player A", "Player B", "clay", empty_db, elo_prob)
    # With neutral (0.5) form, h2h, and surface_win, blended ≈ 0.35*0.65 + 0.65*0.5
    expected = 0.35 * elo_prob + 0.65 * 0.5
    assert abs(result - expected) < 0.01
