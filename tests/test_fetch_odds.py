"""
tests/test_fetch_odds.py
Unit tests for ingestion/fetch_odds.py — surface inference and mock data.
"""

import pytest
from ingestion.fetch_odds import infer_surface, _mock_odds


# ── infer_surface ─────────────────────────────────────────────────────────────

def test_infer_clay_roland_garros():
    assert infer_surface("Roland Garros") == "clay"


def test_infer_clay_french_open():
    assert infer_surface("French Open") == "clay"


def test_infer_clay_rome():
    assert infer_surface("Internazionali BNL d'Italia Rome") == "clay"


def test_infer_clay_madrid():
    assert infer_surface("Mutua Madrid Open") == "clay"


def test_infer_clay_barcelona():
    assert infer_surface("Barcelona Open") == "clay"


def test_infer_clay_monte_carlo():
    assert infer_surface("Monte Carlo Masters") == "clay"


def test_infer_grass_wimbledon():
    assert infer_surface("Wimbledon 2024") == "grass"


def test_infer_grass_queens():
    assert infer_surface("Queen's Club Championships") == "grass"


def test_infer_grass_halle():
    assert infer_surface("Halle Open") == "grass"


def test_infer_hard_australian_open():
    assert infer_surface("Australian Open") == "hard"


def test_infer_hard_us_open():
    assert infer_surface("US Open") == "hard"


def test_infer_hard_indian_wells():
    assert infer_surface("BNP Paribas Open Indian Wells") == "hard"


def test_infer_hard_miami():
    assert infer_surface("Miami Open") == "hard"


def test_infer_hard_unknown():
    """Unknown tournament defaults to hard court."""
    assert infer_surface("ATP 250 Fictional City") == "hard"
    assert infer_surface("Unknown WTA Event") == "hard"
    # "challenger" used to falsely match "halle" (word-boundary fix)
    assert infer_surface("ATP Challenger Somewhere") == "hard"


def test_infer_case_insensitive():
    """Surface inference should be case-insensitive."""
    assert infer_surface("ROLAND GARROS") == "clay"
    assert infer_surface("wimbledon") == "grass"
    assert infer_surface("AUSTRALIAN OPEN") == "hard"


def test_infer_empty_string():
    """Empty tournament name → default 'hard'."""
    assert infer_surface("") == "hard"


# ── _mock_odds ────────────────────────────────────────────────────────────────

def test_mock_odds_returns_list():
    matches = _mock_odds()
    assert isinstance(matches, list)
    assert len(matches) > 0


def test_mock_odds_required_keys():
    """Each mock match must have all required keys."""
    required = {"match_id", "player_a", "player_b", "tournament", "surface", "odds_a", "odds_b"}
    for match in _mock_odds():
        assert required.issubset(match.keys()), f"Missing keys in {match}"


def test_mock_odds_valid_odds():
    """All mock matches should have positive odds."""
    for match in _mock_odds():
        assert match["odds_a"] > 1.0, f"Invalid odds_a in {match}"
        assert match["odds_b"] > 1.0, f"Invalid odds_b in {match}"


def test_mock_odds_valid_surfaces():
    """All mock matches should have a known surface."""
    valid_surfaces = {"clay", "hard", "grass", "carpet"}
    for match in _mock_odds():
        assert match["surface"] in valid_surfaces, f"Unknown surface in {match}"


def test_mock_odds_unique_match_ids():
    """All mock match IDs should be unique."""
    ids = [m["match_id"] for m in _mock_odds()]
    assert len(ids) == len(set(ids)), "Duplicate match_id found in mock data"
