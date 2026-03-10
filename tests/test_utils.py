"""
tests/test_utils.py
Unit tests for utils.py — player name normalisation helpers.
"""

import pytest
from utils import normalize_player_name, to_db_format


# ── normalize_player_name ─────────────────────────────────────────────────────

def test_normalize_empty_string():
    assert normalize_player_name("") == ""


def test_normalize_none_like_empty():
    # falsy input should return empty string
    assert normalize_player_name(None) == ""


def test_normalize_extra_whitespace():
    assert normalize_player_name("Carlos  Alcaraz") == "Carlos Alcaraz"
    assert normalize_player_name("  Iga  Swiatek  ") == "Iga Swiatek"


def test_normalize_last_first_comma_format():
    assert normalize_player_name("Alcaraz, Carlos") == "Carlos Alcaraz"
    assert normalize_player_name("Swiatek, Iga") == "Iga Swiatek"


def test_normalize_title_case():
    assert normalize_player_name("carlos alcaraz") == "Carlos Alcaraz"
    assert normalize_player_name("NOVAK DJOKOVIC") == "Novak Djokovic"


def test_normalize_mc_prefix():
    result = normalize_player_name("john mcdonald")
    assert result.startswith("John Mc")


def test_normalize_de_prefix():
    result = normalize_player_name("Wout de Jong")
    assert " de " in result


def test_normalize_van_prefix():
    result = normalize_player_name("Ruud Van Nistelrooy")
    assert " van " in result


def test_normalize_del_prefix():
    result = normalize_player_name("Fernando del Potro")
    assert " del " in result


# ── to_db_format ──────────────────────────────────────────────────────────────

def test_to_db_format_full_name():
    """'Carlos Alcaraz' → 'Alcaraz C.'"""
    assert to_db_format("Carlos Alcaraz") == "Alcaraz C."


def test_to_db_format_initial_last():
    """'C. Alcaraz' → 'Alcaraz C.'"""
    assert to_db_format("C. Alcaraz") == "Alcaraz C."


def test_to_db_format_already_db_format():
    """'Alcaraz C.' should stay as 'Alcaraz C.'"""
    assert to_db_format("Alcaraz C.") == "Alcaraz C."


def test_to_db_format_single_name():
    """Single-word name should be returned as-is."""
    assert to_db_format("Cher") == "Cher"


def test_to_db_format_three_part_name():
    """'Rafael Nadal Parera' → 'Nadal Parera R.'"""
    result = to_db_format("Rafael Nadal Parera")
    assert result.startswith("Nadal") and result.endswith("R.")


def test_to_db_format_iga_swiatek():
    assert to_db_format("Iga Swiatek") == "Swiatek I."


def test_to_db_format_novak_djokovic():
    assert to_db_format("Novak Djokovic") == "Djokovic N."
