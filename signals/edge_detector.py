"""
signals/edge_detector.py
Core engine: compare model probability vs market probability → detect edges.

BUG FIX: Wrapped each match in try/except.
Previously, one bad match (malformed odds, DB hiccup, player name with special
chars) would raise an uncaught exception that crashed the ENTIRE pipeline loop,
meaning zero signals were sent even for perfectly valid matches.
"""

from config import EDGE_THRESHOLD, MIN_ODDS, MAX_ODDS
from models.elo_model import predict
from database.db import signal_exists, save_signal


def odds_to_prob(odds: float) -> float:
    """Convert decimal odds to implied market probability (overround-naive)."""
    if odds <= 0:
        return 0.0
    return round(1.0 / odds, 4)


def detect_edges(matches: list) -> list:
    """
    Given a list of matches with odds, run the Elo model and detect value bets.
    Returns list of signal dicts for matches where edge exceeds EDGE_THRESHOLD.
    """
    signals = []

    for match in matches:
        try:
            signal = _process_match(match)
            if signal:
                signals.append(signal)
        except Exception as e:
            match_id = match.get("match_id", "unknown")
            print(f"[EdgeDetector] Skipping match {match_id}: {e}")
            continue

    print(f"[EdgeDetector] Found {len(signals)} new edge(s).")
    return signals


def _process_match(match: dict) -> dict:
    """Process a single match. Returns signal dict or None."""
    match_id   = match["match_id"]
    player_a   = match["player_a"]
    player_b   = match["player_b"]
    surface    = match.get("surface", "hard")
    tournament = match.get("tournament", "Unknown")
    odds_a     = float(match["odds_a"])
    odds_b     = float(match["odds_b"])

    # Skip if signal already sent for this match
    if signal_exists(match_id):
        return None

    # Skip if BOTH sides have extreme odds — one side can still be valid
    a_in_range = MIN_ODDS <= odds_a <= MAX_ODDS
    b_in_range = MIN_ODDS <= odds_b <= MAX_ODDS
    if not a_in_range and not b_in_range:
        return None

    # Model probability
    model  = predict(player_a, player_b, surface)
    prob_a = model["prob_a"]
    prob_b = model["prob_b"]

    # Market probability (implied from decimal odds)
    market_a = odds_to_prob(odds_a)
    market_b = odds_to_prob(odds_b)

    # Edge calculation
    edge_a = round(prob_a - market_a, 4)
    edge_b = round(prob_b - market_b, 4)

    best_edge   = None
    bet_player  = None
    bet_odds    = None
    model_prob  = None
    market_prob = None

    if edge_a >= EDGE_THRESHOLD and a_in_range:
        best_edge   = edge_a
        bet_player  = player_a
        bet_odds    = odds_a
        model_prob  = prob_a
        market_prob = market_a

    if edge_b >= EDGE_THRESHOLD and b_in_range:
        if best_edge is None or edge_b > best_edge:
            best_edge   = edge_b
            bet_player  = player_b
            bet_odds    = odds_b
            model_prob  = prob_b
            market_prob = market_b

    if best_edge is None:
        return None

    signal_id = save_signal(
        match_id=match_id,
        tournament=tournament,
        surface=surface,
        player_a=player_a,
        player_b=player_b,
        bet_on=bet_player,
        model_prob=model_prob,
        market_prob=market_prob,
        edge=best_edge,
        odds=bet_odds,
    )

    return {
        "signal_id":   signal_id,
        "match_id":    match_id,
        "tournament":  tournament,
        "surface":     surface,
        "player_a":    player_a,
        "player_b":    player_b,
        "bet_on":      bet_player,
        "model_prob":  model_prob,
        "market_prob": market_prob,
        "edge":        best_edge,
        "odds":        bet_odds,
    }
