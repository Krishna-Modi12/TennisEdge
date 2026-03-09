"""
signals/edge_detector.py
Core engine: compare model probability vs market probability → detect edges.
Applies odds movement filter to suppress signals where sharp money disagrees.
"""

import logging
from config import EDGE_THRESHOLD, MIN_ODDS, MAX_ODDS
from models.elo_model import predict
from database.db import signal_exists, save_signal

logger = logging.getLogger(__name__)


def odds_to_prob(odds: float) -> float:
    """Convert decimal odds to implied market probability."""
    if odds <= 0:
        return 0.0
    return round(1.0 / odds, 4)


def detect_edges(matches: list) -> list:
    """
    Given a list of matches with odds, run the Elo model and detect value bets.

    Returns list of signal dicts for matches where edge > EDGE_THRESHOLD.
    """
    signals = []

    for match in matches:
        match_id   = match["match_id"]
        player_a   = match["player_a"]
        player_b   = match["player_b"]
        surface    = match.get("surface", "hard")
        tournament = match.get("tournament", "Unknown")
        odds_a     = match["odds_a"]
        odds_b     = match["odds_b"]

        # Skip if signal already sent for this match
        if signal_exists(match_id):
            continue

        # Skip extreme odds
        if not (MIN_ODDS <= odds_a <= MAX_ODDS) and not (MIN_ODDS <= odds_b <= MAX_ODDS):
            continue

        # Model probability
        model = predict(player_a, player_b, surface)
        elo_prob_a = model["prob_a"]
        # elo_prob_b = model["prob_b"]

        from models.model_components import final_probability
        from database.db import get_conn
        
        with get_conn() as conn:
            prob_a = final_probability(player_a, player_b, surface, conn, elo_prob_a)

        # Apply isotonic calibration if model is available
        try:
            from models.calibration import calibrate
            prob_a = calibrate(prob_a)
        except Exception:
            pass  # fall back to uncalibrated if model not trained yet

        prob_b = 1.0 - prob_a

        # Market probability
        market_a = odds_to_prob(odds_a)
        market_b = odds_to_prob(odds_b)

        # Edge calculation
        edge_a = round(prob_a - market_a, 4)
        edge_b = round(prob_b - market_b, 4)

        best_edge  = None
        bet_player = None
        bet_odds   = None
        model_prob = None
        market_prob = None

        if edge_a >= EDGE_THRESHOLD and MIN_ODDS <= odds_a <= MAX_ODDS:
            best_edge   = edge_a
            bet_player  = player_a
            bet_odds    = odds_a
            model_prob  = prob_a
            market_prob = market_a

        if edge_b >= EDGE_THRESHOLD and MIN_ODDS <= odds_b <= MAX_ODDS:
            if best_edge is None or edge_b > best_edge:
                best_edge   = edge_b
                bet_player  = player_b
                bet_odds    = odds_b
                model_prob  = prob_b
                market_prob = market_b

        if best_edge is None:
            continue

        # Odds movement filter: suppress if sharp money is against us
        try:
            from signals.odds_movement import should_suppress_signal, get_movement_for_bet
            if should_suppress_signal(match_id, bet_player):
                continue
            movement, odds_open, odds_close = get_movement_for_bet(match_id, bet_player)
        except Exception as e:
            logger.debug("[EdgeDetector] Movement check skipped for %s: %s", match_id, e)
            movement, odds_open, odds_close = "neutral", None, None

        # Save to DB
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

        # Update signal with movement data
        try:
            from database.db import get_conn
            conn = get_conn()
            conn.execute(
                "UPDATE signals SET odds_open=?, odds_close=?, movement=? WHERE id=?",
                (odds_open, odds_close, movement, signal_id),
            )
            conn.commit()
        except Exception as e:
            logger.debug("[EdgeDetector] Could not store movement for signal %d: %s", signal_id, e)

        signals.append({
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
            "odds_open":   odds_open,
            "odds_close":  odds_close,
            "movement":    movement,
        })

    logger.info(f"[EdgeDetector] Found {len(signals)} new edge(s).")
    return signals
