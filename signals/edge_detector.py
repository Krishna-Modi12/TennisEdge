"""
signals/edge_detector.py
Core engine: Dual Validation edge detection.

A signal fires ONLY when both conditions are met:
  1. Value Edge ≥ MIN_VALUE_EDGE  (bookmaker is underpricing the player)
  2. Model Prob ≥ MIN_MODEL_PROB  (model actually backs this player)
  3. Elo Agrees (Secondary Filter) (gap < 0.15 and same direction)

True Edge Score = Value Edge × Confidence
where Confidence = model_prob / (1 - model_prob)
"""

from config import MIN_VALUE_EDGE, MIN_MODEL_PROB, MIN_ODDS, MAX_ODDS
from models.advanced_model import advanced_predict as model_predict
from database.db import signal_exists, save_signal
from integrations.tennis_api import search_player


def odds_to_prob(odds: float) -> float:
    """Convert decimal odds to implied market probability."""
    if odds <= 0:
        return 0.0
    return round(1.0 / odds, 4)


def _de_vig_probs(odds_a: float, odds_b: float) -> tuple[float, float]:
    """Convert two-sided odds into no-vig probabilities."""
    if odds_a is None or odds_b is None or odds_a <= 1.0 or odds_b <= 1.0:
        return None, None
    pa_raw = 1.0 / odds_a
    pb_raw = 1.0 / odds_b
    total = pa_raw + pb_raw
    if total <= 0:
        return None, None
    return pa_raw / total, pb_raw / total


def calculate_true_edge(model_prob: float, decimal_odds: float,
                        min_value_edge: float = MIN_VALUE_EDGE,
                        min_model_prob: float = MIN_MODEL_PROB) -> dict:
    """
    Dual validation edge calculation.
    """
    implied_prob = 1.0 / decimal_odds
    value_edge = (model_prob * decimal_odds) - 1.0
    confidence = model_prob / (1.0 - model_prob) if model_prob < 1.0 else 99.0
    true_edge_score = value_edge * confidence

    signal_valid = (value_edge >= min_value_edge and model_prob >= min_model_prob)

    return {
        "implied_prob":    round(implied_prob, 4),
        "value_edge":      round(value_edge, 4),
        "confidence":      round(confidence, 3),
        "true_edge_score": round(true_edge_score, 4),
        "signal_valid":    signal_valid,
    }


from signals.calculator import process_matches

def detect_edges(matches: list) -> list:
    """
    Run dual-validation edge detection using the new Multi-Factor Weighting logic.
    """
    signals, _ = process_matches(matches)
    return signals



def _process_match(match: dict) -> dict:
    """Process a single match with Advanced Blended Model. Returns signal dict or None."""
    match_id   = match["match_id"]
    player_a   = match["player_a"]
    player_b   = match["player_b"]
    surface    = match.get("surface", "hard")
    tournament = match.get("tournament", "Unknown")
    
    odds_a     = float(match["odds_a"] or 0)
    odds_b     = float(match["odds_b"] or 0)

    if signal_exists(match_id):
        return None

    # Get model prediction (Blended: Elo, Form, H2H, Surface)
    model = model_predict(player_a, player_b, surface)
    prob_a, prob_b = model["prob_a"], model["prob_b"]

    a_in_range = MIN_ODDS <= odds_a <= MAX_ODDS
    b_in_range = MIN_ODDS <= odds_b <= MAX_ODDS
    if not a_in_range and not b_in_range:
        return None

    best_signal = None

    if a_in_range and odds_a > 0:
        te_a = calculate_true_edge(prob_a, odds_a)
        if te_a["signal_valid"]:
            best_signal = {
                "player": player_a, "odds": odds_a, "model_prob": prob_a,
                "market_prob": te_a["implied_prob"],
                **te_a,
                "elo_prob": model.get("elo_prob_a"),
            }

    if b_in_range and odds_b > 0:
        te_b = calculate_true_edge(prob_b, odds_b)
        if te_b["signal_valid"]:
            if best_signal is None or te_b["true_edge_score"] > best_signal["true_edge_score"]:
                best_signal = {
                    "player": player_b, "odds": odds_b, "model_prob": prob_b,
                    "market_prob": te_b["implied_prob"],
                    **te_b,
                    "elo_prob": 1.0 - model.get("elo_prob_a"),
                }

    if best_signal is None:
        return None

    signal_id = save_signal(
        match_id=match_id,
        tournament=tournament,
        surface=surface,
        player_a=player_a,
        player_b=player_b,
        bet_on=best_signal["player"],
        model_prob=best_signal["model_prob"],
        market_prob=best_signal["market_prob"],
        edge=best_signal["true_edge_score"],  # store true edge score as the edge
        odds=best_signal["odds"],
    )

    # Prepare factors for breakdown (correctly flipped for the chosen winner)
    is_a = (best_signal["player"] == player_a)
    factors = {
        "elo_prob":     model.get("elo_prob_a") if is_a else (1.0 - model.get("elo_prob_a", 0.5)),
        "form_prob":    model.get("form_prob_a") if is_a else (1.0 - model.get("form_prob_a", 0.5)),
        "surface_prob": model.get("surface_prob_a") if is_a else (1.0 - model.get("surface_prob_a", 0.5)),
        "h2h_prob":     model.get("h2h_prob_a") if is_a else (1.0 - model.get("h2h_prob_a", 0.5)),
        "h2h_wins_a":   model.get("h2h_wins_a", 0) if is_a else model.get("h2h_wins_b", 0),
        "h2h_wins_b":   model.get("h2h_wins_b", 0) if is_a else model.get("h2h_wins_a", 0),
    }

    return {
        "signal_id":        signal_id,
        "match_id":         match_id,
        "tournament":       tournament,
        "surface":          surface,
        "player_a":         player_a,
        "player_b":         player_b,
        "bet_on":           best_signal["player"],
        "model_prob":       best_signal["model_prob"],
        "market_prob":      best_signal["market_prob"],
        "odds":             best_signal["odds"],
        "value_edge":       best_signal["value_edge"],
        "confidence":       best_signal["confidence"],
        "true_edge_score":  best_signal["true_edge_score"],
        "edge":             best_signal["true_edge_score"],
        "data_quality":     model.get("data_quality", "partial"),
        **factors
    }
