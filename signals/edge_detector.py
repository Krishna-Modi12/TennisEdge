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
from models.elo_model import predict as elo_predict
from database.db import signal_exists, save_signal


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


def calculate_true_edge(model_prob: float, decimal_odds: float) -> dict:
    """
    Dual validation edge calculation.
    Returns value_edge, confidence, true_edge_score, and whether signal is valid based on thresholds.
    """
    implied_prob = 1.0 / decimal_odds
    value_edge = (model_prob * decimal_odds) - 1.0
    confidence = model_prob / (1.0 - model_prob) if model_prob < 1.0 else 99.0
    true_edge_score = value_edge * confidence

    signal_valid = (value_edge >= MIN_VALUE_EDGE and model_prob >= MIN_MODEL_PROB)

    return {
        "implied_prob":    round(implied_prob, 4),
        "value_edge":      round(value_edge, 4),
        "confidence":      round(confidence, 3),
        "true_edge_score": round(true_edge_score, 4),
        "signal_valid":    signal_valid,
    }


def detect_edges(matches: list) -> list:
    """
    Run dual-validation edge detection on a list of matches with odds.
    Returns list of signal dicts for matches that pass both validation gates.
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
    """Process a single match with Pinnacle + Elo confirmation. Returns signal dict or None."""
    match_id   = match["match_id"]
    player_a   = match["player_a"]
    player_b   = match["player_b"]
    surface    = match.get("surface", "hard")
    tournament = match.get("tournament", "Unknown")
    
    odds_a     = float(match["odds_a"] or 0)
    odds_b     = float(match["odds_b"] or 0)
    
    # Try Pinnacle odds first, fallback to max odds for implied prob if missing
    pinny_a = match.get("pinny_odds_a")
    pinny_b = match.get("pinny_odds_b")

    if signal_exists(match_id):
        return None

    a_in_range = MIN_ODDS <= odds_a <= MAX_ODDS
    b_in_range = MIN_ODDS <= odds_b <= MAX_ODDS
    if not a_in_range and not b_in_range:
        return None

    # Primary probability model: Pinnacle implied probability
    # If Pinnacle is missing, fallback to normal odds (which might include margin)
    prob_a, prob_b = _de_vig_probs(pinny_a, pinny_b)
    if prob_a is None:
        prob_a, prob_b = _de_vig_probs(odds_a, odds_b)
        
    if prob_a is None:
        return None

    # Secondary filter: point-in-time Elo
    elo = elo_predict(player_a, player_b, surface)
    elo_prob_a = elo["prob_a"]
    elo_prob_b = elo["prob_b"]

    MAX_ELO_DISAGREEMENT = 0.15

    # Dual validation for each side
    best_signal = None

    if a_in_range and odds_a > 0:
        te_a = calculate_true_edge(prob_a, odds_a)
        
        # Elo Filter Check for Player A
        gap_a = abs(elo_prob_a - prob_a)
        elo_agrees_a = (elo_prob_a > 0.5) == (prob_a > 0.5) and gap_a <= MAX_ELO_DISAGREEMENT
        
        if te_a["signal_valid"] and elo_agrees_a:
            best_signal = {
                "player": player_a, "odds": odds_a, "model_prob": prob_a,
                "market_prob": te_a["implied_prob"],
                **te_a,
                "elo_prob": elo_prob_a,
            }

    if b_in_range and odds_b > 0:
        te_b = calculate_true_edge(prob_b, odds_b)
        
        # Elo Filter Check for Player B
        gap_b = abs(elo_prob_b - prob_b)
        elo_agrees_b = (elo_prob_b > 0.5) == (prob_b > 0.5) and gap_b <= MAX_ELO_DISAGREEMENT

        if te_b["signal_valid"] and elo_agrees_b:
            if best_signal is None or te_b["true_edge_score"] > best_signal["true_edge_score"]:
                best_signal = {
                    "player": player_b, "odds": odds_b, "model_prob": prob_b,
                    "market_prob": te_b["implied_prob"],
                    **te_b,
                    "elo_prob": elo_prob_b,
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
        # Elo verification stats
        "elo_prob":         best_signal.get("elo_prob"),
        "data_quality":     "Pinnacle+Elo",
    }
