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

from config import MIN_VALUE_EDGE, MIN_MODEL_PROB


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
    return sorted(signals, key=lambda s: s.get("ev_score", float("-inf")), reverse=True)
