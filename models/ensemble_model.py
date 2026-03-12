"""
models/ensemble_model.py
Weighted ensemble combiner for model probabilities.
"""

from __future__ import annotations


W_ELO = 0.35
W_STRENGTH = 0.25
W_MC = 0.25
W_MARKET = 0.15


def _clamp(prob: float) -> float:
    return max(0.01, min(0.99, float(prob)))


def combine_ensemble_probability(
    elo_prob: float,
    strength_prob: float | None = None,
    mc_prob: float | None = None,
    market_prob: float | None = None,
) -> tuple[float, dict]:
    """
    Combine available component probabilities with normalized configured weights.
    Returns (ensemble_prob, component_payload).
    """
    components = [("elo_prob", _clamp(elo_prob), W_ELO)]
    if strength_prob is not None:
        components.append(("strength_prob", _clamp(strength_prob), W_STRENGTH))
    if mc_prob is not None:
        components.append(("mc_prob", _clamp(mc_prob), W_MC))
    if market_prob is not None:
        components.append(("market_prob", _clamp(market_prob), W_MARKET))

    total_w = sum(weight for _, _, weight in components)
    if total_w <= 0:
        return _clamp(elo_prob), {"elo_prob": _clamp(elo_prob)}

    ensemble = sum(prob * (weight / total_w) for _, prob, weight in components)
    payload = {name: prob for name, prob, _ in components}
    payload["ensemble_prob"] = _clamp(ensemble)
    return _clamp(ensemble), payload
