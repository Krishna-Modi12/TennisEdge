"""
models/ensemble_model.py
Weighted ensemble combiner for model probabilities.
"""

from __future__ import annotations


DEFAULT_WEIGHTS = {
    "elo": 0.30,
    "strength": 0.20,
    "mc": 0.20,
    "market": 0.15,
    "ml": 0.15,
}


def _clamp(prob: float) -> float:
    return max(0.01, min(0.99, float(prob)))


def _get_runtime_weights() -> dict:
    weights = dict(DEFAULT_WEIGHTS)
    try:
        from database.db import get_model_parameter

        custom = get_model_parameter("ensemble_weights", None)
        if isinstance(custom, dict):
            for key in ("elo", "strength", "mc", "market", "ml"):
                if key in custom:
                    try:
                        weights[key] = max(0.0, float(custom[key]))
                    except (TypeError, ValueError):
                        continue
    except Exception:
        pass
    return weights


def combine_ensemble_probability(
    elo_prob: float,
    strength_prob: float | None = None,
    mc_prob: float | None = None,
    market_prob: float | None = None,
    ml_prob: float | None = None,
) -> tuple[float, dict]:
    """
    Combine available component probabilities with normalized configured weights.
    Returns (ensemble_prob, component_payload).
    """
    w = _get_runtime_weights()
    components = [("elo_prob", _clamp(elo_prob), w["elo"])]
    if strength_prob is not None:
        components.append(("strength_prob", _clamp(strength_prob), w["strength"]))
    if mc_prob is not None:
        components.append(("mc_prob", _clamp(mc_prob), w["mc"]))
    if market_prob is not None:
        components.append(("market_prob", _clamp(market_prob), w["market"]))
    if ml_prob is not None:
        components.append(("ml_prob", _clamp(ml_prob), w["ml"]))

    total_w = sum(weight for _, _, weight in components)
    if total_w <= 0:
        return _clamp(elo_prob), {"elo_prob": _clamp(elo_prob)}

    ensemble = sum(prob * (weight / total_w) for _, prob, weight in components)
    payload = {name: prob for name, prob, _ in components}
    payload["ensemble_prob"] = _clamp(ensemble)
    return _clamp(ensemble), payload
