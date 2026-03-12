"""
ml/features.py
Feature engineering for the secondary ML validation layer.
"""

import pandas as pd
import numpy as np

def extract_features_from_signal(signal_data: dict) -> pd.DataFrame:
    """
    Convert a signal dictionary (historical or live) into a feature row.
    """
    def f(val, default=0.5):
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    # Required keys: surface, model_prob, market_prob, odds, volatility, elo_component_prob, mc_component_prob
    features = {
        "surface_hard": 1 if signal_data.get("surface") == "hard" else 0,
        "surface_clay": 1 if signal_data.get("surface") == "clay" else 0,
        "surface_grass": 1 if signal_data.get("surface") == "grass" else 0,
        "model_prob": f(signal_data.get("model_prob")),
        "market_prob": f(signal_data.get("market_prob")),
        "market_gap": f(signal_data.get("model_prob")) - f(signal_data.get("market_prob")),
        "odds": f(signal_data.get("odds"), 2.0),
        "volatility": f(signal_data.get("volatility"), 0.0),
        "elo_prob": f(signal_data.get("elo_component_prob")),
        "strength_prob": f(signal_data.get("strength_component_prob")),
        "mc_prob": f(signal_data.get("mc_component_prob")),
        "pinnacle_prob": f(signal_data.get("market_component_prob")),
        "ensemble_prob": f(signal_data.get("ensemble_prob")),
        "value_edge": f(signal_data.get("edge"), 0.0),
    }
    
    # Interaction features
    features["edge_to_vol"] = features["value_edge"] / (features["volatility"] + 0.01)
    features["prob_diff_elo_market"] = features["elo_prob"] - features["market_prob"]

    return pd.DataFrame([features])

def prepare_training_dataset(signals_list: list) -> tuple[pd.DataFrame, pd.Series]:
    """
    Convert list of signal dicts from DB into X, y for training.
    """
    frames = []
    labels = []
    for s in signals_list:
        df = extract_features_from_signal(s)
        frames.append(df)
        labels.append(s.get("label", 0))
    
    X = pd.concat(frames, ignore_index=True)
    y = pd.Series(labels)
    return X, y
