"""
ml/odds_movement_features.py
Time-series feature extraction for predicting odds shortening.
"""

import pandas as pd
import numpy as np

def extract_odds_movement_features(odds_history: list[dict]) -> dict:
    """
    Extract features from a list of odds snapshots (oldest to newest).
    """
    if not odds_history or len(odds_history) < 2:
        return {
            "odds_slope_a": 0.0,
            "odds_slope_b": 0.0,
            "odds_volatility_a": 0.0,
            "odds_volatility_b": 0.0,
            "odds_change_1h_a": 0.0,
            "odds_change_1h_b": 0.0,
            "is_shortening_a": 0,
            "is_shortening_b": 0
        }

    # Order snapshots oldest to newest for diffs/slopes
    # assuming incoming list is newest to oldest from DB
    df = pd.DataFrame(reversed(odds_history))
    
    # Simple differentials
    df['diff_a'] = df['odds_a'].diff()
    df['diff_b'] = df['odds_b'].diff()
    
    # Percent change
    df['pct_a'] = df['odds_a'].pct_change()
    df['pct_b'] = df['odds_b'].pct_change()

    features = {
        "odds_slope_a": float(df['pct_a'].mean() or 0.0),
        "odds_slope_b": float(df['pct_b'].mean() or 0.0),
        "odds_volatility_a": float(df['pct_a'].std() or 0.0),
        "odds_volatility_b": float(df['pct_b'].std() or 0.0),
        "odds_change_total_a": float((df['odds_a'].iloc[-1] / df['odds_a'].iloc[0]) - 1.0),
        "odds_change_total_b": float((df['odds_b'].iloc[-1] / df['odds_b'].iloc[0]) - 1.0),
    }

    # Binary flag: Is it shortening now? (Last change was negative)
    features["is_shortening_a"] = 1 if df['pct_a'].iloc[-1] < 0 else 0
    features["is_shortening_b"] = 1 if df['pct_b'].iloc[-1] < 0 else 0

    return features
