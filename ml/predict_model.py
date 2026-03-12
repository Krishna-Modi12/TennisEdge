"""
ml/predict_model.py
Inference layer for the TennisEdge ML prediction layer.
"""

import os
import joblib
import logging
import pandas as pd
from ml.features import extract_features_from_signal

logger = logging.getLogger(__name__)

MODEL_PATH = os.path.join(os.path.dirname(__file__), "match_model.joblib")
_cached_model = None

from ml.predictor import XGBManager

def predict_match_ml_probability(signal_data: dict) -> float | None:
    """
    Predict win probability using the trained ML model.
    Falls back to None if model is unavailable.
    """
    try:
        # Use our new XGBManager which handles loading and feature extraction
        prob = XGBManager.inference(signal_data)
        return prob
    except Exception as e:
        logger.warning(f"ML prediction failed: {e}")
    return None
