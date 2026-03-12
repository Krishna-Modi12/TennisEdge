"""
ml/predictor.py
XGBManager for handling ML inference and model loading.
"""

import os
import joblib
import logging
import pandas as pd
from ml.features import extract_features_from_signal

logger = logging.getLogger(__name__)

class XGBManager:
    _model = None
    _model_path = os.path.join(os.path.dirname(__file__), "match_model.joblib")

    @classmethod
    def _load_model(cls):
        if cls._model is None:
            if os.path.exists(cls._model_path):
                try:
                    cls._model = joblib.load(cls._model_path)
                    logger.info(f"ML model loaded from {cls._model_path}")
                except Exception as e:
                    logger.error(f"Failed to load ML model: {e}")
            else:
                logger.debug(f"ML model not found at {cls._model_path}")
        return cls._model

    @classmethod
    def inference(cls, signal_data: dict) -> float | None:
        """
        Run inference on a single signal's data.
        Returns win probability or None if model/loading fails.
        """
        model = cls._load_model()
        if model is None:
            return None
            
        try:
            X = extract_features_from_signal(signal_data)
            # predict_proba returns [prob_loss, prob_win]
            probs = model.predict_proba(X)
            return float(probs[0][1])
        except Exception as e:
            logger.warning(f"Inference failed: {e}")
            return None
