"""
ml/train_model.py
Training pipeline for the TennisEdge ML prediction layer.
"""

import os
import joblib
import logging
from database.db import get_ml_training_signals
from ml.features import prepare_training_dataset

logger = logging.getLogger(__name__)

# Try to import XGBoost, fallback to Scikit-Learn
try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    from sklearn.ensemble import GradientBoostingClassifier
    HAS_XGB = False

MODEL_PATH = os.path.join(os.path.dirname(__file__), "match_model.joblib")

def train_ml_model(limit: int = 2000):
    """
    Fetch historical signals, train model, and save to disk.
    """
    logger.info("Fetching signals for ML training...")
    try:
        signals = get_ml_training_signals(limit=limit)
    except Exception as e:
        logger.error(f"Failed to fetch training signals: {e}")
        return False

    if len(signals) < 100:
        logger.warning(f"Insufficient training data ({len(signals)} signals). Need at least 100.")
        return False

    logger.info(f"Preparing dataset with {len(signals)} matches...")
    X, y = prepare_training_dataset(signals)

    logger.info(f"Training ML model (XGBoost={HAS_XGB})...")
    
    if HAS_XGB:
        model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.05,
            objective='binary:logistic',
            random_state=42
        )
    else:
        model = GradientBoostingClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.05,
            random_state=42
        )

    model.fit(X, y)
    
    joblib.dump(model, MODEL_PATH)
    logger.info(f"Model saved to {MODEL_PATH}")
    
    # Feature importance logging
    if HAS_XGB:
        importance = model.get_booster().get_score(importance_type='weight')
        logger.info(f"Top features: {sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]}")
    
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    train_ml_model()
