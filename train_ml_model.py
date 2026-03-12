"""
train_ml_model.py
CLI tool to retrain the ML prediction layer from historical signal data.
"""

import argparse
import logging
from ml.train_model import train_ml_model

def main():
    parser = argparse.ArgumentParser(description="Retrain TennisEdge ML validation model")
    parser.add_argument("--limit", type=int, default=5000, help="Max signals to use for training")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🧠 Starting ML model retraining from historical signals...")
    success = train_ml_model(limit=args.limit)
    
    if success:
        print("\n✅ ML TRAINING SUCCESS")
        print("The system will now use the updated model for live signal validation.")
    else:
        print("\n❌ ML TRAINING FAILED")
        print("Check logs for details. Usually caused by insufficient resolved signals in database.")

if __name__ == "__main__":
    main()
