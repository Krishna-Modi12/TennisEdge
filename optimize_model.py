"""
optimize_model.py
CLI tool to run hyperparameter optimization runs.
"""

import argparse
import logging
from optimization.hyperparameter_search import run_optimization

def main():
    parser = argparse.ArgumentParser(description="Run TennisEdge hyperparameter optimization")
    parser.add_argument("--start", type=int, default=2023, help="Start year for backtesting")
    parser.add_argument("--end", type=int, default=2024, help="End year for backtesting")
    parser.add_argument("--name", type=str, default="nightly_optimizer", help="Run name for DB")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print(f"🚀 Starting model optimization: {args.name} ({args.start}-{args.end})")
    best = run_optimization(start_year=args.start, end_year=args.end, run_name=args.name)
    
    if best:
        print("\n🏆 OPTIMIZATION SUCCESS")
        print(f"Best Configuration: {best}")
        print("Optimized parameters saved to 'config_optimized.json'")
    else:
        print("\n⚠️ No optimal configuration found (insufficient signals).")

if __name__ == "__main__":
    main()
