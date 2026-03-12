"""
optimization/hyperparameter_search.py
Automated grid search for optimal model hyperparameters using historical backtests.
"""

import logging
import json
from itertools import product
from datetime import datetime
from backtesting.backtest_engine import run_historical_backtest
from database.db import save_model_optimization_result

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
MAX_CONFIGS = 200

# --- PARAMETER GRID ---
PARAM_GRID = {
    "min_value_edge": [0.03, 0.04, 0.05, 0.06],
    "min_model_prob": [0.35, 0.40, 0.45],
    "min_elo_prob": [0.50, 0.55],
    "stake": [100.0]
}

def run_optimization(start_year: int = 2023, end_year: int = 2024, run_name: str = "opt_run"):
    """
    Search parameter combinations and score results.
    """
    keys = list(PARAM_GRID.keys())
    values = list(PARAM_GRID.values())
    combinations = list(product(*values))
    
    if len(combinations) > MAX_CONFIGS:
        logger.warning(f"Truncating optimization combinations from {len(combinations)} to {MAX_CONFIGS}")
        combinations = combinations[:MAX_CONFIGS]

    results = []
    best_score = -float('inf')
    best_params = None

    logger.info(f"Starting optimization for {len(combinations)} combinations...")

    for i, combo in enumerate(combinations):
        params = dict(zip(keys, combo))
        logger.info(f"Trial {i+1}/{len(combinations)}: {params}")

        # Run backtest with current hyperparams
        backtest_result = run_historical_backtest(
            run_name=f"{run_name}_trial_{i}",
            atp_from=start_year,
            atp_to=end_year,
            wta_from=start_year,
            wta_to=end_year,
            stake=params["stake"],
            min_value_edge=params["min_value_edge"],
            min_model_prob=params["min_model_prob"],
            min_elo_prob=params["min_elo_prob"],
            persist=False # Don't clutter backtest_results table
        )

        if "message" in backtest_result and "No valid signals" in backtest_result["message"]:
            logger.debug(f"Trial {i+1} yielded no signals.")
            continue

        # Score calculation: ROI * log10(Bets + 1) to balance return and volume
        # Or simple: roi + (avg_clv - 1) * 10 if CLV was available.
        # Since backtest_engine currently doesn't return CLV, we use ROI and win rate.
        roi = float(backtest_result.get("roi", "0.0%").replace("%", ""))
        win_rate = float(backtest_result.get("win_rate", "0.0%").replace("%", ""))
        num_bets = backtest_result.get("total_bets", 0)
        
        # Scoring metric: ROI * volume factor
        score = roi * (num_bets / 100.0) # Normalized score

        results.append({
            "params": params,
            "roi": roi,
            "win_rate": win_rate,
            "bets": num_bets,
            "score": score
        })

        # Save to DB
        save_model_optimization_result(
            run_name=run_name,
            params=params,
            score=score,
            roi_pct=roi,
            win_rate_pct=win_rate,
            notes=f"Bets: {num_bets}"
        )

        if score > best_score:
            best_score = score
            best_params = params

    if best_params:
        logger.info(f"OPTIMIZATION COMPLETE. Best Score: {best_score:.2f}")
        logger.info(f"Best Params: {best_params}")
        
        with open("config_optimized.json", "w") as f:
            json.dump(best_params, f, indent=4)
            
    return best_params

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_optimization()
