"""
tennis_backtest/threshold_sweep_v2.py
Sprint-2 threshold optimizer. Sweeps edge/prob cutoffs and finds highest ROI.
"""

from __future__ import annotations

import argparse
import pandas as pd
from backtest.engine import backtest

def main():
    parser = argparse.ArgumentParser(description="Sweep backtest thresholds.")
    parser.add_argument("--atp-from", type=int, default=2023)
    parser.add_argument("--atp-to", type=int, default=2024)
    parser.add_argument("--min-edge-start", type=float, default=0.02)
    parser.add_argument("--min-edge-end", type=float, default=0.10)
    parser.add_argument("--min-edge-step", type=float, default=0.01)
    parser.add_argument("--min-prob-start", type=float, default=0.30)
    parser.add_argument("--min-prob-end", type=float, default=0.55)
    parser.add_argument("--min-prob-step", type=float, default=0.05)
    parser.add_argument("--output", default="tennis_backtest/out/threshold_sweep.csv")
    args = parser.parse_args()

    results = []
    
    edge = args.min_edge_start
    while edge <= args.min_edge_end + 0.0001:
        prob = args.min_prob_start
        while prob <= args.min_prob_end + 0.0001:
            print(f"[sweep] Testing edge={edge:.1%}, prob={prob:.1%}...")
            res = backtest(
                atp_from=args.atp_from,
                atp_to=args.atp_to,
                min_value_edge=edge,
                min_model_prob=prob,
                min_elo_prob=0.50 # This is now the "agreement" filter
            )
            
            # Extract ROI as float
            roi_str = res.get("roi", "0%")
            roi = float(roi_str.replace("%", ""))
            
            results.append({
                "min_value_edge": edge,
                "min_model_prob": prob,
                "roi": roi,
                "win_rate": res.get("win_rate", "0%"),
                "total_bets": res.get("total_bets", 0),
                "profit": res.get("total_profit", 0)
            })
            prob += args.min_prob_step
        edge += args.min_edge_step

    df = pd.DataFrame(results)
    df.to_csv(args.output, index=False)
    print(f"[sweep] Completed. Best ROI config:")
    if not df.empty:
        best = df.sort_values("roi", ascending=False).iloc[0]
        print(best)
    print(f"[sweep] Results saved to {args.output}")

if __name__ == "__main__":
    main()
