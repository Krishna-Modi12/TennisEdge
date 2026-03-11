"""
tennis_backtest/step6_backtest_v2.py
Sprint-2 backtest wrapper with validation checks.
"""

from __future__ import annotations

import argparse
import json

from backtest.engine import backtest


def _parse_pct(s: str) -> float:
    if isinstance(s, str) and s.endswith("%"):
        return float(s[:-1])
    return float(s)


def _validate(result: dict) -> list[str]:
    issues = []
    if "message" in result:
        issues.append(result["message"])
        return issues

    avg_true_edge = _parse_pct(result.get("avg_true_edge", "0%"))
    roi = _parse_pct(result.get("roi", "0%"))
    bets = int(result.get("total_bets", 0))
    if avg_true_edge < 5.0 or avg_true_edge > 20.0:
        issues.append(f"avg_true_edge out of target range (5-20): {avg_true_edge:.2f}%")
    if bets < 100:
        issues.append(f"sample size low (<100 bets): {bets}")
    if roi <= 0:
        issues.append(f"non-positive ROI: {roi:.2f}%")
    return issues


def _parse_args():
    p = argparse.ArgumentParser(description="Run Sprint-2 backtest v2 with checks.")
    p.add_argument("--atp-from", type=int, default=2023)
    p.add_argument("--atp-to", type=int, default=2024)
    p.add_argument("--wta-from", type=int, default=2023)
    p.add_argument("--wta-to", type=int, default=2024)
    p.add_argument("--stake", type=float, default=100.0)
    p.add_argument("--min-value-edge", type=float, default=0.04)
    p.add_argument("--min-model-prob", type=float, default=0.35)
    p.add_argument("--min-elo-prob", type=float, default=0.50)
    p.add_argument("--json", action="store_true")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    result = backtest(
        atp_from=args.atp_from,
        atp_to=args.atp_to,
        wta_from=args.wta_from,
        wta_to=args.wta_to,
        stake=args.stake,
        min_value_edge=args.min_value_edge,
        min_model_prob=args.min_model_prob,
        min_elo_prob=args.min_elo_prob,
    )
    issues = _validate(result)

    if args.json:
        print(json.dumps({"result": result, "issues": issues}, indent=2))
    else:
        print("[step6] Result summary:")
        for k, v in result.items():
            if k in ("monthly", "by_surface"):
                continue
            print(f"  {k}: {v}")
        if result.get("by_surface"):
            print("[step6] By surface:")
            for s in result["by_surface"]:
                print(
                    f"  {s['surface']}: bets={s['bets']}, "
                    f"wr={s['win_rate']}%, roi={s['roi']}%"
                )
        if issues:
            print("[step6] Validation issues:")
            for i in issues:
                print(f"  - {i}")
        else:
            print("[step6] Validation checks passed.")
    return 0 if not issues else 1


if __name__ == "__main__":
    raise SystemExit(main())
