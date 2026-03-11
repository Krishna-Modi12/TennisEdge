"""
paper_trading_status.py
Operational status report for paper trading progress and 100-bet validation clock.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from clv_tracker import calculate_clv
from database.db import get_conn, get_signal_accuracy


def get_paper_trading_status(target_bets: int = 100) -> dict:
    try:
        conn = get_conn()
        cur = conn.execute(
            """
            SELECT
              COUNT(*) AS total_signals,
              SUM(CASE WHEN result = 'pending' THEN 1 ELSE 0 END) AS pending,
              SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
              SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
              SUM(CASE WHEN result = 'push' THEN 1 ELSE 0 END) AS pushes,
              MIN(created_at) AS first_signal_at,
              MAX(created_at) AS last_signal_at
            FROM signals
            """
        )
        row = cur.fetchone() or (0, 0, 0, 0, 0, None, None)
    except Exception as e:
        return {
            "ok": False,
            "error": f"Database unavailable: {e}",
        }

    total_signals = int(row[0] or 0)
    pending = int(row[1] or 0)
    wins = int(row[2] or 0)
    losses = int(row[3] or 0)
    pushes = int(row[4] or 0)
    first_signal_at = row[5]
    last_signal_at = row[6]

    accuracy = get_signal_accuracy()
    tracked_bets = int(accuracy.get("tracked_results", 0))  # win/loss only
    progress_pct = round((tracked_bets / target_bets) * 100, 1) if target_bets > 0 else 0.0
    remaining = max(0, target_bets - tracked_bets)

    clv = calculate_clv()

    return {
        "ok": True,
        "generated_signals": total_signals,
        "pending_signals": pending,
        "resolved": {
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
        },
        "tracked_bets": tracked_bets,
        "target_bets": target_bets,
        "remaining_to_target": remaining,
        "progress_pct": progress_pct,
        "accuracy_pct": accuracy.get("accuracy_pct", 0.0),
        "roi_pct": accuracy.get("roi_pct", 0.0),
        "first_signal_at": str(first_signal_at) if first_signal_at else None,
        "last_signal_at": str(last_signal_at) if last_signal_at else None,
        "clv": {
            "source": clv.get("source"),
            "coverage_pct": clv.get("coverage_pct", 0.0),
            "avg_clv": clv.get("avg_clv", 0.0),
            "positive": clv.get("positive", 0),
            "negative": clv.get("negative", 0),
            "zero": clv.get("zero", 0),
            "note": clv.get("note"),
        },
    }


def _format_report(status: dict) -> str:
    if not status.get("ok"):
        return f"PAPER TRADING STATUS\n====================\nERROR: {status.get('error')}"

    lines = [
        "PAPER TRADING STATUS",
        "====================",
        f"Generated Signals: {status['generated_signals']}",
        f"Pending Signals: {status['pending_signals']}",
        f"Resolved: W {status['resolved']['wins']} | L {status['resolved']['losses']} | P {status['resolved']['pushes']}",
        "",
        f"100-Bet Clock: {status['tracked_bets']}/{status['target_bets']} ({status['progress_pct']}%)",
        f"Remaining To Target: {status['remaining_to_target']}",
        f"Accuracy: {status['accuracy_pct']}%",
        f"ROI: {status['roi_pct']}%",
        "",
        f"First Signal At: {status['first_signal_at'] or 'N/A'}",
        f"Last Signal At: {status['last_signal_at'] or 'N/A'}",
        "",
        "CLV",
        "---",
        f"Source: {status['clv']['source']}",
        f"Coverage: {status['clv']['coverage_pct']}%",
        f"Avg CLV: {status['clv']['avg_clv']:+.4f}",
        f"Positive/Negative/Zero: {status['clv']['positive']}/{status['clv']['negative']}/{status['clv']['zero']}",
    ]

    if status["clv"].get("note"):
        lines.append(f"CLV Note: {status['clv']['note']}")

    if status["tracked_bets"] == 0:
        lines.append("")
        lines.append("Action: no resolved paper bets yet. Ensure scheduler is running and trigger /scan as admin.")

    return "\n".join(lines)


def _parse_args():
    p = argparse.ArgumentParser(description="Show paper trading status and 100-bet progress.")
    p.add_argument("--target-bets", type=int, default=100)
    p.add_argument("--json", action="store_true", help="Print JSON output")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    status = get_paper_trading_status(target_bets=args.target_bets)
    if args.json:
        print(json.dumps(status, indent=2))
    else:
        print(_format_report(status))
    return 0 if status.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
