"""
tennis_backtest/paper_trading/weekly_summary.py
Weekly paper-trading summary including CLV coverage and distribution.
"""

from __future__ import annotations

from clv_tracker import calculate_clv
from database.db import get_signal_accuracy


def generate_weekly_summary() -> str:
    db_note = None
    try:
        acc = get_signal_accuracy()
    except Exception as e:
        acc = {
            "tracked_results": 0,
            "wins": 0,
            "losses": 0,
            "accuracy_pct": 0.0,
            "roi_pct": 0.0,
            "avg_odds": 0.0,
        }
        db_note = f"Database unavailable: {e}"
    clv = calculate_clv()

    lines = [
        "WEEKLY PAPER TRADING SUMMARY",
        "============================",
        f"Tracked Results: {acc.get('tracked_results', 0)}",
        f"Wins / Losses: {acc.get('wins', 0)} / {acc.get('losses', 0)}",
        f"Accuracy: {acc.get('accuracy_pct', 0.0)}%",
        f"ROI: {acc.get('roi_pct', 0.0)}%",
        f"Avg Odds: {acc.get('avg_odds', 0.0)}",
        "",
        "CLV",
        "---",
        f"Coverage: {clv['with_closing_odds']}/{clv['total_resolved']} ({clv['coverage_pct']}%)",
        f"Positive / Negative / Zero: {clv['positive']} / {clv['negative']} / {clv['zero']}",
        f"Average CLV: {clv['avg_clv']:+.4f}",
    ]

    if db_note:
        lines.append("")
        lines.append(f"Note: {db_note}")

    if clv["by_surface"]:
        lines.append("")
        lines.append("CLV by Surface:")
        for row in clv["by_surface"]:
            lines.append(
                f"- {row['surface']}: n={row['count']}, +={row['positive']}, "
                f"-={row['negative']}, 0={row['zero']}, avg={row['avg_clv']:+.4f}"
            )
    elif clv.get("note"):
        lines.append("")
        lines.append(f"CLV Note: {clv['note']}")

    return "\n".join(lines)


def main():
    print(generate_weekly_summary())


if __name__ == "__main__":
    main()
