"""
backtesting/backtest_report.py
Backtest reporting helpers.
"""

from __future__ import annotations

from backtest.engine import format_backtest_report as format_core_backtest_report


def format_backtest_summary(result: dict) -> str:
    text = format_core_backtest_report(result)
    if result.get("backtest_result_id"):
        text += f"\n\n🧾 Backtest Result ID: {result['backtest_result_id']}"
    return text
