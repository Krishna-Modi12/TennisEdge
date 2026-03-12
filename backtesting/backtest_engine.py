"""
backtesting/backtest_engine.py
Chronological backtest runner with persistence.
"""

from __future__ import annotations

from backtest.engine import backtest as run_core_backtest
from database.db import save_backtest_result


def _pct_to_float(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def run_historical_backtest(
    run_name: str = "default_backtest",
    atp_from: int = 2023,
    atp_to: int = 2024,
    wta_from: int = 2023,
    wta_to: int = 2024,
    stake: float = 100.0,
    min_value_edge: float = 0.04,
    min_model_prob: float = 0.35,
    min_elo_prob: float = 0.50,
    persist: bool = True,
) -> dict:
    result = run_core_backtest(
        atp_from=atp_from,
        atp_to=atp_to,
        wta_from=wta_from,
        wta_to=wta_to,
        stake=stake,
        min_value_edge=min_value_edge,
        min_model_prob=min_model_prob,
        min_elo_prob=min_elo_prob,
    )

    if "message" in result:
        return result

    roi_pct = _pct_to_float(result.get("roi"))
    win_rate_pct = _pct_to_float(result.get("win_rate"))
    bankroll_start = float(result.get("total_staked", 0.0))
    bankroll_end = bankroll_start + float(result.get("total_profit", 0.0))

    if persist:
        backtest_id = save_backtest_result(
            run_name=run_name,
            roi_pct=roi_pct,
            win_rate_pct=win_rate_pct,
            clv_avg=None,
            bankroll_start=bankroll_start,
            bankroll_end=bankroll_end,
            summary=result,
        )
        result["backtest_result_id"] = backtest_id

    return result
