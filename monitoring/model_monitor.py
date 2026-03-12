"""
monitoring/model_monitor.py
Rolling performance analytics and adaptive parameter tuning.
"""

from __future__ import annotations

from database.db import (
    get_model_parameter,
    get_recent_signal_performance,
    set_model_parameter,
)


PARAM_EDGE_THRESHOLD = "dynamic_edge_base"
PARAM_KELLY_MULTIPLIER = "kelly_multiplier"


def calculate_recent_model_performance(window: int = 100) -> dict:
    rows = get_recent_signal_performance(limit=window)
    resolved = [r for r in rows if r.get("is_win") is not None]
    total = len(resolved)
    if total == 0:
        return {
            "sample_size": 0,
            "roi_pct": 0.0,
            "win_rate_pct": 0.0,
            "avg_clv_ratio": None,
            "avg_line_movement": None,
        }

    pnl = 0.0
    wins = 0
    clv_vals = []
    lm_vals = []
    for row in resolved:
        odds = float(row.get("taken_odds") or 0.0)
        is_win = bool(row["is_win"])
        if is_win:
            pnl += max(0.0, odds - 1.0)
            wins += 1
        else:
            pnl -= 1.0
        if row.get("clv_ratio") is not None:
            clv_vals.append(float(row["clv_ratio"]))
        if row.get("line_movement") is not None:
            lm_vals.append(float(row["line_movement"]))

    return {
        "sample_size": total,
        "roi_pct": (pnl / total) * 100.0,
        "win_rate_pct": (wins / total) * 100.0,
        "avg_clv_ratio": (sum(clv_vals) / len(clv_vals)) if clv_vals else None,
        "avg_line_movement": (sum(lm_vals) / len(lm_vals)) if lm_vals else None,
    }


def apply_adaptive_tuning(window: int = 100) -> dict:
    """
    Adaptive safety tuning:
    - If rolling ROI < 0: reduce Kelly multiplier by 10% (floor 0.25).
    - If rolling CLV < 1.00: increase base edge threshold by 0.0025 (cap 0.08).
    """
    perf = calculate_recent_model_performance(window=window)
    edge_base = float(get_model_parameter(PARAM_EDGE_THRESHOLD, 0.04))
    kelly_mult = float(get_model_parameter(PARAM_KELLY_MULTIPLIER, 1.0))

    changed = False
    if perf["sample_size"] > 0 and perf["roi_pct"] < 0:
        kelly_mult = max(0.25, kelly_mult * 0.90)
        set_model_parameter(PARAM_KELLY_MULTIPLIER, kelly_mult)
        changed = True

    avg_clv = perf.get("avg_clv_ratio")
    if avg_clv is not None and avg_clv < 1.0:
        edge_base = min(0.08, edge_base + 0.0025)
        set_model_parameter(PARAM_EDGE_THRESHOLD, edge_base)
        changed = True

    snapshot = {
        **perf,
        "edge_threshold_base": edge_base,
        "kelly_multiplier": kelly_mult,
        "changed": changed,
    }
    set_model_parameter("last_monitor_snapshot", snapshot)
    return snapshot
