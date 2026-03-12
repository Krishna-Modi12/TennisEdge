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
EDGE_BASE_MIN = 0.03
EDGE_BASE_MAX = 0.06
KELLY_MULT_MIN = 0.25
KELLY_MULT_MAX = 1.25


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, float(value)))


def _read_numeric_parameter(name: str, default: float) -> float:
    try:
        return float(get_model_parameter(name, default))
    except Exception:
        return float(default)


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
    pre_bounds = enforce_parameter_safety_bounds()
    perf = calculate_recent_model_performance(window=window)
    edge_base = _read_numeric_parameter(PARAM_EDGE_THRESHOLD, 0.04)
    kelly_mult = _read_numeric_parameter(PARAM_KELLY_MULTIPLIER, 1.0)
    edge_base = _clamp(edge_base, EDGE_BASE_MIN, EDGE_BASE_MAX)
    kelly_mult = _clamp(kelly_mult, KELLY_MULT_MIN, KELLY_MULT_MAX)

    changed = False
    if perf["sample_size"] > 0 and perf["roi_pct"] < 0:
        kelly_mult = _clamp(kelly_mult * 0.90, KELLY_MULT_MIN, KELLY_MULT_MAX)
        set_model_parameter(PARAM_KELLY_MULTIPLIER, kelly_mult)
        changed = True

    avg_clv = perf.get("avg_clv_ratio")
    if avg_clv is not None and avg_clv < 1.0:
        edge_base = _clamp(edge_base + 0.0025, EDGE_BASE_MIN, EDGE_BASE_MAX)
        set_model_parameter(PARAM_EDGE_THRESHOLD, edge_base)
        changed = True

    post_bounds = enforce_parameter_safety_bounds()
    snapshot = {
        **perf,
        "edge_threshold_base": edge_base,
        "kelly_multiplier": kelly_mult,
        "changed": changed or pre_bounds["changed"] or post_bounds["changed"],
        "safety": post_bounds,
    }
    set_model_parameter("last_monitor_snapshot", snapshot)
    return snapshot


def enforce_parameter_safety_bounds() -> dict:
    """
    Clamp adaptive parameters into safe operating bounds.
    """
    edge_raw = _read_numeric_parameter(PARAM_EDGE_THRESHOLD, 0.04)
    kelly_raw = _read_numeric_parameter(PARAM_KELLY_MULTIPLIER, 1.0)
    edge_base = _clamp(edge_raw, EDGE_BASE_MIN, EDGE_BASE_MAX)
    kelly_mult = _clamp(kelly_raw, KELLY_MULT_MIN, KELLY_MULT_MAX)

    changed = False
    if edge_base != edge_raw:
        set_model_parameter(PARAM_EDGE_THRESHOLD, edge_base)
        changed = True
    if kelly_mult != kelly_raw:
        set_model_parameter(PARAM_KELLY_MULTIPLIER, kelly_mult)
        changed = True

    return {
        "edge_threshold_base": edge_base,
        "kelly_multiplier": kelly_mult,
        "edge_base_min": EDGE_BASE_MIN,
        "edge_base_max": EDGE_BASE_MAX,
        "kelly_min": KELLY_MULT_MIN,
        "kelly_max": KELLY_MULT_MAX,
        "changed": changed,
    }


def get_parameter_safety_report() -> dict:
    """Read current adaptive parameters and report safety status."""
    edge_base_raw = _read_numeric_parameter(PARAM_EDGE_THRESHOLD, 0.04)
    kelly_raw = _read_numeric_parameter(PARAM_KELLY_MULTIPLIER, 1.0)
    return {
        "edge_threshold_base": edge_base_raw,
        "kelly_multiplier": kelly_raw,
        "edge_in_bounds": EDGE_BASE_MIN <= edge_base_raw <= EDGE_BASE_MAX,
        "kelly_in_bounds": KELLY_MULT_MIN <= kelly_raw <= KELLY_MULT_MAX,
        "safe": (EDGE_BASE_MIN <= edge_base_raw <= EDGE_BASE_MAX)
        and (KELLY_MULT_MIN <= kelly_raw <= KELLY_MULT_MAX),
        "bounds": {
            "edge_base_min": EDGE_BASE_MIN,
            "edge_base_max": EDGE_BASE_MAX,
            "kelly_min": KELLY_MULT_MIN,
            "kelly_max": KELLY_MULT_MAX,
        },
    }
