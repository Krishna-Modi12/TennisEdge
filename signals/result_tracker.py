"""
signals/result_tracker.py
Track resolved signals in signal_performance with ROI + CLV ratio metrics.
"""

from __future__ import annotations

from database.db import (
    get_resolved_signals_for_performance,
    upsert_signal_performance,
)


def calculate_profit(result: str, taken_odds: float, stake: float = 1.0) -> float:
    """Flat-stake P/L in units for result in {win, loss, push}."""
    if result == "win":
        return (taken_odds - 1.0) * stake
    if result == "loss":
        return -stake
    return 0.0


def calculate_roi(profit: float, stake: float = 1.0) -> float:
    """Return ROI as decimal ratio (profit / stake)."""
    if stake <= 0:
        return 0.0
    return profit / stake


def calculate_clv_ratio(taken_odds: float, closing_odds: float | None) -> float | None:
    """Return CLV ratio = taken_odds / closing_odds when closing odds are available."""
    if closing_odds is None or closing_odds <= 0:
        return None
    return taken_odds / closing_odds


def _build_performance_row(signal: dict, stake: float = 1.0) -> dict | None:
    result = str(signal.get("result", "")).strip().lower()
    if result not in {"win", "loss", "push"}:
        return None

    taken_odds = signal.get("taken_odds")
    if taken_odds is None:
        return None

    closing_odds = signal.get("closing_odds")
    profit = calculate_profit(result=result, taken_odds=taken_odds, stake=stake)
    roi = calculate_roi(profit=profit, stake=stake)
    clv_ratio = calculate_clv_ratio(taken_odds=taken_odds, closing_odds=closing_odds)
    line_movement = None
    if closing_odds is not None:
        line_movement = float(closing_odds) - float(taken_odds)
    is_win = signal.get("is_win")

    return {
        "match_id": signal["match_id"],
        "surface": signal.get("surface"),
        "taken_odds": float(taken_odds),
        "closing_odds": float(closing_odds) if closing_odds is not None else None,
        "model_prob": float(signal["model_prob"]) if signal.get("model_prob") is not None else None,
        "is_win": is_win,
        "clv_ratio": round(float(clv_ratio), 6) if clv_ratio is not None else None,
        "line_movement": round(float(line_movement), 6) if line_movement is not None else None,
        "result": result,
        "profit": round(float(profit), 6),
        "roi": round(float(roi), 6),
    }


def sync_signal_performance(limit: int = 500, stake: float = 1.0) -> dict:
    """
    Backfill/update signal_performance from resolved signals.
    Safe to run repeatedly (UPSERT by match_id).
    """
    rows = get_resolved_signals_for_performance(limit=limit)
    processed = 0
    skipped = 0
    errors = 0
    total_profit = 0.0
    total_staked = 0.0

    for row in rows:
        payload = _build_performance_row(row, stake=stake)
        if payload is None:
            skipped += 1
            continue
        try:
            upsert_signal_performance(
                match_id=payload["match_id"],
                surface=payload["surface"],
                taken_odds=payload["taken_odds"],
                closing_odds=payload["closing_odds"],
                model_prob=payload["model_prob"],
                is_win=payload["is_win"],
                clv_ratio=payload["clv_ratio"],
                line_movement=payload["line_movement"],
            )
            processed += 1
            total_profit += payload["profit"]
            total_staked += stake
        except Exception:
            errors += 1

    roi = calculate_roi(total_profit, total_staked) if total_staked > 0 else 0.0
    return {
        "ok": errors == 0,
        "processed": processed,
        "skipped": skipped,
        "errors": errors,
        "total_candidates": len(rows),
        "roi": round(float(roi), 6),
        "total_profit": round(float(total_profit), 6),
        "total_staked": round(float(total_staked), 6),
    }


if __name__ == "__main__":
    print(sync_signal_performance())
