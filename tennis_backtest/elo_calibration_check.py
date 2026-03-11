"""
tennis_backtest/elo_calibration_check.py
Quick calibration diagnostics for stored model probabilities in `signals`.
"""

from __future__ import annotations

from database.db import get_conn


def run_calibration_check(bins: int = 10) -> int:
    try:
        conn = get_conn()
        cur = conn.execute(
            "SELECT model_prob, result FROM signals "
            "WHERE result IN ('win','loss') AND model_prob IS NOT NULL"
        )
        rows = cur.fetchall()
    except Exception as e:
        print(f"[elo-cal] Database unavailable: {e}")
        return 1

    if not rows:
        print("[elo-cal] No resolved model_prob rows found. Skipping calibration check.")
        return 0

    pairs = []
    for model_prob, result in rows:
        try:
            p = float(model_prob)
        except (TypeError, ValueError):
            continue
        if p <= 0 or p >= 1:
            continue
        y = 1.0 if str(result).lower() == "win" else 0.0
        pairs.append((p, y))

    if not pairs:
        print("[elo-cal] No valid probability rows after cleaning. Skipping.")
        return 0

    # Brier
    brier = sum((p - y) ** 2 for p, y in pairs) / len(pairs)
    print(f"[elo-cal] Samples: {len(pairs)}")
    print(f"[elo-cal] Brier: {brier:.6f}")

    # Calibration table + ECE
    bucket_size = 1.0 / bins
    ece = 0.0
    print("[elo-cal] Bin calibration (predicted vs empirical):")
    for i in range(bins):
        lo = i * bucket_size
        hi = (i + 1) * bucket_size
        if i == bins - 1:
            bucket = [(p, y) for p, y in pairs if lo <= p <= hi]
        else:
            bucket = [(p, y) for p, y in pairs if lo <= p < hi]
        if not bucket:
            continue
        avg_p = sum(p for p, _ in bucket) / len(bucket)
        avg_y = sum(y for _, y in bucket) / len(bucket)
        weight = len(bucket) / len(pairs)
        ece += weight * abs(avg_p - avg_y)
        print(f"  [{lo:0.1f}, {hi:0.1f}) n={len(bucket):>4}  pred={avg_p:0.3f}  emp={avg_y:0.3f}")
    print(f"[elo-cal] ECE: {ece:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_calibration_check())
