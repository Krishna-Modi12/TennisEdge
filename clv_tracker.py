"""
clv_tracker.py
Compute Closing Line Value (CLV) on resolved signals.

CLV definition used:
    clv = opening_odds - closing_odds
Positive CLV means we beat the closing price.
"""

from __future__ import annotations

import csv
import os

from database.db import get_conn


def _bucket(clv: float, eps: float = 1e-6) -> str:
    if clv > eps:
        return "positive"
    if clv < -eps:
        return "negative"
    return "zero"


def _empty_stats(source: str, note: str, error: str | None = None) -> dict:
    stats = {
        "total_resolved": 0,
        "with_closing_odds": 0,
        "coverage_pct": 0.0,
        "positive": 0,
        "negative": 0,
        "zero": 0,
        "avg_clv": 0.0,
        "by_surface": [],
        "source": source,
        "note": note,
    }
    if error:
        stats["error"] = error
    return stats


def _calculate_from_rows(total_resolved: int, rows: list[tuple]) -> dict:
    by_surface = {}
    totals = {"positive": 0, "negative": 0, "zero": 0, "sum_clv": 0.0, "count": 0}

    for surface, opening, closing, _result in rows:
        opening = float(opening)
        closing = float(closing)
        clv = opening - closing
        b = _bucket(clv)

        totals[b] += 1
        totals["sum_clv"] += clv
        totals["count"] += 1

        if surface not in by_surface:
            by_surface[surface] = {
                "surface": surface,
                "positive": 0,
                "negative": 0,
                "zero": 0,
                "sum_clv": 0.0,
                "count": 0,
            }
        by_surface[surface][b] += 1
        by_surface[surface]["sum_clv"] += clv
        by_surface[surface]["count"] += 1

    with_closing = totals["count"]
    coverage_pct = round((with_closing / total_resolved) * 100, 1) if total_resolved else 0.0
    avg_clv = round(totals["sum_clv"] / with_closing, 4) if with_closing else 0.0

    surface_rows = []
    for s in sorted(by_surface.values(), key=lambda x: x["count"], reverse=True):
        s_avg = round(s["sum_clv"] / s["count"], 4) if s["count"] else 0.0
        surface_rows.append(
            {
                "surface": s["surface"],
                "count": s["count"],
                "positive": s["positive"],
                "negative": s["negative"],
                "zero": s["zero"],
                "avg_clv": s_avg,
            }
        )

    return {
        "total_resolved": total_resolved,
        "with_closing_odds": with_closing,
        "coverage_pct": coverage_pct,
        "positive": totals["positive"],
        "negative": totals["negative"],
        "zero": totals["zero"],
        "avg_clv": avg_clv,
        "by_surface": surface_rows,
    }


def _calculate_from_csv(csv_path: str) -> dict:
    rows = []
    total_resolved = 0
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            result = str(row.get("result", "")).strip().lower()
            if result not in {"win", "loss", "push"}:
                continue
            total_resolved += 1
            opening_raw = row.get("odds")
            closing_raw = row.get("closing_odds")
            try:
                opening = float(opening_raw)
                closing = float(closing_raw)
            except (TypeError, ValueError):
                continue
            surface = str(row.get("surface", "unknown")).strip().lower() or "unknown"
            rows.append((surface, opening, closing, result))
    stats = _calculate_from_rows(total_resolved=total_resolved, rows=rows)
    stats["source"] = "signals.csv"
    if not rows:
        stats["note"] = "No CLV rows available in signals.csv."
    return stats


def calculate_clv(csv_path: str = "signals.csv") -> dict:
    try:
        conn = get_conn()
        cur_total = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE result IN ('win', 'loss', 'push')"
        )
        total_resolved = cur_total.fetchone()[0] or 0

        cur = conn.execute(
            "SELECT surface, odds, closing_odds, result "
            "FROM signals "
            "WHERE result IN ('win', 'loss', 'push') "
            "AND odds IS NOT NULL "
            "AND closing_odds IS NOT NULL"
        )
        rows = cur.fetchall()
        stats = _calculate_from_rows(total_resolved=total_resolved, rows=rows)
        stats["source"] = "database"
        return stats
    except Exception as db_error:
        if os.path.exists(csv_path):
            try:
                return _calculate_from_csv(csv_path)
            except Exception as csv_error:
                return _empty_stats(
                    source="none",
                    note="Database unavailable and signals.csv parsing failed.",
                    error=f"db_error={db_error}; csv_error={csv_error}",
                )
        return _empty_stats(
            source="none",
            note="Database unavailable and signals.csv not found.",
            error=str(db_error),
        )


def format_clv_report(stats: dict) -> str:
    lines = [
        "CLV REPORT",
        "==========",
        f"Source: {stats.get('source', 'unknown')}",
        f"Resolved Signals: {stats['total_resolved']}",
        f"With Closing Odds: {stats['with_closing_odds']} ({stats['coverage_pct']}%)",
        f"Positive CLV: {stats['positive']}",
        f"Negative CLV: {stats['negative']}",
        f"Zero CLV: {stats['zero']}",
        f"Average CLV: {stats['avg_clv']:+.4f}",
        "",
    ]
    if stats.get("note"):
        lines.append(f"Note: {stats['note']}")
        lines.append("")
    lines.extend([
        "By Surface:",
    ])
    if not stats["by_surface"]:
        lines.append("- No CLV rows available yet.")
    else:
        for row in stats["by_surface"]:
            lines.append(
                f"- {row['surface']}: n={row['count']}, +={row['positive']}, "
                f"-={row['negative']}, 0={row['zero']}, avg={row['avg_clv']:+.4f}"
            )
    return "\n".join(lines)


def main():
    stats = calculate_clv()
    print(format_clv_report(stats))


if __name__ == "__main__":
    main()
