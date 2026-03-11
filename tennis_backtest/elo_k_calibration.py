"""
tennis_backtest/elo_k_calibration.py
Sweep Elo K-factor values and pick lowest Brier score on historical matches.
"""

from __future__ import annotations

import argparse
import math

import pandas as pd

from config import ELO_DEFAULT_RATING, SURFACE_ELO_WEIGHT
from backtest.engine import _download_year, _parse_matches


def _expected_win_prob(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + math.pow(10, (rating_b - rating_a) / 400.0))


def _get_rating(ratings: dict, player: str, surface: str) -> float:
    return ratings.get((player, surface), ELO_DEFAULT_RATING)


def _predict_winner_prob(ratings: dict, winner: str, loser: str, surface: str) -> float:
    overall_w = _get_rating(ratings, winner, "overall")
    overall_l = _get_rating(ratings, loser, "overall")
    surface_w = _get_rating(ratings, winner, surface)
    surface_l = _get_rating(ratings, loser, surface)
    blended_w = SURFACE_ELO_WEIGHT * surface_w + (1 - SURFACE_ELO_WEIGHT) * overall_w
    blended_l = SURFACE_ELO_WEIGHT * surface_l + (1 - SURFACE_ELO_WEIGHT) * overall_l
    return _expected_win_prob(blended_w, blended_l)


def _update_elo(ratings: dict, winner: str, loser: str, surface: str, k: float):
    for surf in ("overall", surface):
        rw = _get_rating(ratings, winner, surf)
        rl = _get_rating(ratings, loser, surf)
        ew = _expected_win_prob(rw, rl)
        ratings[(winner, surf)] = rw + k * (1 - ew)
        ratings[(loser, surf)] = rl + k * (0 - (1 - ew))


def calibrate_k(matches: list[dict], k_values: list[int]) -> list[dict]:
    """Return sorted calibration results by Brier score ascending."""
    if not matches:
        return []

    ordered = sorted(matches, key=lambda m: m["date"])
    results = []
    for k in k_values:
        ratings = {}
        sse = 0.0
        n = 0
        for m in ordered:
            p = _predict_winner_prob(ratings, m["winner"], m["loser"], m["surface"])
            # Event defined as winner side = 1
            sse += (p - 1.0) ** 2
            n += 1
            _update_elo(ratings, m["winner"], m["loser"], m["surface"], float(k))
        brier = sse / n if n else float("inf")
        results.append({"k": k, "brier": brier, "matches": n})
    return sorted(results, key=lambda x: x["brier"])


def _load_matches_from_years(atp_from: int, atp_to: int, wta_from: int, wta_to: int) -> list[dict]:
    all_matches = []
    for tour, y0, y1 in [("atp", atp_from, atp_to), ("wta", wta_from, wta_to)]:
        for year in range(y0, y1 + 1):
            df = _download_year(year, tour)
            if df.empty:
                continue
            all_matches.extend(_parse_matches(df))
    return all_matches


def _load_matches_from_csv(path: str) -> list[dict]:
    df = pd.read_csv(path)
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "winner" not in df.columns or "loser" not in df.columns:
        raise ValueError("CSV must include winner and loser columns")
    if "surface" not in df.columns:
        df["surface"] = "hard"
    if "date" not in df.columns:
        df["date"] = "1970-01-01"

    rows = []
    for _, r in df.iterrows():
        w = str(r.get("winner", "")).strip()
        l = str(r.get("loser", "")).strip()
        if not w or not l or w == "nan" or l == "nan":
            continue
        rows.append(
            {
                "date": str(r.get("date", "1970-01-01")),
                "winner": w,
                "loser": l,
                "surface": str(r.get("surface", "hard")).strip().lower() or "hard",
            }
        )
    return rows


def _parse_args():
    p = argparse.ArgumentParser(description="Elo K-factor calibration (min Brier).")
    p.add_argument("--input-csv", help="Optional input CSV with winner/loser/surface/date")
    p.add_argument("--atp-from", type=int, default=2020)
    p.add_argument("--atp-to", type=int, default=2024)
    p.add_argument("--wta-from", type=int, default=2020)
    p.add_argument("--wta-to", type=int, default=2024)
    p.add_argument("--k-min", type=int, default=8)
    p.add_argument("--k-max", type=int, default=40)
    p.add_argument("--k-step", type=int, default=1)
    p.add_argument("--top", type=int, default=5)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if args.k_step <= 0:
        print("[k-cal] k-step must be > 0")
        return 1
    if args.k_max < args.k_min:
        print("[k-cal] k-max must be >= k-min")
        return 1

    if args.input_csv:
        matches = _load_matches_from_csv(args.input_csv)
    else:
        matches = _load_matches_from_years(
            atp_from=args.atp_from,
            atp_to=args.atp_to,
            wta_from=args.wta_from,
            wta_to=args.wta_to,
        )

    if not matches:
        print("[k-cal] No matches loaded. Cannot calibrate K.")
        return 1

    k_values = list(range(args.k_min, args.k_max + 1, args.k_step))
    ranked = calibrate_k(matches, k_values)
    best = ranked[0]

    print(f"[k-cal] Matches: {best['matches']}")
    print(f"[k-cal] Best K: {best['k']} | Brier: {best['brier']:.6f}")
    print("[k-cal] Top candidates:")
    for row in ranked[: max(1, args.top)]:
        print(f"  K={row['k']:>2}  Brier={row['brier']:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
