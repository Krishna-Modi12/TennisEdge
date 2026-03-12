"""
ingestion/build_player_stats.py
One-time script to build player form, surface win rates, and H2H records
from tennis-data.co.uk historical match data.

Stores results in player_stats and h2h_records tables.

Usage:
    python -m ingestion.build_player_stats --atp-from 2015 --atp-to 2025
"""

import argparse
import datetime
import io
import sys
from collections import defaultdict

import pandas as pd
import requests

from database.db import (
    init_schema,
    upsert_player_stats,
    upsert_h2h,
    upsert_player_surface_strength,
    resolve_player_name,
)


def _download_year(year: int, tour: str) -> pd.DataFrame:
    """Download a single year's match data from tennis-data.co.uk."""
    suffix = "w" if tour == "wta" else ""
    # Try xlsx first, then xls
    for ext in ["xlsx", "xls"]:
        url = f"http://www.tennis-data.co.uk/{year}{suffix}/{year}.{ext}"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                df = pd.read_excel(io.BytesIO(resp.content))
                # Normalize column names to lowercase
                df.columns = [c.strip().lower() for c in df.columns]
                return df
        except Exception as e:
            print(f"  {ext} failed for {tour.upper()} {year}: {e}")
            continue
    return pd.DataFrame()


def _normalize_surface(s) -> str:
    if not isinstance(s, str):
        return "hard"
    s = s.strip().lower()
    mapping = {"hard": "hard", "clay": "clay", "grass": "grass",
               "carpet": "hard", "indoor": "hard", "outdoor": "hard"}
    return mapping.get(s, "hard")


def build_stats(atp_from=2015, atp_to=2025, wta_from=2015, wta_to=2025,
                flush_to_db=True):
    """
    Process historical match data to compute:
    1. Recent form score (weighted win rate, last 20 matches per player)
    2. Surface win rate (last 2 years per surface)
    3. H2H records (all time, per surface + overall)
    """
    # In-memory accumulators
    # player_matches[player] = list of (date, surface, won: bool) sorted by date
    player_matches = defaultdict(list)
    # h2h[sorted(a,b)][surface] = {"a_wins": int, "b_wins": int}
    h2h = defaultdict(lambda: defaultdict(lambda: {"a_wins": 0, "b_wins": 0}))

    total_matches = 0

    # Process ATP
    for year in range(atp_from, atp_to + 1):
        print(f"Downloading ATP {year}...", end=" ", flush=True)
        df = _download_year(year, "atp")
        if df.empty:
            print("no data")
            continue
        n = _process_dataframe(df, player_matches, h2h)
        total_matches += n
        print(f"{n} matches")

    # Process WTA
    for year in range(wta_from, wta_to + 1):
        print(f"Downloading WTA {year}...", end=" ", flush=True)
        df = _download_year(year, "wta")
        if df.empty:
            print("no data")
            continue
        n = _process_dataframe(df, player_matches, h2h)
        total_matches += n
        print(f"{n} matches")

    print(f"\nTotal matches processed: {total_matches}")
    print(f"Unique players: {len(player_matches)}")

    # Compute stats
    cutoff = datetime.date.today() - datetime.timedelta(days=730)  # 2 years

    player_count = 0
    h2h_count = 0

    if flush_to_db:
        print("\nComputing and flushing player stats to DB...")
        for player, matches in player_matches.items():
            matches.sort(key=lambda x: x[0])  # sort by date

            # Recent form: last 20 matches, weighted
            recent = matches[-20:]
            form_score = _calc_form_score(recent)

            # Overall win rate (all matches)
            total = len(matches)
            wins = sum(1 for _, _, won in matches if won)
            overall_wr = wins / total if total > 0 else 0.5

            upsert_player_stats(player, "overall", form_score, overall_wr, total)

            # Surface-specific win rates (last 2 years only)
            for surface in ["clay", "hard", "grass"]:
                surface_matches = [
                    (d, s, w) for d, s, w in matches
                    if s == surface and d >= cutoff
                ]
                if surface_matches:
                    s_total = len(surface_matches)
                    s_wins = sum(1 for _, _, w in surface_matches if w)
                    s_wr = s_wins / s_total if s_total > 0 else 0.5
                    upsert_player_stats(player, surface, form_score, s_wr, s_total)
                    serve_strength, return_strength = _proxy_surface_strength(s_wr)
                    upsert_player_surface_strength(
                        player=player,
                        surface=surface,
                        serve_strength=serve_strength,
                        return_strength=return_strength,
                        matches_counted=s_total,
                    )

            player_count += 1
            if player_count % 200 == 0:
                print(f"  ...{player_count} players processed")

        print(f"Player stats: {player_count} players written")

        print("Flushing H2H records to DB...")
        for pair_key, surfaces in h2h.items():
            sorted_a, sorted_b = pair_key
            for surface, record in surfaces.items():
                upsert_h2h(sorted_a, sorted_b, surface,
                          record["a_wins"], record["b_wins"])
                h2h_count += 1
                if h2h_count % 1000 == 0:
                    print(f"  ...{h2h_count} H2H records written")

        print(f"H2H records: {h2h_count} written")

    print("\n✅ Player stats build complete!")
    return player_count, h2h_count


def _process_dataframe(df: pd.DataFrame, player_matches: dict, h2h: dict) -> int:
    """Process one year's matches. Returns count processed."""
    required = ["winner", "loser"]
    for col in required:
        if col not in df.columns:
            return 0

    # Parse dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["date"])
    else:
        df["date"] = pd.Timestamp.today()

    count = 0
    for _, row in df.iterrows():
        try:
            winner = str(row["winner"]).strip()
            loser = str(row["loser"]).strip()
            if not winner or not loser or winner == "nan" or loser == "nan":
                continue

            surface = _normalize_surface(row.get("surface", "hard"))
            match_date = row["date"].date() if hasattr(row["date"], "date") else datetime.date.today()

            # Record matches for both players
            player_matches[winner].append((match_date, surface, True))
            player_matches[loser].append((match_date, surface, False))

            # Record H2H (sorted alphabetically)
            sorted_a, sorted_b = sorted([winner, loser])
            pair_key = (sorted_a, sorted_b)

            for surf in [surface, "overall"]:
                if winner == sorted_a:
                    h2h[pair_key][surf]["a_wins"] += 1
                else:
                    h2h[pair_key][surf]["b_wins"] += 1

            count += 1
        except Exception:
            continue

    return count


def _calc_form_score(recent_matches: list) -> float:
    """
    Calculate weighted form score from recent matches.
    Most recent match gets weight 1.0, decreasing by 0.05 per match.
    """
    if not recent_matches:
        return 0.5

    n = len(recent_matches)
    weighted_sum = 0.0
    weight_sum = 0.0

    for i, (_, _, won) in enumerate(recent_matches):
        # i=0 is oldest, i=n-1 is most recent
        weight = max(0.05, 1.0 - (n - 1 - i) * 0.05)
        weighted_sum += (1.0 if won else 0.0) * weight
        weight_sum += weight

    return round(weighted_sum / weight_sum, 4) if weight_sum > 0 else 0.5


def _proxy_surface_strength(surface_win_rate: float) -> tuple[float, float]:
    """Conservative mapping from surface win rate to serve/return strengths."""
    wr = max(0.0, min(1.0, float(surface_win_rate)))
    serve_strength = max(0.45, min(0.80, 0.62 + ((wr - 0.5) * 0.30)))
    return_strength = max(0.25, min(0.55, 0.38 + ((wr - 0.5) * 0.24)))
    return serve_strength, return_strength


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build player stats from historical data")
    parser.add_argument("--atp-from", type=int, default=2015)
    parser.add_argument("--atp-to", type=int, default=2025)
    parser.add_argument("--wta-from", type=int, default=2015)
    parser.add_argument("--wta-to", type=int, default=2025)
    parser.add_argument("--no-db", action="store_true", help="Dry run, don't write to DB")
    args = parser.parse_args()

    print("Initializing database schema...")
    init_schema()

    build_stats(
        atp_from=args.atp_from, atp_to=args.atp_to,
        wta_from=args.wta_from, wta_to=args.wta_to,
        flush_to_db=not args.no_db,
    )
