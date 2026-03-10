"""
ingestion/build_elo_from_history.py

Downloads historical ATP + WTA match data from tennis-data.co.uk
and builds real surface-adjusted Elo ratings for all players.

Run once before starting the bot:
    python -m ingestion.build_elo_from_history

Data source: http://www.tennis-data.co.uk/alldata.php
Column reference (tennis-data.co.uk standard format):
    ATP: ATP, Location, Tournament, Date, Series, Court, Surface,
         Round, Best of, Winner, Loser, WRank, LRank, W1..W5, L1..L5,
         Wsets, Lsets, Comment, B365W, B365L, PSW, PSL, MaxW, MaxL, AvgW, AvgL
    WTA: WTA, Location, Tournament, Date, Tier, Court, Surface,
         Round, Best of, Winner, Loser, WRank, LRank, ...same odds cols
"""

import io
import requests
import pandas as pd
from database.db import upsert_elo, get_elo
from config import ELO_K_FACTOR, ELO_DEFAULT_RATING

BASE_URL = "http://www.tennis-data.co.uk"

# ── Surface normalisation ─────────────────────────────────────────────────────

SURFACE_MAP = {
    "hard":    "hard",
    "clay":    "clay",
    "grass":   "grass",
    "carpet":  "carpet",
    "indoor":  "hard",
    "outdoor": "hard",
}

def norm_surface(raw: str) -> str:
    if not raw or pd.isna(raw):
        return "hard"
    return SURFACE_MAP.get(str(raw).strip().lower(), "hard")


# ── Elo maths ─────────────────────────────────────────────────────────────────

def expected(ra: float, rb: float) -> float:
    import math
    return 1.0 / (1.0 + math.pow(10, (rb - ra) / 400.0))

def new_ratings(ra: float, rb: float, k: int = ELO_K_FACTOR):
    """Winner is A, Loser is B. Returns (new_ra, new_rb)."""
    ea = expected(ra, rb)
    return ra + k * (1 - ea), rb + k * (0 - (1 - ea))


# ── In-memory Elo store (before flushing to DB) ───────────────────────────────

_elo: dict = {}   # key: (player, surface) → float

def _get(player: str, surface: str) -> float:
    return _elo.get((player, surface), ELO_DEFAULT_RATING)

def _set(player: str, surface: str, rating: float):
    _elo[(player, surface)] = rating

def _process_match(winner: str, loser: str, surface: str):
    for surf in ["overall", surface]:
        ra = _get(winner, surf)
        rb = _get(loser,  surf)
        new_ra, new_rb = new_ratings(ra, rb)
        _set(winner, surf, new_ra)
        _set(loser,  surf, new_rb)


# ── Download helpers ──────────────────────────────────────────────────────────

def _download_xlsx(url: str) -> pd.DataFrame | None:
    try:
        print(f"  Downloading {url} ...")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        # Try xlsx first, fall back to legacy xls
        try:
            return pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
        except Exception:
            # BUG FIX: Original code referenced `resp` inside a nested except that
            # was outside the scope where resp was defined. If requests.get() itself
            # threw (e.g. connection error), resp would be undefined and the except
            # block would raise a NameError on top of the original error.
            # Now both read attempts are inside the same try block where resp exists.
            return pd.read_excel(io.BytesIO(resp.content), engine="xlrd")
    except Exception as e:
        print(f"  ⚠ Failed to download {url}: {e}")
        return None


def _download_year(year: int, tour: str) -> pd.DataFrame | None:
    """
    tour: 'atp' or 'wta'
    URL pattern:
        ATP 2013+:  /2024/2024.xlsx
        ATP pre-2012: /2011/2011.xls
        WTA 2013+:  /2024w/2024.xlsx
        WTA pre-2012: /2011w/2011.xls
    """
    ext = "xlsx" if year >= 2013 else "xls"
    suffix = "w" if tour == "wta" else ""
    url = f"{BASE_URL}/{year}{suffix}/{year}.{ext}"
    return _download_xlsx(url)


# ── Parse a year's dataframe ──────────────────────────────────────────────────

def _parse_df(df: pd.DataFrame, tour: str) -> int:
    """Process all matches in a dataframe. Returns count of matches processed."""
    # Normalise column names to lowercase
    df.columns = [str(c).strip().lower() for c in df.columns]

    count = 0
    for _, row in df.iterrows():
        winner = str(row.get("winner", "")).strip()
        loser  = str(row.get("loser",  "")).strip()
        if not winner or not loser or winner == "nan" or loser == "nan":
            continue

        surface = norm_surface(row.get("surface"))
        _process_match(winner, loser, surface)
        count += 1

    return count


# ── Main entry point ──────────────────────────────────────────────────────────

def build_elo(
    atp_years: list[int] = None,
    wta_years: list[int] = None,
    flush_to_db: bool = True
):
    """
    Download historical data and build Elo ratings.

    Args:
        atp_years: list of years to process for ATP (default: 2010–2025)
        wta_years: list of years to process for WTA (default: 2010–2025)
        flush_to_db: write computed Elo ratings to PostgreSQL
    """
    if atp_years is None:
        atp_years = list(range(2010, 2026))
    if wta_years is None:
        wta_years = list(range(2010, 2026))

    # BUG FIX: _elo is a module-level global. Without clearing it at the start
    # of each build_elo() call, running the function twice (e.g. in testing or
    # after a partial run) accumulates stale ratings from the previous run,
    # corrupting the final Elo values written to the DB.
    global _elo
    _elo = {}

    total_matches = 0

    # ── ATP ──
    print("\n[Elo Builder] Processing ATP data...")
    for year in atp_years:
        df = _download_year(year, "atp")
        if df is None:
            continue
        n = _parse_df(df, "atp")
        total_matches += n
        print(f"  ATP {year}: {n} matches")

    # ── WTA ──
    print("\n[Elo Builder] Processing WTA data...")
    for year in wta_years:
        df = _download_year(year, "wta")
        if df is None:
            continue
        n = _parse_df(df, "wta")
        total_matches += n
        print(f"  WTA {year}: {n} matches")

    print(f"\n[Elo Builder] Total matches processed: {total_matches}")
    print(f"[Elo Builder] Total player-surface Elo entries: {len(_elo)}")

    # ── Flush to DB ──
    if flush_to_db:
        print("\n[Elo Builder] Writing to database...")
        for (player, surface), rating in _elo.items():
            upsert_elo(player, surface, round(rating, 2))
        print(f"[Elo Builder] ✅ Written {len(_elo)} Elo ratings to DB.")
    else:
        print("\n[Elo Builder] flush_to_db=False — ratings not saved.")

    return _elo


def top_players_by_surface(surface: str = "overall", n: int = 20) -> list[tuple]:
    """Print top N players for a given surface after building."""
    entries = [
        (player, rating)
        for (player, surf), rating in _elo.items()
        if surf == surface
    ]
    entries.sort(key=lambda x: x[1], reverse=True)
    return entries[:n]


# ── CLI usage ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build Elo ratings from tennis-data.co.uk")
    parser.add_argument("--atp-from", type=int, default=2010, help="ATP start year")
    parser.add_argument("--atp-to",   type=int, default=2025, help="ATP end year")
    parser.add_argument("--wta-from", type=int, default=2010, help="WTA start year")
    parser.add_argument("--wta-to",   type=int, default=2025, help="WTA end year")
    parser.add_argument("--no-db",    action="store_true",   help="Don't write to DB (dry run)")
    args = parser.parse_args()

    atp_years = list(range(args.atp_from, args.atp_to + 1))
    wta_years = list(range(args.wta_from, args.wta_to + 1))

    build_elo(
        atp_years=atp_years,
        wta_years=wta_years,
        flush_to_db=not args.no_db
    )

    print("\n── Top 10 Overall ──────────────────────────────")
    for player, rating in top_players_by_surface("overall", 10):
        print(f"  {player:<25} {rating:.1f}")

    print("\n── Top 10 Clay ─────────────────────────────────")
    for player, rating in top_players_by_surface("clay", 10):
        print(f"  {player:<25} {rating:.1f}")

    print("\n── Top 10 Grass ────────────────────────────────")
    for player, rating in top_players_by_surface("grass", 10):
        print(f"  {player:<25} {rating:.1f}")
