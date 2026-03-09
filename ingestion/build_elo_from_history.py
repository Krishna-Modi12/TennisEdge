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
import logging
import requests
import pandas as pd
from database.db import upsert_elo, get_elo
from config import ELO_K_FACTOR, ELO_DEFAULT_RATING
from utils import normalize_player_name

logger = logging.getLogger(__name__)

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
    logger.info(f"  Downloading {url} ...")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"  ⚠ Download failed: {e}")
        return None

    # Try .xlsx first, then fall back to .xls
    try:
        return pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
    except Exception:
        try:
            return pd.read_excel(io.BytesIO(resp.content), engine="xlrd")
        except Exception as e2:
            logger.warning(f"  ⚠ Parse failed: {e2}")
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

def _parse_df(df: pd.DataFrame, tour: str, flush_to_db: bool = True) -> int:
    """Process all matches in a dataframe. Returns count of matches processed."""
    # Normalise column names to lowercase
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date", na_position="last").reset_index(drop=True)

    match_records = []
    h2h_records = []
    
    count = 0
    for _, row in df.iterrows():
        raw_winner = row.get("winner")
        raw_loser  = row.get("loser")
        if pd.isna(raw_winner) or pd.isna(raw_loser):
            continue
        winner = normalize_player_name(str(raw_winner).strip())
        loser  = normalize_player_name(str(raw_loser).strip())
        if not winner or not loser:
            continue

        surface = norm_surface(row.get("surface"))
        _process_match(winner, loser, surface)
        
        # Get extra fields for database
        date_val = row.get("date")
        if pd.notna(date_val):
            try:
                date_str = pd.Timestamp(date_val).strftime("%Y-%m-%d")
            except Exception:
                date_str = "1970-01-01"
        else:
            date_str = "1970-01-01"
            
        try:
            odds_w = float(row.get("avgw", 0))
            if pd.isna(odds_w): odds_w = 0.0
            odds_l = float(row.get("avgl", 0))
            if pd.isna(odds_l): odds_l = 0.0
        except Exception:
            odds_w, odds_l = 0.0, 0.0
            
        tournament = str(row.get("tournament", "Unknown")).strip()
        player1, player2 = (winner, loser) if winner <= loser else (loser, winner)
        odds_p1 = odds_w if winner == player1 else odds_l
        odds_p2 = odds_l if winner == player1 else odds_w
        
        match_records.append((date_str, player1, player2, surface, winner, odds_p1, odds_p2, tournament))
        h2h_records.append((player1, player2, surface, winner, date_str))
        
        count += 1
        
    if flush_to_db and match_records:
        from database.db import get_conn
        with get_conn() as conn:
            conn.executemany(
                "INSERT INTO matches (date, player1, player2, surface, winner, odds_p1, odds_p2, tournament) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                match_records
            )
            conn.executemany(
                "INSERT INTO h2h (player1, player2, surface, winner, date) VALUES (?, ?, ?, ?, ?)",
                h2h_records
            )
            conn.commit()

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
        flush_to_db: write computed Elo ratings to SQLite
    """
    if atp_years is None:
        atp_years = list(range(2010, 2026))
    if wta_years is None:
        wta_years = list(range(2010, 2026))

    total_matches = 0

    # ── ATP ──
    logger.info("[Elo Builder] Processing ATP data...")
    for year in atp_years:
        df = _download_year(year, "atp")
        if df is None:
            continue
        n = _parse_df(df, "atp", flush_to_db)
        total_matches += n
        logger.info(f"  ATP {year}: {n} matches")

    # ── WTA ──
    logger.info("[Elo Builder] Processing WTA data...")
    for year in wta_years:
        df = _download_year(year, "wta")
        if df is None:
            continue
        n = _parse_df(df, "wta", flush_to_db)
        total_matches += n
        logger.info(f"  WTA {year}: {n} matches")

    logger.info(f"[Elo Builder] Total matches processed: {total_matches}")
    logger.info(f"[Elo Builder] Total player-surface Elo entries: {len(_elo)}")

    # ── Flush to DB ──
    if flush_to_db:
        logger.info("[Elo Builder] Writing to database...")
        for (player, surface), rating in _elo.items():
            upsert_elo(player, surface, round(rating, 2))
        logger.info(f"[Elo Builder] ✅ Written {len(_elo)} Elo ratings to DB.")
    else:
        logger.info("[Elo Builder] flush_to_db=False — ratings not saved.")

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
