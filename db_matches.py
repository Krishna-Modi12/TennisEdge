"""
db_matches.py — Parse tennis-data.co.uk Excel files and populate matches + h2h tables.

Usage:
    python db_matches.py                    # parse data/*.xlsx for 2015-2024
    python db_matches.py --from-year 2018   # start from 2018
    python db_matches.py --download         # download missing Excel files first

Tables created:
    matches  — full match record with odds
    h2h      — head-to-head lookup (surface-aware)

Safe to re-run: uses INSERT OR IGNORE on UNIQUE constraints.
"""

import argparse
import logging
import os
import sqlite3
import sys
import time

import pandas as pd
import requests

# ── Setup ─────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "tennis.db")

DOWNLOAD_BASE = "http://www.tennis-data.co.uk"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("db_matches")

# ── Column mapping ────────────────────────────────────────────────────────────
# tennis-data.co.uk columns vary by year.  We normalise to lowercase keys
# then map to our canonical names.

COLUMN_MAP = {
    # Date
    "date": "date",
    # Tournament
    "tournament": "tournament",
    "event":      "tournament",
    # Surface
    "surface": "surface",
    "court":   "surface",
    # Round
    "round":   "round",
    # Players
    "winner":  "winner",
    "loser":   "loser",
    # Score
    "score":   "score",
    # Bet365 odds (most consistent across years)
    "b365w":   "odds_winner",
    "b365l":   "odds_loser",
    # Pinnacle fallback
    "psw":     "odds_winner_ps",
    "psl":     "odds_loser_ps",
    # Average odds fallback
    "avgw":    "odds_winner_avg",
    "avgl":    "odds_loser_avg",
}

SURFACE_MAP = {
    "hard":    "hard",
    "clay":    "clay",
    "grass":   "grass",
    "carpet":  "hard",    # carpet courts are effectively hard
    "indoor":  "hard",
    "outdoor": "hard",
}

REQUIRED_COLS = {"date", "winner", "loser", "surface"}

# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS matches (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date       TEXT NOT NULL,
    tournament TEXT,
    surface    TEXT,
    round      TEXT,
    player1    TEXT NOT NULL,
    player2    TEXT NOT NULL,
    winner     TEXT NOT NULL,
    score      TEXT,
    odds_p1    REAL,
    odds_p2    REAL,
    year       INTEGER,
    UNIQUE(date, player1, player2)
);

CREATE TABLE IF NOT EXISTS h2h (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    player1  TEXT NOT NULL,
    player2  TEXT NOT NULL,
    surface  TEXT,
    winner   TEXT NOT NULL,
    date     TEXT NOT NULL,
    UNIQUE(date, player1, player2, surface)
);

CREATE INDEX IF NOT EXISTS idx_matches_players ON matches(player1, player2);
CREATE INDEX IF NOT EXISTS idx_matches_surface ON matches(surface);
CREATE INDEX IF NOT EXISTS idx_matches_year    ON matches(year);
CREATE INDEX IF NOT EXISTS idx_h2h_players     ON h2h(player1, player2);
CREATE INDEX IF NOT EXISTS idx_h2h_surface     ON h2h(surface);
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Consistent player-name normalisation (mirrors utils.normalize_player_name)."""
    if not name or not isinstance(name, str):
        return ""
    name = " ".join(name.split())          # collapse whitespace
    if "," in name:                        # "Nadal R." → "R. Nadal"
        parts = [p.strip() for p in name.split(",", 1)]
        name = f"{parts[1]} {parts[0]}" if len(parts) == 2 else name
    name = name.strip().title()
    # Fix common title-case artefacts
    for prefix in ("Mc", "Mac"):
        idx = name.find(prefix)
        while idx != -1:
            nxt = idx + len(prefix)
            if nxt < len(name) and name[nxt].islower():
                name = name[:nxt] + name[nxt].upper() + name[nxt + 1:]
            idx = name.find(prefix, nxt)
    name = name.replace("O'", "O'")  # ensure curly → straight quote
    for token in ("O'", ):
        idx = name.find(token)
        while idx != -1:
            nxt = idx + len(token)
            if nxt < len(name) and name[nxt].islower():
                name = name[:nxt] + name[nxt].upper() + name[nxt + 1:]
            idx = name.find(token, nxt)
    for lc in ("De ", "Van ", "Del ", "Von "):
        name = name.replace(lc, lc.lower())
    return name.strip()


def ordered_pair(a: str, b: str):
    """Return (player1, player2) in deterministic alphabetical order."""
    return (a, b) if a <= b else (b, a)


def get_conn() -> sqlite3.Connection:
    """Open (or reuse) a WAL-mode connection to the matches DB."""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_tables(conn: sqlite3.Connection):
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    logger.info("Tables 'matches' and 'h2h' ready  (%s)", DB_PATH)


# ── Download ──────────────────────────────────────────────────────────────────

def download_file(year: int) -> str | None:
    """Download an Excel file for `year` if it doesn't already exist.
    Returns the local path, or None on failure.
    """
    # 2013+ are xlsx, older are xls
    ext = "xlsx" if year >= 2013 else "xls"
    local = os.path.join(DATA_DIR, f"{year}.{ext}")
    if os.path.exists(local):
        return local

    url = f"{DOWNLOAD_BASE}/{year}/{year}.{ext}"
    logger.info("  ↓ Downloading %s …", url)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with open(local, "wb") as f:
            f.write(resp.content)
        time.sleep(1)  # polite pause
        return local
    except Exception as e:
        logger.warning("  ✗ Download failed for %d: %s", year, e)
        return None


# ── Parsing ───────────────────────────────────────────────────────────────────

def read_excel_safe(path: str) -> pd.DataFrame | None:
    """Read an Excel file, trying openpyxl then xlrd."""
    for engine in ("openpyxl", "xlrd", None):
        try:
            df = pd.read_excel(path, engine=engine)
            return df
        except Exception:
            continue
    logger.error("  ✗ Could not read %s with any engine", path)
    return None


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase all column names, then rename via COLUMN_MAP."""
    df.columns = [c.strip().lower() for c in df.columns]
    rename = {}
    for orig, canon in COLUMN_MAP.items():
        if orig in df.columns and canon not in rename.values():
            rename[orig] = canon
    df = df.rename(columns=rename)
    return df


def parse_year_file(path: str, year: int) -> list[dict]:
    """Parse one Excel file → list of row dicts ready for insertion."""
    df = read_excel_safe(path)
    if df is None or df.empty:
        return []

    df = normalise_columns(df)

    # Check required columns
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        logger.warning("  ⚠ %s missing columns %s — skipping", path, missing)
        return []

    rows = []
    for _, r in df.iterrows():
        # Skip rows with NaN in critical fields
        if pd.isna(r.get("winner")) or pd.isna(r.get("loser")):
            continue

        winner = normalize_name(str(r["winner"]))
        loser = normalize_name(str(r["loser"]))
        if not winner or not loser:
            continue

        # Surface
        raw_surface = str(r.get("surface", "hard")).strip().lower()
        surface = SURFACE_MAP.get(raw_surface, "hard")

        # Date
        date_val = r.get("date")
        if pd.isna(date_val):
            continue
        try:
            date_str = pd.Timestamp(date_val).strftime("%Y-%m-%d")
        except Exception:
            continue

        # Tournament / round / score
        tournament = str(r.get("tournament", "")).strip() if not pd.isna(r.get("tournament")) else ""
        rnd = str(r.get("round", "")).strip() if not pd.isna(r.get("round")) else ""
        score = str(r.get("score", "")).strip() if not pd.isna(r.get("score")) else ""

        # Odds — prefer B365, fallback to Pinnacle, then Average
        odds_w = _safe_float(r.get("odds_winner"))
        odds_l = _safe_float(r.get("odds_loser"))
        if odds_w is None:
            odds_w = _safe_float(r.get("odds_winner_ps"))
        if odds_l is None:
            odds_l = _safe_float(r.get("odds_loser_ps"))
        if odds_w is None:
            odds_w = _safe_float(r.get("odds_winner_avg"))
        if odds_l is None:
            odds_l = _safe_float(r.get("odds_loser_avg"))

        # Alphabetical ordering for player1/player2
        p1, p2 = ordered_pair(winner, loser)
        odds_p1 = odds_w if p1 == winner else odds_l
        odds_p2 = odds_l if p1 == winner else odds_w

        rows.append({
            "date":       date_str,
            "tournament": tournament,
            "surface":    surface,
            "round":      rnd,
            "player1":    p1,
            "player2":    p2,
            "winner":     winner,
            "score":      score,
            "odds_p1":    odds_p1,
            "odds_p2":    odds_p2,
            "year":       year,
        })

    return rows


def _safe_float(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        f = float(val)
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


# ── Insertion ─────────────────────────────────────────────────────────────────

INSERT_MATCH = """
INSERT OR IGNORE INTO matches
    (date, tournament, surface, round, player1, player2, winner, score, odds_p1, odds_p2, year)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_H2H = """
INSERT OR IGNORE INTO h2h (player1, player2, surface, winner, date)
VALUES (?, ?, ?, ?, ?)
"""


def insert_rows(conn: sqlite3.Connection, rows: list[dict]) -> tuple[int, int]:
    """Insert rows into matches and h2h.  Returns (matches_inserted, h2h_inserted)."""
    cur = conn.cursor()
    m_count = 0
    h_count = 0

    for r in rows:
        # matches
        cur.execute(INSERT_MATCH, (
            r["date"], r["tournament"], r["surface"], r["round"],
            r["player1"], r["player2"], r["winner"], r["score"],
            r["odds_p1"], r["odds_p2"], r["year"],
        ))
        m_count += cur.rowcount  # 1 if inserted, 0 if ignored

        # h2h  (same ordered pair)
        cur.execute(INSERT_H2H, (
            r["player1"], r["player2"], r["surface"], r["winner"], r["date"],
        ))
        h_count += cur.rowcount

    conn.commit()
    return m_count, h_count


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Populate matches & h2h tables from tennis-data.co.uk Excel files")
    parser.add_argument("--from-year", type=int, default=2015, help="Start year (default: 2015)")
    parser.add_argument("--to-year",   type=int, default=2024, help="End year (default: 2024)")
    parser.add_argument("--download",  action="store_true", help="Download missing Excel files before parsing")
    parser.add_argument("--db",        type=str, default=None, help=f"SQLite DB path (default: {DB_PATH})")
    args = parser.parse_args()

    db_path = args.db or DB_PATH

    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_tables(conn)

    total_m = 0
    total_h = 0
    years = range(args.from_year, args.to_year + 1)

    logger.info("Processing years %d–%d  (download=%s)", args.from_year, args.to_year, args.download)

    for year in years:
        # Locate or download file
        ext = "xlsx" if year >= 2013 else "xls"
        path = os.path.join(DATA_DIR, f"{year}.{ext}")

        if not os.path.exists(path):
            # Also check for alternative extension
            alt_ext = "xls" if ext == "xlsx" else "xlsx"
            alt_path = os.path.join(DATA_DIR, f"{year}.{alt_ext}")
            if os.path.exists(alt_path):
                path = alt_path
            elif args.download:
                path = download_file(year)
                if path is None:
                    continue
            else:
                logger.warning("  ⚠ %d.%s not found — skip  (use --download to fetch)", year, ext)
                continue

        rows = parse_year_file(path, year)
        if not rows:
            logger.warning("  ⚠ %d: 0 parseable rows", year)
            continue

        m, h = insert_rows(conn, rows)
        total_m += m
        total_h += h
        logger.info("  %d: %5d rows parsed → %5d matches, %5d h2h inserted", year, len(rows), m, h)

    conn.close()
    logger.info("━━━ Done: %d matches, %d h2h rows across %d years ━━━", total_m, total_h, len(years))


if __name__ == "__main__":
    main()
