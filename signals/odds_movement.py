"""
signals/odds_movement.py
Odds movement tracking and filtering for TennisEdge.

Tracks opening and closing odds for each match, classifies line movement
as "with" / "against" / "neutral", and provides a filter to suppress
signals where sharp money is moving against the model's pick.

Schema:
  - odds_tracking table: stores first-seen (open) and latest (close) odds per match
  - signals table: adds odds_open + odds_close columns (migration-safe)
"""

import logging
from database.db import get_conn

logger = logging.getLogger(__name__)

# Movement threshold: < 3% change is "neutral"
MOVEMENT_THRESHOLD = 0.03


# ── Schema migration ──────────────────────────────────────────────────────────

def migrate_odds_movement():
    """
    Safely add odds movement tracking to the database.
    Creates the odds_tracking table and adds columns to signals.
    Idempotent — safe to call on every startup.
    """
    conn = get_conn()
    try:
        # 1) Create the odds_tracking table for live tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS odds_tracking (
                match_id   TEXT PRIMARY KEY,
                player_a   TEXT NOT NULL,
                player_b   TEXT NOT NULL,
                odds_a_open  REAL NOT NULL,
                odds_b_open  REAL NOT NULL,
                odds_a_close REAL NOT NULL,
                odds_b_close REAL NOT NULL,
                first_seen   TEXT DEFAULT (datetime('now')),
                last_updated TEXT DEFAULT (datetime('now'))
            )
        """)

        # 2) Add odds_open + odds_close columns to signals table if missing
        cur = conn.execute("PRAGMA table_info(signals)")
        columns = {row[1] for row in cur.fetchall()}
        if "odds_open" not in columns:
            conn.execute("ALTER TABLE signals ADD COLUMN odds_open REAL")
            logger.info("[OddsMovement] Migrated: added 'odds_open' to signals.")
        if "odds_close" not in columns:
            conn.execute("ALTER TABLE signals ADD COLUMN odds_close REAL")
            logger.info("[OddsMovement] Migrated: added 'odds_close' to signals.")
        if "movement" not in columns:
            conn.execute("ALTER TABLE signals ADD COLUMN movement TEXT")
            logger.info("[OddsMovement] Migrated: added 'movement' to signals.")

        conn.commit()
        logger.info("[OddsMovement] Schema migration complete.")
    except Exception:
        conn.rollback()
        raise


# ── Odds tracking ─────────────────────────────────────────────────────────────

def track_odds(match_id: str, player_a: str, player_b: str,
               odds_a: float, odds_b: float):
    """
    Record odds for a match. First call stores opening odds;
    subsequent calls update closing odds only (never overwrites open).
    """
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO odds_tracking
                (match_id, player_a, player_b, odds_a_open, odds_b_open, odds_a_close, odds_b_close)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (match_id) DO UPDATE SET
                odds_a_close = excluded.odds_a_close,
                odds_b_close = excluded.odds_b_close,
                last_updated = datetime('now')
        """, (match_id, player_a, player_b, odds_a, odds_b, odds_a, odds_b))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_odds_tracking(match_id: str) -> dict | None:
    """Retrieve tracked odds for a match. Returns None if never tracked."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT match_id, player_a, player_b, "
            "odds_a_open, odds_b_open, odds_a_close, odds_b_close, "
            "first_seen, last_updated "
            "FROM odds_tracking WHERE match_id = ?",
            (match_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "match_id": row[0],
            "player_a": row[1], "player_b": row[2],
            "odds_a_open": row[3], "odds_b_open": row[4],
            "odds_a_close": row[5], "odds_b_close": row[6],
            "first_seen": row[7], "last_updated": row[8],
        }
    except Exception:
        conn.rollback()
        raise


# ── Movement classification ───────────────────────────────────────────────────

def line_movement(odds_open: float, odds_close: float) -> str:
    """
    Classify how odds moved for the player we're betting on.

    Args:
        odds_open:  Opening decimal odds (first time we saw this match)
        odds_close: Closing decimal odds (at signal send time)

    Returns:
        "with"    — odds shortened (e.g. 1.80 → 1.65), sharp money agrees
        "against" — odds drifted  (e.g. 1.80 → 1.95), sharp money disagrees
        "neutral" — movement < 3%, no meaningful signal
    """
    if odds_open is None or odds_close is None or odds_open <= 0:
        return "neutral"

    # Percentage change: negative = odds shortened (good), positive = drifted (bad)
    pct_change = (odds_close - odds_open) / odds_open

    if pct_change < -MOVEMENT_THRESHOLD:
        return "with"       # odds dropped = money flowing onto this player
    elif pct_change > MOVEMENT_THRESHOLD:
        return "against"    # odds lengthened = money flowing away
    else:
        return "neutral"


def get_movement_for_bet(match_id: str, bet_on: str) -> tuple[str, float | None, float | None]:
    """
    Get the line movement classification for the player we're betting on.

    Returns: (movement_str, odds_open, odds_close)
    """
    tracking = get_odds_tracking(match_id)
    if not tracking:
        return "neutral", None, None

    # Determine which player's odds to compare
    if bet_on == tracking["player_a"]:
        odds_open = tracking["odds_a_open"]
        odds_close = tracking["odds_a_close"]
    elif bet_on == tracking["player_b"]:
        odds_open = tracking["odds_b_open"]
        odds_close = tracking["odds_b_close"]
    else:
        # Name mismatch — fall back to neutral
        logger.warning(
            "[OddsMovement] Player '%s' not found in tracking for %s (have %s vs %s)",
            bet_on, match_id, tracking["player_a"], tracking["player_b"],
        )
        return "neutral", None, None

    movement = line_movement(odds_open, odds_close)
    return movement, odds_open, odds_close


# ── Signal filter ─────────────────────────────────────────────────────────────

def should_suppress_signal(match_id: str, bet_on: str) -> bool:
    """
    Returns True if the signal should be suppressed (not sent).
    Suppresses when sharp money is moving against our pick.
    """
    movement, odds_open, odds_close = get_movement_for_bet(match_id, bet_on)
    if movement == "against":
        logger.info(
            "[OddsMovement] SUPPRESSED signal on %s for %s — "
            "odds drifted %.2f → %.2f (against)",
            match_id, bet_on, odds_open or 0, odds_close or 0,
        )
        return True
    return False


def annotate_signal(signal: dict) -> dict:
    """
    Enrich a signal dict with odds movement data.
    Called after edge detection, before DB save.
    """
    match_id = signal.get("match_id")
    bet_on = signal.get("bet_on")
    if not match_id or not bet_on:
        return signal

    movement, odds_open, odds_close = get_movement_for_bet(match_id, bet_on)
    signal["odds_open"] = odds_open
    signal["odds_close"] = odds_close
    signal["movement"] = movement
    return signal
