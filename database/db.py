"""
database/db.py  –  PostgreSQL database + all helpers

Thread-safe via per-thread connections with psycopg2.
"""

import logging
import threading
import psycopg2
import psycopg2.extras
from config import DATABASE_URL
from utils import normalize_player_name

logger = logging.getLogger(__name__)

_local = threading.local()


class _ConnWrapper:
    """Thin wrapper providing sqlite3-compatible .execute() on a psycopg2 connection."""

    def __init__(self, raw):
        self._conn = raw

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql, params_list):
        cur = self._conn.cursor()
        cur.executemany(sql, params_list)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    @property
    def closed(self):
        return self._conn.closed

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *args):
        return self._conn.__exit__(*args)


def get_conn() -> _ConnWrapper:
    """Return a thread-local PostgreSQL connection."""
    wrapper = getattr(_local, "conn", None)
    if wrapper is None or wrapper.closed:
        raw = psycopg2.connect(DATABASE_URL)
        wrapper = _ConnWrapper(raw)
        _local.conn = wrapper
    return wrapper


def release_conn(conn):
    """No-op (connections are thread-local and reused)."""
    pass


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS users (
        id          SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE NOT NULL,
        username    TEXT,
        credits     INTEGER DEFAULT 0,
        created_at  TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS transactions (
        id             SERIAL PRIMARY KEY,
        user_id        INTEGER REFERENCES users(id),
        credits_added  INTEGER,
        payment_amount REAL DEFAULT 0,
        payment_id     TEXT,
        package        TEXT,
        timestamp      TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS signals (
        id           SERIAL PRIMARY KEY,
        match_id     TEXT UNIQUE,
        tournament   TEXT,
        surface      TEXT,
        player_a     TEXT,
        player_b     TEXT,
        bet_on       TEXT,
        model_prob   REAL,
        market_prob  REAL,
        edge         REAL,
        odds         REAL,
        result       TEXT DEFAULT 'pending',
        result_recorded_at TIMESTAMP,
        created_at   TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS signal_deliveries (
        id           SERIAL PRIMARY KEY,
        signal_id    INTEGER REFERENCES signals(id),
        user_id      INTEGER REFERENCES users(id),
        delivered_at TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS signal_results (
        id            SERIAL PRIMARY KEY,
        signal_id     INTEGER REFERENCES signals(id) UNIQUE,
        actual_winner TEXT NOT NULL,
        is_correct    INTEGER NOT NULL DEFAULT 0,
        recorded_at   TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS player_elo (
        id             SERIAL PRIMARY KEY,
        player_name    TEXT NOT NULL,
        surface        TEXT NOT NULL,
        elo_rating     REAL DEFAULT 1500,
        matches_played INTEGER DEFAULT 0,
        updated_at     TIMESTAMP DEFAULT NOW(),
        UNIQUE(player_name, surface)
    )""",
    """CREATE TABLE IF NOT EXISTS elo_history (
        id             SERIAL PRIMARY KEY,
        event_key      TEXT UNIQUE NOT NULL,
        event_date     DATE NOT NULL,
        winner         TEXT NOT NULL,
        loser          TEXT NOT NULL,
        surface        TEXT NOT NULL,
        tournament     TEXT,
        source         TEXT DEFAULT 'allsportsapi',
        processed_at   TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS player_aliases (
        alias          TEXT PRIMARY KEY,
        canonical_name TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS matches (
        id          SERIAL PRIMARY KEY,
        date        TEXT,
        player1     TEXT,
        player2     TEXT,
        surface     TEXT,
        winner      TEXT,
        odds_p1     REAL,
        odds_p2     REAL,
        tournament  TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS h2h (
        id          SERIAL PRIMARY KEY,
        player1     TEXT,
        player2     TEXT,
        surface     TEXT,
        winner      TEXT,
        date        TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS player_stats (
        id               SERIAL PRIMARY KEY,
        player_name      TEXT NOT NULL,
        surface          TEXT NOT NULL,
        form_score       NUMERIC(5,4),
        surface_win_rate NUMERIC(5,4),
        matches_counted  INTEGER DEFAULT 0,
        updated_at       TIMESTAMP DEFAULT NOW(),
        UNIQUE(player_name, surface)
    )""",
    """CREATE TABLE IF NOT EXISTS h2h_records (
        id            SERIAL PRIMARY KEY,
        player_a      TEXT NOT NULL,
        player_b      TEXT NOT NULL,
        surface       TEXT NOT NULL,
        wins_a        INTEGER DEFAULT 0,
        wins_b        INTEGER DEFAULT 0,
        updated_at    TIMESTAMP DEFAULT NOW(),
        UNIQUE(player_a, player_b, surface)
    )""",
]


def init_schema():
    conn = get_conn()
    try:
        for stmt in _SCHEMA_STATEMENTS:
            conn.execute(stmt)
        conn.commit()
        _migrate_signals_result_columns(conn)
        from signals.odds_movement import migrate_odds_movement
        migrate_odds_movement()
        logger.info("[DB] PostgreSQL schema ready.")
    except Exception:
        conn.rollback()
        raise


def _migrate_signals_result_columns(conn):
    """Add result-related columns on signals if they don't exist."""
    cur = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'signals'"
    )
    columns = {row[0] for row in cur.fetchall()}
    if "result" not in columns:
        conn.execute("ALTER TABLE signals ADD COLUMN result TEXT DEFAULT 'pending'")
        logger.info("[DB] Migrated: added 'result' column to signals.")
    if "result_recorded_at" not in columns:
        conn.execute("ALTER TABLE signals ADD COLUMN result_recorded_at TIMESTAMP")
        logger.info("[DB] Migrated: added 'result_recorded_at' column to signals.")
    if "closing_odds" not in columns:
        conn.execute("ALTER TABLE signals ADD COLUMN closing_odds REAL")
        logger.info("[DB] Migrated: added 'closing_odds' column to signals.")
    conn.commit()


# ── User helpers ──────────────────────────────────────────────────────────────

def get_or_create_user(telegram_id: int, username: str = None) -> dict:
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO users (telegram_id, username) VALUES (%s, %s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET username = excluded.username",
            (telegram_id, username),
        )
        conn.commit()
        cur = conn.execute(
            "SELECT id, telegram_id, username, credits FROM users WHERE telegram_id = %s",
            (telegram_id,),
        )
        r = cur.fetchone()
        return {"id": r[0], "telegram_id": r[1], "username": r[2], "credits": r[3]}
    except Exception:
        conn.rollback()
        raise

def get_user(telegram_id: int) -> dict:
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, telegram_id, username, credits FROM users WHERE telegram_id = %s",
            (telegram_id,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {"id": r[0], "telegram_id": r[1], "username": r[2], "credits": r[3]}
    except Exception:
        conn.rollback()
        raise

def add_credits_manual(user_id: int, credits: int):
    """Admin command – add credits without payment. Returns new balance."""
    conn = get_conn()
    try:
        conn.execute("UPDATE users SET credits = credits + %s WHERE id = %s", (credits, user_id))
        conn.execute(
            "INSERT INTO transactions (user_id, credits_added, payment_amount, payment_id, package) "
            "VALUES (%s, %s, 0, 'manual', 'manual')",
            (user_id, credits),
        )
        cur = conn.execute("SELECT credits FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else 0
    except Exception:
        conn.rollback()
        raise

def deduct_credit(user_id: int) -> bool:
    conn = get_conn()
    try:
        cur = conn.execute(
            "UPDATE users SET credits = credits - 1 WHERE id = %s AND credits > 0",
            (user_id,),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        raise

def get_all_subscribers() -> list:
    """All users with credits > 0."""
    conn = get_conn()
    try:
        cur = conn.execute("SELECT telegram_id FROM users WHERE credits > 0")
        return [r[0] for r in cur.fetchall()]
    except Exception:
        conn.rollback()
        raise


def get_subscribers_with_info() -> list:
    """All users with credits > 0 — returns full user dicts (avoids N+1)."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, telegram_id, username, credits FROM users WHERE credits > 0"
        )
        return [
            {"id": r[0], "telegram_id": r[1], "username": r[2], "credits": r[3]}
            for r in cur.fetchall()
        ]
    except Exception:
        conn.rollback()
        raise


# ── Signal helpers ────────────────────────────────────────────────────────────

def signal_exists(match_id: str) -> bool:
    conn = get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM signals WHERE match_id = %s", (match_id,))
        return cur.fetchone() is not None
    except Exception:
        conn.rollback()
        raise

def save_signal(match_id, tournament, surface, player_a, player_b,
                bet_on, model_prob, market_prob, edge, odds) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO signals (match_id,tournament,surface,player_a,player_b,"
            "bet_on,model_prob,market_prob,edge,odds) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (match_id, tournament, surface, player_a, player_b,
             bet_on, model_prob, market_prob, edge, odds),
        )
        sid = cur.fetchone()[0]
        conn.commit()
        return sid
    except Exception:
        conn.rollback()
        raise

def get_recent_signals(limit: int = 5) -> list:
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id,tournament,surface,player_a,player_b,bet_on,"
            "model_prob,market_prob,edge,odds,created_at "
            "FROM signals ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        return [
            {
                "id": r[0], "tournament": r[1], "surface": r[2],
                "player_a": r[3], "player_b": r[4], "bet_on": r[5],
                "model_prob": float(r[6]), "market_prob": float(r[7]),
                "edge": float(r[8]), "odds": float(r[9]),
                "created_at": r[10],
            }
            for r in rows
        ]
    except Exception:
        conn.rollback()
        raise


# ── Delivery tracking ─────────────────────────────────────────────────────────

def record_delivery(signal_id: int, user_id: int):
    """Record that a signal was successfully delivered to a user."""
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO signal_deliveries (signal_id, user_id) VALUES (%s, %s)",
            (signal_id, user_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ── Signal result tracking ────────────────────────────────────────────────────

def record_signal_result(signal_id: int, actual_winner: str) -> dict:
    """Record the actual outcome of a signal. Returns the signal result info."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT bet_on, player_a, player_b FROM signals WHERE id = %s",
            (signal_id,),
        )
        signal = cur.fetchone()
        if not signal:
            return None

        bet_on = signal[0]
        norm_winner = normalize_player_name(actual_winner)
        norm_bet = normalize_player_name(bet_on)
        is_correct = 1 if norm_winner == norm_bet else 0

        conn.execute(
            "INSERT INTO signal_results (signal_id, actual_winner, is_correct) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (signal_id) DO UPDATE "
            "SET actual_winner = excluded.actual_winner, is_correct = excluded.is_correct, "
            "recorded_at = NOW()",
            (signal_id, actual_winner, is_correct),
        )
        conn.commit()
        return {
            "signal_id": signal_id,
            "bet_on": bet_on,
            "actual_winner": actual_winner,
            "is_correct": bool(is_correct),
        }
    except Exception:
        conn.rollback()
        raise

def get_signal_accuracy() -> dict:
    """Return overall signal accuracy and ROI stats."""
    conn = get_conn()
    try:
        cur = conn.execute("""
            SELECT
                SUM(CASE WHEN r.is_correct IN (0, 1) THEN 1 ELSE 0 END) AS total,
                SUM(CASE WHEN r.is_correct = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN r.is_correct = 0 THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN r.is_correct = 1 THEN s.odds - 1.0 WHEN r.is_correct = 0 THEN -1.0 ELSE 0 END) AS total_profit,
                AVG(CASE WHEN r.is_correct IN (0, 1) THEN s.odds ELSE NULL END) AS avg_odds
            FROM signal_results r
            JOIN signals s ON r.signal_id = s.id
        """)
        r = cur.fetchone()
        # Fallbacks to `0` instead of `None` in case the table has zero completed signal_results resulting in NULL SUMs out of postgres
        total = r[0] or 0
        wins, losses = r[1] or 0, r[2] or 0
        total_profit = r[3] or 0.0
        avg_odds = r[4] or 0.0
        
        accuracy = round(wins / total * 100, 1) if total > 0 else 0.0
        roi = round((total_profit / total) * 100, 1) if total > 0 else 0.0

        cur2 = conn.execute("SELECT COUNT(*) FROM signals")
        total_signals = cur2.fetchone()[0]

        return {
            "total_signals": total_signals,
            "tracked_results": total,
            "wins": wins,
            "losses": losses,
            "accuracy_pct": accuracy,
            "untracked": total_signals - total,
            "roi_pct": roi,
            "avg_odds": round(avg_odds, 2),
        }
    except Exception:
        conn.rollback()
        raise

def get_signal_by_id(signal_id: int) -> dict:
    """Fetch a single signal by ID."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, match_id, tournament, surface, player_a, player_b, "
            "bet_on, model_prob, market_prob, edge, odds, created_at "
            "FROM signals WHERE id = %s",
            (signal_id,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "id": r[0], "match_id": r[1], "tournament": r[2], "surface": r[3],
            "player_a": r[4], "player_b": r[5], "bet_on": r[6],
            "model_prob": float(r[7]), "market_prob": float(r[8]),
            "edge": float(r[9]), "odds": float(r[10]), "created_at": r[11],
        }
    except Exception:
        conn.rollback()
        raise


# ── Player name resolution ────────────────────────────────────────────────────

def resolve_player_name(name: str) -> str:
    """Normalize name, then check alias table for a canonical mapping."""
    canonical = normalize_player_name(name)
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT canonical_name FROM player_aliases WHERE alias = %s",
            (canonical,),
        )
        r = cur.fetchone()
        return r[0] if r else canonical
    except Exception:
        return canonical


def add_player_alias(alias: str, canonical_name: str):
    """Map an alternate name to a canonical name."""
    conn = get_conn()
    try:
        norm_alias = normalize_player_name(alias)
        norm_canonical = normalize_player_name(canonical_name)
        conn.execute(
            "INSERT INTO player_aliases (alias, canonical_name) VALUES (%s, %s) "
            "ON CONFLICT (alias) DO UPDATE SET canonical_name = excluded.canonical_name",
            (norm_alias, norm_canonical),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_player_aliases() -> list:
    """Return all alias mappings."""
    conn = get_conn()
    try:
        cur = conn.execute("SELECT alias, canonical_name FROM player_aliases ORDER BY alias")
        return [{"alias": r[0], "canonical": r[1]} for r in cur.fetchall()]
    except Exception:
        conn.rollback()
        raise


# ── Elo helpers ───────────────────────────────────────────────────────────────

def get_elo(player: str, surface: str) -> float:
    player = resolve_player_name(player)
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT elo_rating FROM player_elo WHERE player_name=%s AND surface=%s",
            (player, surface),
        )
        r = cur.fetchone()
        return float(r[0]) if r else 1500.0
    except Exception:
        conn.rollback()
        raise

def upsert_elo(player: str, surface: str, new_rating: float):
    player = resolve_player_name(player)
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO player_elo (player_name,surface,elo_rating,matches_played) "
            "VALUES (%s,%s,%s,1) "
            "ON CONFLICT (player_name,surface) DO UPDATE "
            "SET elo_rating=%s, matches_played=player_elo.matches_played+1, "
            "updated_at=NOW()",
            (player, surface, new_rating, new_rating),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_elo_player_count() -> int:
    """Return the number of unique players with Elo ratings."""
    conn = get_conn()
    try:
        cur = conn.execute("SELECT COUNT(DISTINCT player_name) FROM player_elo")
        return cur.fetchone()[0]
    except Exception:
        conn.rollback()
        raise


def elo_history_event_exists(event_key: str) -> bool:
    """Return True if an event_key has already been processed by daily Elo updater."""
    conn = get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM elo_history WHERE event_key = %s", (str(event_key),))
        return cur.fetchone() is not None
    except Exception:
        conn.rollback()
        raise


def record_elo_history_event(
    event_key: str,
    event_date: str,
    winner: str,
    loser: str,
    surface: str,
    tournament: str = None,
    source: str = "allsportsapi",
):
    """Insert a processed Elo-update event with dedupe on event_key."""
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO elo_history (event_key, event_date, winner, loser, surface, tournament, source) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (event_key) DO NOTHING",
            (str(event_key), event_date, winner, loser, surface, tournament, source),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ── Player stats helpers (advanced model) ─────────────────────────────────────

def get_player_stats(player: str, surface: str) -> dict:
    """Get player form score and surface win rate. Returns None if not found."""
    player = resolve_player_name(player)
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT form_score, surface_win_rate, matches_counted "
            "FROM player_stats WHERE player_name=%s AND surface=%s",
            (player, surface),
        )
        r = cur.fetchone()
        if r:
            return {
                "form_score": float(r[0]) if r[0] is not None else None,
                "surface_win_rate": float(r[1]) if r[1] is not None else None,
                "matches_counted": r[2] or 0,
            }
        return None
    except Exception:
        conn.rollback()
        raise


def upsert_player_stats(player: str, surface: str, form_score: float,
                        surface_win_rate: float, matches_counted: int):
    player = resolve_player_name(player)
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO player_stats (player_name, surface, form_score, "
            "surface_win_rate, matches_counted) VALUES (%s,%s,%s,%s,%s) "
            "ON CONFLICT (player_name, surface) DO UPDATE SET "
            "form_score=%s, surface_win_rate=%s, matches_counted=%s, updated_at=NOW()",
            (player, surface, form_score, surface_win_rate, matches_counted,
             form_score, surface_win_rate, matches_counted),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_h2h_record(player_a: str, player_b: str, surface: str) -> dict:
    """Get H2H record. Players are sorted alphabetically internally.
    Returns {"wins_a": int, "wins_b": int} where a/b are the sorted names."""
    player_a = resolve_player_name(player_a)
    player_b = resolve_player_name(player_b)
    sorted_a, sorted_b = sorted([player_a, player_b])
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT wins_a, wins_b FROM h2h_records "
            "WHERE player_a=%s AND player_b=%s AND surface=%s",
            (sorted_a, sorted_b, surface),
        )
        r = cur.fetchone()
        if r:
            wa, wb = r[0], r[1]
            # If caller's player_a matches sorted_a, wins are in correct order
            if player_a == sorted_a:
                return {"wins_a": wa, "wins_b": wb}
            else:
                return {"wins_a": wb, "wins_b": wa}
        return None
    except Exception:
        conn.rollback()
        raise


def upsert_h2h(player_a: str, player_b: str, surface: str,
               wins_a: int, wins_b: int):
    """Store H2H record. Players are sorted alphabetically."""
    player_a = resolve_player_name(player_a)
    player_b = resolve_player_name(player_b)
    sorted_a, sorted_b = sorted([player_a, player_b])
    # Ensure wins match sorted order
    if player_a == sorted_a:
        wa, wb = wins_a, wins_b
    else:
        wa, wb = wins_b, wins_a
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO h2h_records (player_a, player_b, surface, wins_a, wins_b) "
            "VALUES (%s,%s,%s,%s,%s) "
            "ON CONFLICT (player_a, player_b, surface) DO UPDATE SET "
            "wins_a=%s, wins_b=%s, updated_at=NOW()",
            (sorted_a, sorted_b, surface, wa, wb, wa, wb),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ── Signal result tracking ────────────────────────────────────────────────────

def get_pending_signals(max_age_days: int = 7) -> list:
    """Return signals with result='pending' created within max_age_days."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, match_id, player_a, player_b, bet_on, surface, edge, odds, created_at "
            "FROM signals WHERE result='pending' "
            "AND created_at >= NOW() - %s * INTERVAL '1 day'",
            (max_age_days,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        conn.rollback()
        raise


def update_signal_result(
    signal_id: int,
    result: str,
    actual_winner: str = None,
    closing_odds: float = None,
):
    """Set signal result with timestamp and optional closing odds."""
    conn = get_conn()
    try:
        if closing_odds is None:
            conn.execute(
                "UPDATE signals SET result=%s, result_recorded_at=NOW() WHERE id=%s",
                (result, signal_id),
            )
        else:
            conn.execute(
                "UPDATE signals SET result=%s, result_recorded_at=NOW(), closing_odds=%s WHERE id=%s",
                (result, closing_odds, signal_id),
            )
        if actual_winner and result in ("win", "loss"):
            actual_winner = normalize_player_name(actual_winner)
            is_correct_val = 1 if result == 'win' else 0
            conn.execute(
                "INSERT INTO signal_results (signal_id, actual_winner, is_correct, recorded_at) "
                "VALUES (%s, %s, %s, NOW()) "
                "ON CONFLICT (signal_id) DO UPDATE "
                "SET actual_winner = excluded.actual_winner, is_correct = excluded.is_correct, "
                "recorded_at = NOW()",
                (signal_id, actual_winner, is_correct_val),
            )
        elif result == "push":
            conn.execute(
                "INSERT INTO signal_results (signal_id, actual_winner, is_correct, recorded_at) "
                "VALUES (%s, 'Push', -1, NOW()) "
                "ON CONFLICT (signal_id) DO UPDATE "
                "SET actual_winner = excluded.actual_winner, is_correct = -1, "
                "recorded_at = NOW()",
                (signal_id,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_user_credits(telegram_id: int) -> int:
    """Return credit balance for a user by telegram_id."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT credits FROM users WHERE telegram_id=%s", (telegram_id,)
        )
        row = cur.fetchone()
        return row[0] if row else 0
    except Exception:
        conn.rollback()
        raise


def get_stats_by_surface() -> list:
    """Return signal win/loss stats grouped by surface."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT surface, "
            "  COUNT(*) as total, "
            "  SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins, "
            "  SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses, "
            "  AVG(CASE WHEN result='win' THEN edge ELSE NULL END) as avg_edge_wins, "
            "  AVG(CASE WHEN result='loss' THEN edge ELSE NULL END) as avg_edge_losses "
            "FROM signals WHERE result IN ('win', 'loss') "
            "GROUP BY surface ORDER BY total DESC"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        conn.rollback()
        raise


def get_signal_by_match_id(match_id: str) -> dict:
    """Return a signal by match_id, or None."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, match_id, player_a, player_b, bet_on, surface, edge, odds, result "
            "FROM signals WHERE match_id=%s", (match_id,)
        )
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        return None
    except Exception:
        conn.rollback()
        raise
