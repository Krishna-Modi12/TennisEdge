"""
database/db.py  –  PostgreSQL database + all helpers

Thread-safe via per-thread connections with psycopg2.
"""

import logging
import re
import threading
import hashlib
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

    def cursor(self, *args, **kwargs):
        return self._conn.cursor(*args, **kwargs)

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
        calibrated_prob REAL,
        raw_model_prob REAL,
        elo_component_prob REAL,
        strength_component_prob REAL,
        mc_component_prob REAL,
        market_component_prob REAL,
        ensemble_prob REAL,
        edge         REAL,
        odds         REAL,
        volatility   REAL,
        edge_threshold REAL,
        kelly_fraction REAL,
        recommended_bet_size REAL,
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
    """CREATE TABLE IF NOT EXISTS api_cache (
        key        TEXT PRIMARY KEY,
        value      JSONB,
        expires_at TIMESTAMP
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
    """CREATE TABLE IF NOT EXISTS sent_signals (
        signal_hash TEXT PRIMARY KEY,
        match_id    TEXT NOT NULL,
        sent_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS signal_performance (
        id           SERIAL PRIMARY KEY,
        match_id     TEXT,
        surface      TEXT,
        taken_odds   NUMERIC,
        closing_odds NUMERIC,
        model_prob   NUMERIC,
        is_win       BOOLEAN,
        clv_ratio    NUMERIC,
        line_movement NUMERIC
    )""",
    """CREATE TABLE IF NOT EXISTS player_surface_stats (
        id                    SERIAL PRIMARY KEY,
        player_name           TEXT NOT NULL,
        surface               TEXT NOT NULL,
        service_points_won    NUMERIC DEFAULT 0,
        total_service_points  NUMERIC DEFAULT 0,
        return_points_won     NUMERIC DEFAULT 0,
        total_return_points   NUMERIC DEFAULT 0,
        serve_strength        NUMERIC,
        return_strength       NUMERIC,
        matches_counted       INTEGER DEFAULT 0,
        updated_at            TIMESTAMP DEFAULT NOW(),
        UNIQUE(player_name, surface)
    )""",
    """CREATE TABLE IF NOT EXISTS model_parameters (
        name        TEXT PRIMARY KEY,
        value_num   NUMERIC,
        value_text  TEXT,
        value_json  JSONB,
        updated_at  TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS backtest_results (
        id             SERIAL PRIMARY KEY,
        run_name       TEXT,
        roi_pct        NUMERIC,
        win_rate_pct   NUMERIC,
        clv_avg        NUMERIC,
        bankroll_start NUMERIC,
        bankroll_end   NUMERIC,
        summary        JSONB,
        created_at     TIMESTAMP DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS unmatched_players (
        id         SERIAL PRIMARY KEY,
        name       TEXT UNIQUE NOT NULL,
        first_seen TIMESTAMP DEFAULT NOW()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_sent_signals_match ON sent_signals(match_id)",
    "CREATE INDEX IF NOT EXISTS idx_signal_perf_match ON signal_performance(match_id)",
    "CREATE INDEX IF NOT EXISTS idx_api_cache_expiry ON api_cache(expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_player_surface_stats_lookup ON player_surface_stats(player_name, surface)",
    "CREATE INDEX IF NOT EXISTS idx_backtest_results_created_at ON backtest_results(created_at)",
]


def init_schema():
    conn = get_conn()
    try:
        # Enable JSONB support for dict conversion
        psycopg2.extras.register_default_jsonb(conn._conn)

        for stmt in _SCHEMA_STATEMENTS:
            conn.execute(stmt)
        conn.commit()
        _migrate_signals_result_columns(conn)
        _migrate_risk_tables(conn)
        from signals.odds_movement import migrate_odds_movement
        migrate_odds_movement()
        logger.info("[DB] PostgreSQL schema ready.")
    except Exception:
        conn.rollback()
        raise



def _migrate_signals_result_columns(conn):
    """Add result-related columns on signals if they don't exist."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'signals'"
        )
        columns = {row[0] for row in cur.fetchall()}
        if "result" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN result TEXT DEFAULT 'pending'")
            logger.info("[DB] Migrated: added 'result' column to signals.")
        if "result_recorded_at" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN result_recorded_at TIMESTAMP")
            logger.info("[DB] Migrated: added 'result_recorded_at' column to signals.")
        if "closing_odds" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN closing_odds REAL")
            logger.info("[DB] Migrated: added 'closing_odds' column to signals.")
        if "calibrated_prob" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN calibrated_prob REAL")
            logger.info("[DB] Migrated: added 'calibrated_prob' column to signals.")
        if "raw_model_prob" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN raw_model_prob REAL")
            logger.info("[DB] Migrated: added 'raw_model_prob' column to signals.")
        if "elo_component_prob" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN elo_component_prob REAL")
            logger.info("[DB] Migrated: added 'elo_component_prob' column to signals.")
        if "strength_component_prob" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN strength_component_prob REAL")
            logger.info("[DB] Migrated: added 'strength_component_prob' column to signals.")
        if "mc_component_prob" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN mc_component_prob REAL")
            logger.info("[DB] Migrated: added 'mc_component_prob' column to signals.")
        if "market_component_prob" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN market_component_prob REAL")
            logger.info("[DB] Migrated: added 'market_component_prob' column to signals.")
        if "ensemble_prob" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN ensemble_prob REAL")
            logger.info("[DB] Migrated: added 'ensemble_prob' column to signals.")
        if "volatility" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN volatility REAL")
            logger.info("[DB] Migrated: added 'volatility' column to signals.")
        if "edge_threshold" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN edge_threshold REAL")
            logger.info("[DB] Migrated: added 'edge_threshold' column to signals.")
        if "kelly_fraction" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN kelly_fraction REAL")
            logger.info("[DB] Migrated: added 'kelly_fraction' column to signals.")
        if "recommended_bet_size" not in columns:
            cur.execute("ALTER TABLE signals ADD COLUMN recommended_bet_size REAL")
            logger.info("[DB] Migrated: added 'recommended_bet_size' column to signals.")
    conn.commit()


def _migrate_risk_tables(conn):
    """
    Backward-compatible migrations for risk mitigation tables.
    Safe to run repeatedly on startup.
    """
    with conn.cursor() as cur:
        # sent_signals: move to signal_hash PK model while preserving existing rows.
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'sent_signals'"
        )
        sent_cols = {row[0] for row in cur.fetchall()}

        if "signal_hash" not in sent_cols:
            cur.execute("ALTER TABLE sent_signals ADD COLUMN signal_hash TEXT")
            sent_cols.add("signal_hash")
        if "match_id" not in sent_cols:
            cur.execute("ALTER TABLE sent_signals ADD COLUMN match_id TEXT")
            sent_cols.add("match_id")
        if "sent_at" not in sent_cols:
            cur.execute(
                "ALTER TABLE sent_signals ADD COLUMN sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )
            sent_cols.add("sent_at")

        if {"market", "selection"}.issubset(sent_cols):
            cur.execute(
                "SELECT ctid, match_id, market, selection "
                "FROM sent_signals WHERE signal_hash IS NULL"
            )
            for ctid, match_id, market, selection in cur.fetchall():
                norm_selection = str(selection or "").lower().strip()
                digest = hashlib.sha1(
                    f"{match_id}:{market}:{norm_selection}".encode()
                ).hexdigest()
                cur.execute(
                    "UPDATE sent_signals SET signal_hash = %s WHERE ctid = %s",
                    (digest, ctid),
                )
        else:
            cur.execute(
                "SELECT ctid, match_id, sent_at FROM sent_signals WHERE signal_hash IS NULL"
            )
            for ctid, match_id, sent_at in cur.fetchall():
                digest = hashlib.sha1(
                    f"{match_id}:legacy:{sent_at}".encode()
                ).hexdigest()
                cur.execute(
                    "UPDATE sent_signals SET signal_hash = %s WHERE ctid = %s",
                    (digest, ctid),
                )

        cur.execute(
            "DELETE FROM sent_signals a USING sent_signals b "
            "WHERE a.ctid < b.ctid "
            "AND a.signal_hash IS NOT NULL "
            "AND a.signal_hash = b.signal_hash"
        )
        cur.execute(
            "ALTER TABLE sent_signals "
            "ALTER COLUMN signal_hash SET NOT NULL"
        )
        cur.execute("ALTER TABLE sent_signals DROP CONSTRAINT IF EXISTS sent_signals_pkey")
        cur.execute("ALTER TABLE sent_signals ADD PRIMARY KEY (signal_hash)")

        # signal_performance: ensure required columns exist.
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'signal_performance'"
        )
        perf_cols = {row[0] for row in cur.fetchall()}

        if "surface" not in perf_cols:
            cur.execute("ALTER TABLE signal_performance ADD COLUMN surface TEXT")
        if "model_prob" not in perf_cols:
            cur.execute("ALTER TABLE signal_performance ADD COLUMN model_prob NUMERIC")
        if "is_win" not in perf_cols:
            cur.execute("ALTER TABLE signal_performance ADD COLUMN is_win BOOLEAN")
        if "line_movement" not in perf_cols:
            cur.execute("ALTER TABLE signal_performance ADD COLUMN line_movement NUMERIC")

        # Backfill from legacy columns when available.
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'signal_performance'"
        )
        perf_cols = {row[0] for row in cur.fetchall()}

        if {"signal_id", "surface", "model_prob"}.issubset(perf_cols):
            cur.execute(
                "UPDATE signal_performance sp "
                "SET surface = COALESCE(sp.surface, s.surface), "
                "model_prob = COALESCE(sp.model_prob, s.model_prob) "
                "FROM signals s "
                "WHERE sp.signal_id = s.id "
                "AND (sp.surface IS NULL OR sp.model_prob IS NULL)"
            )

        if {"result", "is_win"}.issubset(perf_cols):
            cur.execute(
                "UPDATE signal_performance "
                "SET is_win = CASE "
                "WHEN result = 'win' THEN TRUE "
                "WHEN result = 'loss' THEN FALSE "
                "ELSE NULL END "
                "WHERE is_win IS NULL"
            )

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


def deduct_credit_atomic(telegram_id: int) -> bool:
    """
    Atomically deduct one credit using SELECT ... FOR UPDATE.
    Returns False when user does not exist or has insufficient credits.
    """
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, credits FROM users WHERE telegram_id = %s FOR UPDATE",
            (telegram_id,),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return False

        user_id, credits = row
        if (credits or 0) < 1:
            conn.rollback()
            return False

        conn.execute("UPDATE users SET credits = credits - 1 WHERE id = %s", (user_id,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def get_all_user_telegram_ids() -> list:
    """All registered users (for broad announcements like beta-channel invites)."""
    conn = get_conn()
    try:
        cur = conn.execute("SELECT telegram_id FROM users ORDER BY created_at ASC")
        return [r[0] for r in cur.fetchall()]
    except Exception:
        conn.rollback()
        raise


# ── Signal helpers ────────────────────────────────────────────────────────────

def signal_exists(match_id: str) -> bool:
    # Test fixtures must never be treated as duplicates
    if match_id.startswith("test_"):
        return False
    conn = get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM signals WHERE match_id = %s", (match_id,))
        return cur.fetchone() is not None
    except Exception:
        conn.rollback()
        raise

def save_signal(
    match_id,
    tournament,
    surface,
    player_a,
    player_b,
    bet_on,
    model_prob,
    market_prob,
    edge,
    odds,
    calibrated_prob=None,
    raw_model_prob=None,
    elo_component_prob=None,
    strength_component_prob=None,
    mc_component_prob=None,
    market_component_prob=None,
    ensemble_prob=None,
    volatility=None,
    edge_threshold=None,
    kelly_fraction=None,
    recommended_bet_size=None,
) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO signals (match_id,tournament,surface,player_a,player_b,"
            "bet_on,model_prob,market_prob,calibrated_prob,raw_model_prob,"
            "elo_component_prob,strength_component_prob,mc_component_prob,market_component_prob,ensemble_prob,"
            "edge,odds,"
            "volatility,edge_threshold,kelly_fraction,recommended_bet_size) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (match_id, tournament, surface, player_a, player_b,
             bet_on, model_prob, market_prob, calibrated_prob, raw_model_prob,
             elo_component_prob, strength_component_prob, mc_component_prob,
             market_component_prob, ensemble_prob,
             edge, odds, volatility, edge_threshold, kelly_fraction, recommended_bet_size),
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


def is_signal_alert_sent(signal_hash: str) -> bool:
    """Return True if this alert hash was sent in the last 6 hours."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM sent_signals "
                "WHERE signal_hash = %s "
                "AND sent_at > NOW() - INTERVAL '6 hours' "
                "LIMIT 1",
                (signal_hash,),
            )
            return cur.fetchone() is not None
    except psycopg2.Error as e:
        conn.rollback()
        logger.warning("[DB] is_signal_alert_sent failed: %s", e)
        raise


def record_signal_alert(signal_hash: str, match_id: str) -> bool:
    """Record or refresh a dispatched signal hash."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sent_signals (signal_hash, match_id) VALUES (%s, %s) "
                "ON CONFLICT (signal_hash) DO UPDATE "
                "SET match_id = excluded.match_id, sent_at = CURRENT_TIMESTAMP",
                (signal_hash, match_id),
            )
            conn.commit()
            return cur.rowcount > 0
    except psycopg2.Error as e:
        conn.rollback()
        logger.warning("[DB] record_signal_alert failed: %s", e)
        raise


def get_resolved_signals_for_performance(limit: int = 500) -> list:
    """Fetch recently resolved signals required for performance/CLV tracking."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT match_id, surface, odds, closing_odds, model_prob, result "
                "FROM signals "
                "WHERE result IN ('win', 'loss', 'push') "
                "ORDER BY result_recorded_at DESC NULLS LAST, id DESC "
                "LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()

        mapped = []
        for match_id, surface, taken_odds, closing_odds, model_prob, result in rows:
            if result == "win":
                is_win = True
            elif result == "loss":
                is_win = False
            else:
                is_win = None
            mapped.append(
                {
                    "match_id": match_id,
                    "surface": surface,
                    "taken_odds": float(taken_odds) if taken_odds is not None else None,
                    "closing_odds": float(closing_odds) if closing_odds is not None else None,
                    "model_prob": float(model_prob) if model_prob is not None else None,
                    "is_win": is_win,
                    "result": result,
                }
            )
        return mapped
    except psycopg2.Error as e:
        conn.rollback()
        logger.warning("[DB] get_resolved_signals_for_performance failed: %s", e)
        raise


def upsert_signal_performance(
    match_id: str,
    surface: str,
    taken_odds: float,
    closing_odds: float = None,
    model_prob: float = None,
    is_win: bool = None,
    clv_ratio: float = None,
    line_movement: float = None,
):
    """Insert or update a tracked signal performance row by match_id."""
    if not match_id:
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE signal_performance SET "
                "surface = %s, taken_odds = %s, closing_odds = COALESCE(%s, closing_odds), "
                "model_prob = %s, is_win = %s, clv_ratio = COALESCE(%s, clv_ratio), "
                "line_movement = COALESCE(%s, line_movement) "
                "WHERE match_id = %s",
                (
                    surface,
                    taken_odds,
                    closing_odds,
                    model_prob,
                    is_win,
                    clv_ratio,
                    line_movement,
                    match_id,
                ),
            )
            if cur.rowcount == 0:
                cur.execute(
                    "INSERT INTO signal_performance "
                    "(match_id, surface, taken_odds, closing_odds, model_prob, is_win, clv_ratio, line_movement) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        match_id,
                        surface,
                        taken_odds,
                        closing_odds,
                        model_prob,
                        is_win,
                        clv_ratio,
                        line_movement,
                    ),
                )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.warning("[DB] upsert_signal_performance failed: %s", e)
        raise


def get_recent_signal_performance(limit: int = 100) -> list:
    """Fetch latest signal_performance rows for monitoring and tuning."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT match_id, taken_odds, model_prob, is_win, clv_ratio, surface, line_movement "
            "FROM signal_performance "
            "WHERE taken_odds IS NOT NULL AND model_prob IS NOT NULL "
            "ORDER BY id DESC "
            "LIMIT %s",
            (int(limit),),
        )
        rows = cur.fetchall()
        out = []
        for row in rows:
            out.append(
                {
                    "match_id": row[0],
                    "taken_odds": float(row[1]) if row[1] is not None else None,
                    "model_prob": float(row[2]) if row[2] is not None else None,
                    "is_win": row[3],
                    "clv_ratio": float(row[4]) if row[4] is not None else None,
                    "surface": row[5],
                    "line_movement": float(row[6]) if row[6] is not None else None,
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise


def get_model_parameter(name: str, default=None):
    """Get adaptive model parameter value by name."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT value_num, value_text, value_json FROM model_parameters WHERE name = %s",
            (name,),
        )
        row = cur.fetchone()
        if not row:
            return default
        value_num, value_text, value_json = row
        if value_json is not None:
            return value_json
        if value_num is not None:
            return float(value_num)
        if value_text is not None:
            return value_text
        return default
    except Exception:
        conn.rollback()
        raise


def set_model_parameter(name: str, value):
    """Set or update adaptive model parameter value."""
    conn = get_conn()
    try:
        value_num = None
        value_text = None
        value_json = None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            value_num = float(value)
        elif isinstance(value, (dict, list)):
            value_json = psycopg2.extras.Json(value)
        elif value is not None:
            value_text = str(value)

        conn.execute(
            "INSERT INTO model_parameters (name, value_num, value_text, value_json, updated_at) "
            "VALUES (%s, %s, %s, %s, NOW()) "
            "ON CONFLICT (name) DO UPDATE SET "
            "value_num = excluded.value_num, value_text = excluded.value_text, "
            "value_json = excluded.value_json, updated_at = NOW()",
            (name, value_num, value_text, value_json),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_all_model_parameters() -> list:
    """Return all adaptive model parameters."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT name, value_num, value_text, value_json, updated_at "
            "FROM model_parameters ORDER BY name"
        )
        rows = cur.fetchall()
        out = []
        for name, value_num, value_text, value_json, updated_at in rows:
            if value_json is not None:
                value = value_json
            elif value_num is not None:
                value = float(value_num)
            else:
                value = value_text
            out.append({"name": name, "value": value, "updated_at": updated_at})
        return out
    except Exception:
        conn.rollback()
        raise


def save_backtest_result(
    run_name: str,
    roi_pct: float,
    win_rate_pct: float,
    clv_avg: float = None,
    bankroll_start: float = None,
    bankroll_end: float = None,
    summary: dict | None = None,
) -> int:
    """Persist one backtest run summary."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO backtest_results "
            "(run_name, roi_pct, win_rate_pct, clv_avg, bankroll_start, bankroll_end, summary) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (
                run_name,
                roi_pct,
                win_rate_pct,
                clv_avg,
                bankroll_start,
                bankroll_end,
                psycopg2.extras.Json(summary) if summary is not None else None,
            ),
        )
        backtest_id = cur.fetchone()[0]
        conn.commit()
        return backtest_id
    except Exception:
        conn.rollback()
        raise


def get_recent_backtest_results(limit: int = 20) -> list:
    """Fetch recent persisted backtest summaries."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT id, run_name, roi_pct, win_rate_pct, clv_avg, bankroll_start, bankroll_end, summary, created_at "
            "FROM backtest_results "
            "ORDER BY created_at DESC, id DESC "
            "LIMIT %s",
            (int(limit),),
        )
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "run_name": r[1],
                "roi_pct": float(r[2]) if r[2] is not None else None,
                "win_rate_pct": float(r[3]) if r[3] is not None else None,
                "clv_avg": float(r[4]) if r[4] is not None else None,
                "bankroll_start": float(r[5]) if r[5] is not None else None,
                "bankroll_end": float(r[6]) if r[6] is not None else None,
                "summary": r[7],
                "created_at": r[8],
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
    """Normalize name, then check alias table for a canonical mapping.
    Also tries variants to match existing players in DB.
    Falls back to fuzzy last-name matching if variants fail."""
    from utils import get_name_variants, extract_last_name
    
    # 1. Try aliases first
    canonical = normalize_player_name(name)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT canonical_name FROM player_aliases WHERE alias = %s",
                (canonical,),
            )
            r = cur.fetchone()
            if r:
                return r[0]
    except psycopg2.Error as e:
        conn.rollback()
        logger.warning(f"[NameResolve] Alias lookup DB error for '{name}': {e}")
    except Exception:
        pass

    # 2. Try variants in player_elo or player_stats to see if they exist
    variants = get_name_variants(name)
    for v in variants:
        try:
            with conn.cursor() as cur:
                # Check if this variant exists in any major table
                cur.execute(
                    "SELECT player_name FROM player_elo WHERE player_name = %s LIMIT 1",
                    (v,),
                )
                if cur.fetchone():
                    # Auto-create alias for future fast lookups
                    if v != canonical:
                        try:
                            conn.execute(
                                "INSERT INTO player_aliases (alias, canonical_name) VALUES (%s, %s) "
                                "ON CONFLICT (alias) DO NOTHING",
                                (canonical, v),
                            )
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    return v
                
                cur.execute(
                    "SELECT player_name FROM player_stats WHERE player_name = %s LIMIT 1",
                    (v,),
                )
                if cur.fetchone():
                    if v != canonical:
                        try:
                            conn.execute(
                                "INSERT INTO player_aliases (alias, canonical_name) VALUES (%s, %s) "
                                "ON CONFLICT (alias) DO NOTHING",
                                (canonical, v),
                            )
                            conn.commit()
                        except Exception:
                            conn.rollback()
                    return v
        except psycopg2.Error as e:
            conn.rollback()
            logger.warning(f"[NameResolve] Variant lookup DB error for '{name}' (variant '{v}'): {e}")
            continue
        except Exception:
            continue

    # 3. Fuzzy last-name fallback: extract last name and search with LIKE
    last_name = extract_last_name(name)
    if last_name and len(last_name) >= 3:
        try:
            # Search player_elo for names containing this last name
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT player_name FROM player_elo "
                    "WHERE player_name ILIKE %s "
                    "GROUP BY player_name "
                    "ORDER BY MAX(matches_played) DESC LIMIT 5",
                    (f"%{last_name}%",),
                )
                candidates = [r[0] for r in cur.fetchall()]
            if candidates:
                # If exactly one match, use it
                if len(candidates) == 1:
                    found = candidates[0]
                    logger.info(f"[NameResolve] Fuzzy matched '{name}' → '{found}'")
                    try:
                        conn.execute(
                            "INSERT INTO player_aliases (alias, canonical_name) VALUES (%s, %s) "
                            "ON CONFLICT (alias) DO NOTHING",
                            (canonical, found),
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                    return found
                # Multiple candidates: try to narrow by initial
                m_init = re.match(r"^([A-Z])\.", canonical)
                if m_init:
                    initial = m_init.group(1)
                    for c in candidates:
                        # Check if candidate's first letter matches the initial
                        c_first = c.strip().split()[0] if c.strip() else ""
                        if c_first and c_first[0].upper() == initial.upper():
                            logger.info(f"[NameResolve] Fuzzy+initial matched '{name}' → '{c}'")
                            try:
                                conn.execute(
                                    "INSERT INTO player_aliases (alias, canonical_name) VALUES (%s, %s) "
                                    "ON CONFLICT (alias) DO NOTHING",
                                    (canonical, c),
                                )
                                conn.commit()
                            except Exception:
                                conn.rollback()
                            return c
                # Still multiple: try first candidate (most matches played)
                if candidates:
                    found = candidates[0]
                    logger.info(f"[NameResolve] Fuzzy best-guess matched '{name}' → '{found}'")
                    try:
                        conn.execute(
                            "INSERT INTO player_aliases (alias, canonical_name) VALUES (%s, %s) "
                            "ON CONFLICT (alias) DO NOTHING",
                            (canonical, found),
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                    return found
        except psycopg2.Error as e:
            conn.rollback()
            logger.warning(f"[NameResolve] Fuzzy search DB error for '{name}': {e}")
        except Exception as e:
            logger.warning(f"[NameResolve] Fuzzy search error for '{name}': {e}")

    # 4. No match found - record as unmatched and return normalized
    record_unmatched_player(name)
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


def get_recent_player_matches_for_form(player: str, limit: int = 5) -> list:
    """
    Fetch the most recent Elo-history matches for a player.
    Returns rows as:
        [{"opponent": str, "surface": str, "actual_result": int}, ...]
    where actual_result is 1 for win and 0 for loss.
    """
    player = resolve_player_name(player)
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT winner, loser, surface "
            "FROM elo_history "
            "WHERE winner = %s OR loser = %s "
            "ORDER BY event_date DESC, id DESC "
            "LIMIT %s",
            (player, player, int(limit)),
        )
        rows = cur.fetchall()
        out = []
        for winner, loser, surface in rows:
            actual_result = 1 if winner == player else 0
            opponent = loser if actual_result == 1 else winner
            out.append(
                {
                    "opponent": opponent,
                    "surface": surface,
                    "actual_result": actual_result,
                }
            )
        return out
    except Exception:
        conn.rollback()
        raise


def get_recent_match_count(player_name: str, days: int = 3) -> int:
    """
    Return count of matches found for a player in elo_history over the
    trailing `days` window (inclusive).
    """
    player_name = resolve_player_name(player_name)
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT COUNT(*) "
            "FROM elo_history "
            "WHERE (winner = %s OR loser = %s) "
            "AND event_date >= (CURRENT_DATE - (%s * INTERVAL '1 day'))",
            (player_name, player_name, int(days)),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
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


def get_player_surface_stats(player: str, surface: str) -> dict:
    """Fetch serve/return strength stats for a player on a given surface."""
    player = resolve_player_name(player)
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT serve_strength, return_strength, matches_counted, "
            "service_points_won, total_service_points, return_points_won, total_return_points "
            "FROM player_surface_stats "
            "WHERE player_name=%s AND surface=%s",
            (player, surface),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "serve_strength": float(row[0]) if row[0] is not None else None,
            "return_strength": float(row[1]) if row[1] is not None else None,
            "matches_counted": int(row[2] or 0),
            "service_points_won": float(row[3]) if row[3] is not None else None,
            "total_service_points": float(row[4]) if row[4] is not None else None,
            "return_points_won": float(row[5]) if row[5] is not None else None,
            "total_return_points": float(row[6]) if row[6] is not None else None,
        }
    except Exception:
        conn.rollback()
        raise


def upsert_player_surface_stats(
    player: str,
    surface: str,
    service_points_won: float,
    total_service_points: float,
    return_points_won: float,
    total_return_points: float,
    matches_counted: int,
):
    """
    Upsert serve/return strength records.
    Strength metrics:
      serve_strength  = service_points_won / total_service_points
      return_strength = return_points_won / total_return_points
    """
    player = resolve_player_name(player)
    conn = get_conn()
    try:
        spw = float(service_points_won or 0.0)
        tsp = float(total_service_points or 0.0)
        rpw = float(return_points_won or 0.0)
        trp = float(total_return_points or 0.0)

        serve_strength = (spw / tsp) if tsp > 0 else None
        return_strength = (rpw / trp) if trp > 0 else None

        conn.execute(
            "INSERT INTO player_surface_stats ("
            "player_name, surface, service_points_won, total_service_points, "
            "return_points_won, total_return_points, serve_strength, return_strength, matches_counted"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (player_name, surface) DO UPDATE SET "
            "service_points_won=%s, total_service_points=%s, "
            "return_points_won=%s, total_return_points=%s, "
            "serve_strength=%s, return_strength=%s, matches_counted=%s, updated_at=NOW()",
            (
                player, surface, spw, tsp, rpw, trp, serve_strength, return_strength, matches_counted,
                spw, tsp, rpw, trp, serve_strength, return_strength, matches_counted,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def upsert_player_surface_strength(
    player: str,
    surface: str,
    serve_strength: float,
    return_strength: float,
    matches_counted: int,
):
    """
    Convenience upsert when strengths are already computed.
    Stores synthetic point totals to preserve ratio-based definitions.
    """
    denom = max(100.0, float(matches_counted or 0) * 100.0)
    spw = max(0.0, min(1.0, float(serve_strength or 0.0))) * denom
    rpw = max(0.0, min(1.0, float(return_strength or 0.0))) * denom
    upsert_player_surface_stats(
        player=player,
        surface=surface,
        service_points_won=spw,
        total_service_points=denom,
        return_points_won=rpw,
        total_return_points=denom,
        matches_counted=int(matches_counted or 0),
    )


def refresh_player_surface_stats_from_player_stats(min_matches: int = 8) -> int:
    """
    Populate player_surface_stats using existing player_stats rows.
    Uses a conservative proxy transform from surface win-rate to serve/return
    strengths when point-level stats are unavailable.
    """
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT player_name, surface, surface_win_rate, matches_counted "
            "FROM player_stats "
            "WHERE surface <> 'overall' "
            "AND matches_counted >= %s "
            "AND surface_win_rate IS NOT NULL",
            (int(min_matches),),
        )
        rows = cur.fetchall()
        updated = 0
        for player_name, surface, surface_wr, matches_counted in rows:
            wr = float(surface_wr)
            # Conservative proxy mapping with tight bounds to avoid overfitting.
            serve_strength = max(0.45, min(0.80, 0.62 + ((wr - 0.5) * 0.30)))
            return_strength = max(0.25, min(0.55, 0.38 + ((wr - 0.5) * 0.24)))
            denom = max(100.0, float(matches_counted or 0) * 100.0)
            spw = serve_strength * denom
            rpw = return_strength * denom
            conn.execute(
                "INSERT INTO player_surface_stats ("
                "player_name, surface, service_points_won, total_service_points, "
                "return_points_won, total_return_points, serve_strength, return_strength, matches_counted"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (player_name, surface) DO UPDATE SET "
                "service_points_won=%s, total_service_points=%s, "
                "return_points_won=%s, total_return_points=%s, "
                "serve_strength=%s, return_strength=%s, matches_counted=%s, updated_at=NOW()",
                (
                    player_name, surface, spw, denom, rpw, denom, serve_strength, return_strength, matches_counted,
                    spw, denom, rpw, denom, serve_strength, return_strength, matches_counted,
                ),
            )
            updated += 1
        conn.commit()
        return updated
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


def update_signal_closing_odds(signal_id: int, closing_odds: float):
    """Set closing_odds for a signal without modifying the stored result."""
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE signals SET closing_odds=%s WHERE id=%s",
            (closing_odds, signal_id),
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


def record_unmatched_player(name: str):
    """Log a player name that couldn't be matched in the DB."""
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO unmatched_players (name) VALUES (%s) "
            "ON CONFLICT (name) DO UPDATE SET first_seen = NOW()",
            (name,)
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_unmatched_players(limit: int = 10) -> list:
    """Return top N recently seen unmatched player names."""
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT name, first_seen FROM unmatched_players "
            "ORDER BY first_seen DESC LIMIT %s",
            (limit,)
        )
        return [{"name": r[0], "first_seen": r[1]} for r in cur.fetchall()]
    except Exception:
        conn.rollback()
        raise
