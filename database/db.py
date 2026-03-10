"""
database/db.py  –  PostgreSQL connection pool + all helpers

BUG FIX: Added rollback() in except blocks throughout.
Without rollback, a failed query leaves the connection in an aborted
transaction state. Returning that connection to the pool causes every
subsequent caller that gets it to immediately fail with
"InFailedSqlTransaction" — silently breaking all DB operations.
"""

import psycopg2
from psycopg2 import pool
from config import DATABASE_URL

_pool = None


def get_pool():
    global _pool
    if _pool is None:
        # ThreadedConnectionPool is safe for concurrent access from the
        # APScheduler background thread + Telegram bot main thread.
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
    return _pool


def get_conn():
    return get_pool().getconn()


def release_conn(conn):
    get_pool().putconn(conn)


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    username    TEXT,
    credits     INTEGER DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id             SERIAL PRIMARY KEY,
    user_id        INTEGER REFERENCES users(id),
    credits_added  INTEGER,
    payment_amount NUMERIC(10,2),
    payment_id     TEXT,
    package        TEXT,
    timestamp      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signals (
    id           SERIAL PRIMARY KEY,
    match_id     TEXT UNIQUE,
    tournament   TEXT,
    surface      TEXT,
    player_a     TEXT,
    player_b     TEXT,
    bet_on       TEXT,
    model_prob   NUMERIC(5,4),
    market_prob  NUMERIC(5,4),
    edge         NUMERIC(5,4),
    odds         NUMERIC(6,2),
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_deliveries (
    id           SERIAL PRIMARY KEY,
    signal_id    INTEGER REFERENCES signals(id),
    user_id      INTEGER REFERENCES users(id),
    delivered_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS player_elo (
    id             SERIAL PRIMARY KEY,
    player_name    TEXT NOT NULL,
    surface        TEXT NOT NULL,
    elo_rating     NUMERIC(8,2) DEFAULT 1500,
    matches_played INTEGER DEFAULT 0,
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(player_name, surface)
);
"""


def init_schema():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
        conn.commit()
        print("[DB] Schema ready.")
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"[DB] Schema init failed: {e}") from e
    finally:
        release_conn(conn)


# ── User helpers ──────────────────────────────────────────────────────────────

def get_or_create_user(telegram_id: int, username: str = None) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (telegram_id, username) VALUES (%s, %s) "
                "ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username "
                "RETURNING id, telegram_id, username, credits",
                (telegram_id, username)
            )
            r = cur.fetchone()
        conn.commit()
        return {"id": r[0], "telegram_id": r[1], "username": r[2], "credits": r[3]}
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def get_user(telegram_id: int) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, telegram_id, username, credits FROM users WHERE telegram_id = %s",
                (telegram_id,)
            )
            r = cur.fetchone()
        if not r:
            return None
        return {"id": r[0], "telegram_id": r[1], "username": r[2], "credits": r[3]}
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def add_credits_manual(user_id: int, credits: int) -> int:
    """
    Admin: add credits without payment. Returns the new balance.
    BUG FIX: Now returns new balance so callers don't show stale data.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET credits = credits + %s WHERE id = %s RETURNING credits",
                (credits, user_id)
            )
            new_balance = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO transactions (user_id, credits_added, payment_amount, payment_id, package) "
                "VALUES (%s, %s, 0, 'manual', 'manual')",
                (user_id, credits)
            )
        conn.commit()
        return new_balance
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def deduct_credit(user_id: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET credits = credits - 1 "
                "WHERE id = %s AND credits > 0 RETURNING credits",
                (user_id,)
            )
            r = cur.fetchone()
        conn.commit()
        return r is not None
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def get_all_subscribers() -> list:
    """All users with credits > 0."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT telegram_id FROM users WHERE credits > 0")
            return [r[0] for r in cur.fetchall()]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ── Signal helpers ────────────────────────────────────────────────────────────

def signal_exists(match_id: str) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM signals WHERE match_id = %s", (match_id,))
            return cur.fetchone() is not None
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def save_signal(match_id, tournament, surface, player_a, player_b,
                bet_on, model_prob, market_prob, edge, odds) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO signals "
                "(match_id,tournament,surface,player_a,player_b,"
                "bet_on,model_prob,market_prob,edge,odds) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (match_id, tournament, surface, player_a, player_b,
                 bet_on, model_prob, market_prob, edge, odds)
            )
            sid = cur.fetchone()[0]
        conn.commit()
        return sid
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def get_recent_signals(limit: int = 5) -> list:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id,tournament,surface,player_a,player_b,bet_on,"
                "model_prob,market_prob,edge,odds,created_at "
                "FROM signals ORDER BY created_at DESC LIMIT %s",
                (limit,)
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
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ── Elo helpers ───────────────────────────────────────────────────────────────

def get_elo(player: str, surface: str) -> float:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT elo_rating FROM player_elo "
                "WHERE player_name=%s AND surface=%s",
                (player, surface)
            )
            r = cur.fetchone()
        return float(r[0]) if r else 1500.0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


def upsert_elo(player: str, surface: str, new_rating: float):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO player_elo (player_name,surface,elo_rating,matches_played) "
                "VALUES (%s,%s,%s,1) "
                "ON CONFLICT (player_name,surface) DO UPDATE "
                "SET elo_rating=%s, "
                "matches_played=player_elo.matches_played+1, "
                "updated_at=NOW()",
                (player, surface, new_rating, new_rating)
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)
