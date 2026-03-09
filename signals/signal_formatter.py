"""
signals/signal_formatter.py
Rich Telegram signal formatter for TennisEdge.

Generates MarkdownV2-formatted messages with live stats pulled from
the SQLite database: H2H record, recent form, surface win rate, and
odds movement indicator.
"""

import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "tennis.db")

SURFACE_EMOJI = {"clay": "\U0001f7e4", "hard": "\U0001f535",
                 "grass": "\U0001f7e2", "carpet": "\u2b1c"}
SURFACE_LABEL = {"clay": "Clay", "hard": "Hard", "grass": "Grass", "carpet": "Carpet"}

MOVEMENT_DISPLAY = {
    "with":    "\U0001f4c8 Sharp money WITH us",
    "against": "\U0001f6ab Sharp money AGAINST",
    "neutral": "\u26a0\ufe0f Neutral movement",
}

MIN_H2H = 1  # show H2H even with 1 meeting (display purposes, not model)


# ── MarkdownV2 escaping ──────────────────────────────────────────────────────

_MDV2_SPECIAL = re.compile(r"([_*\[\]()~`>#+\-=|{}.!\\])")


def _esc(text) -> str:
    """Escape special characters for Telegram MarkdownV2."""
    return _MDV2_SPECIAL.sub(r"\\\1", str(text))


# ── Name conversion ──────────────────────────────────────────────────────────

def _to_db_name(bot_name: str) -> str:
    """
    Convert bot-format name ("Carlos Alcaraz") to DB-format ("Alcaraz C.").
    Handles multi-word last names by taking the last word as the surname guess,
    then falls back to a LIKE query if no exact match.
    """
    parts = bot_name.strip().split()
    if len(parts) < 2:
        return bot_name
    first = parts[0]
    last = " ".join(parts[1:])
    return f"{last} {first[0]}."


def _resolve_db_name(bot_name: str, conn: sqlite3.Connection) -> str:
    """Try exact conversion, then fall back to LIKE search in matches table."""
    candidate = _to_db_name(bot_name)
    row = conn.execute(
        "SELECT player1 FROM matches WHERE player1 = ? LIMIT 1", (candidate,)
    ).fetchone()
    if row:
        return candidate

    # Fuzzy: search by last name + first initial
    parts = bot_name.strip().split()
    if len(parts) >= 2:
        last = parts[-1]
        first_init = parts[0][0]
        pattern = f"{last}%{first_init}%"
        row = conn.execute(
            "SELECT player1 FROM matches WHERE player1 LIKE ? LIMIT 1", (pattern,)
        ).fetchone()
        if row:
            return row[0]

    return candidate  # best guess


# ── DB connection helper ──────────────────────────────────────────────────────

def _get_conn() -> sqlite3.Connection | None:
    if not os.path.exists(DB_PATH):
        return None
    return sqlite3.connect(DB_PATH, timeout=10)


# ── Stats queries ─────────────────────────────────────────────────────────────

def _h2h_record(player: str, opponent: str, surface: str,
                conn: sqlite3.Connection) -> dict:
    """H2H record on this specific surface."""
    a, b = (player, opponent) if player <= opponent else (opponent, player)
    rows = conn.execute(
        "SELECT winner FROM h2h WHERE player1 = ? AND player2 = ? AND surface = ?",
        (a, b, surface),
    ).fetchall()

    total = len(rows)
    if total < MIN_H2H:
        return {"total": 0, "wins": 0, "losses": 0}

    wins = sum(1 for (w,) in rows if w == player)
    return {"total": total, "wins": wins, "losses": total - wins}


def _recent_form_str(player: str, conn: sqlite3.Connection, n: int = 10) -> str:
    """Last N matches as 'W W L W W ...' string."""
    rows = conn.execute(
        """
        SELECT winner FROM (
            SELECT winner, date FROM matches
            WHERE player1 = ? OR player2 = ?
            ORDER BY date DESC LIMIT ?
        ) sub
        """,
        (player, player, n),
    ).fetchall()

    if not rows:
        return ""

    return " ".join("W" if w == player else "L" for (w,) in rows)


def _surface_winrate_stats(player: str, surface: str,
                           conn: sqlite3.Connection, years: int = 2) -> dict:
    """Win rate on surface over last N years. Returns total + wins."""
    cutoff = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN winner = ? THEN 1 ELSE 0 END) AS wins
        FROM matches
        WHERE (player1 = ? OR player2 = ?)
          AND surface = ?
          AND date >= ?
        """,
        (player, player, player, surface, cutoff),
    ).fetchone()

    total = row[0] if row and row[0] else 0
    wins = row[1] if row and row[1] else 0
    return {"total": total, "wins": wins,
            "pct": round(wins / total * 100) if total > 0 else 0}


# ── Main formatter ────────────────────────────────────────────────────────────

def format_rich_signal(signal: dict) -> str:
    """
    Generate a rich MarkdownV2 Telegram message from a signal dict.

    Accepts signal dicts from either the edge_detector pipeline:
        {player_a, player_b, bet_on, surface, odds, model_prob, market_prob, edge, ...}
    or the user-specified format:
        {player, opponent, surface, odds, model_prob, market_prob, edge, movement}
    """
    # Normalize keys — support both pipeline and user-specified formats
    player_a   = signal.get("player_a") or signal.get("player", "")
    player_b   = signal.get("player_b") or signal.get("opponent", "")
    bet_on     = signal.get("bet_on") or player_a
    opponent   = player_b if bet_on == player_a else player_a
    surface    = signal.get("surface", "hard")
    tournament = signal.get("tournament", "")
    odds       = signal.get("odds", 0)
    model_prob = signal.get("model_prob", 0)
    market_prob = signal.get("market_prob", 0)
    edge       = signal.get("edge", 0)
    movement   = signal.get("movement", "neutral")

    model_pct  = round(model_prob * 100, 1)
    market_pct = round(market_prob * 100, 1)
    edge_pct   = round(edge * 100, 1)

    surf_emoji = SURFACE_EMOJI.get(surface, "\U0001f3be")
    surf_label = SURFACE_LABEL.get(surface, surface.title())
    mvmt_line  = MOVEMENT_DISPLAY.get(movement, MOVEMENT_DISPLAY["neutral"])

    # ── Pull live stats from DB ──────────────────────────────────────────
    h2h = {"total": 0, "wins": 0, "losses": 0}
    form_str = ""
    sw = {"total": 0, "wins": 0, "pct": 0}

    conn = _get_conn()
    if conn:
        try:
            db_bet = _resolve_db_name(bet_on, conn)
            db_opp = _resolve_db_name(opponent, conn)

            h2h = _h2h_record(db_bet, db_opp, surface, conn)
            form_str = _recent_form_str(db_bet, conn, 10)
            sw = _surface_winrate_stats(db_bet, surface, conn, 2)
        except Exception as e:
            logger.warning("[Formatter] DB stats error: %s", e)
        finally:
            conn.close()

    # ── Build message ────────────────────────────────────────────────────
    lines = []

    # Header
    lines.append(f"*{_esc('🎾 TENNISEDGE SIGNAL')}*")
    lines.append(_esc("━━━━━━━━━━━━━━━━━━━"))

    # Tournament (if available)
    if tournament:
        lines.append(f"\n{_esc('🏆')} *{_esc('Tournament')}*")
        lines.append(_esc(tournament))

    # Match
    lines.append(f"\n{_esc('⚔️')} *{_esc('Match')}*")
    lines.append(f"{_esc(player_a)} vs {_esc(player_b)}")

    # Surface
    lines.append(f"\n{_esc(surf_emoji)} *Surface:* {_esc(surf_label)}")

    lines.append(_esc("━━━━━━━━━━━━━━━━━━━"))

    # Bet recommendation
    lines.append(f"\n{_esc('✅')} *Bet On:* {_esc(bet_on)}")
    lines.append(f"{_esc('💰')} *Odds:* {_esc(f'{odds:.2f}')}")

    # Probabilities
    lines.append(f"\n{_esc('📊')} *Model:* {_esc(f'{model_pct}%')}")
    lines.append(f"{_esc('📉')} *Market:* {_esc(f'{market_pct}%')}")
    lines.append(f"{_esc('🔥')} *EDGE: \\+{_esc(f'{edge_pct}%')}*")

    lines.append(_esc("━━━━━━━━━━━━━━━━━━━"))

    # Stats section
    lines.append(f"\n{_esc('📋')} *Stats*")

    # H2H
    if h2h["total"] > 0:
        h2h_w = h2h["wins"]
        h2h_l = h2h["losses"]
        lines.append(
            f"{_esc('🆚')} H2H on {_esc(surf_label)}: "
            f"*{_esc(f'{h2h_w}-{h2h_l}')}*"
        )
    else:
        lines.append(f"{_esc('🆚')} H2H on {_esc(surf_label)}: {_esc('No data')}")

    # Recent form
    if form_str:
        lines.append(f"{_esc('📈')} Last 10: {_esc(form_str)}")
    else:
        lines.append(f"{_esc('📈')} Last 10: {_esc('No history')}")

    # Surface win rate
    if sw["total"] > 0:
        sw_pct = sw["pct"]
        sw_total = sw["total"]
        lines.append(
            f"{_esc('🏟️')} {_esc(surf_label)} record: "
            f"*{_esc(f'{sw_pct}%')}* {_esc(f'({sw_total} matches, 2yr)')}"
        )
    else:
        lines.append(f"{_esc('🏟️')} {_esc(surf_label)} record: {_esc('No data')}")

    # Odds movement
    lines.append(f"\n{_esc(mvmt_line)}")

    lines.append(_esc("━━━━━━━━━━━━━━━━━━━"))

    # Disclaimer
    lines.append(
        f"\n_{_esc('Betting involves risk. Past performance does not guarantee future results.')}_"
    )

    return "\n".join(lines)
