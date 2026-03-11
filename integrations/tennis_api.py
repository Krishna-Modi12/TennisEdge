"""
integrations/tennis_api.py
RapidAPI tennis integration with Postgres-backed response cache.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import time
from typing import Any

import psycopg2.extras
import requests

from database.db import get_conn

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "c413ab2d11msha135da281753633p19881ejsn83d7601a4708")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "tennis-api-atp-wta-itf.p.rapidapi.com")
BASE_URL = f"https://{RAPIDAPI_HOST}"
CACHE_TTL_HOURS = 24
PLAYER_ID_CACHE_HOURS = 24 * 7
REQ_TIMEOUT = 20

_SESSION = requests.Session()
_LOG = logging.getLogger("integrations.tennis_api")
if not _LOG.handlers:
    os.makedirs("logs", exist_ok=True)
    _fh = logging.FileHandler("logs/tennis_api.log", encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _LOG.addHandler(_fh)
    _LOG.setLevel(logging.INFO)
    _LOG.propagate = False


def _log_error(msg: str, exc: Exception | None = None):
    if exc is None:
        _LOG.error(msg)
    else:
        _LOG.exception("%s: %s", msg, exc)


def _cache_get(key: str):
    try:
        conn = get_conn()
        cur = conn.execute(
            "SELECT value FROM api_cache WHERE key = %s AND expires_at > NOW()",
            (key,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        _log_error("cache_get_failed", e)
        return None


def _cache_set(key: str, value: Any, ttl_hours: int):
    try:
        conn = get_conn()
        expires_at = dt.datetime.utcnow() + dt.timedelta(hours=ttl_hours)
        conn.execute(
            "INSERT INTO api_cache(key, value, expires_at) VALUES (%s, %s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at",
            (key, psycopg2.extras.Json(value), expires_at),
        )
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        _log_error("cache_set_failed", e)


def _request_json(path: str, params: dict | None = None) -> tuple[Any | None, int | None]:
    if not RAPIDAPI_KEY or not RAPIDAPI_HOST:
        return None, None

    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }
    url = f"{BASE_URL}{path}"

    for attempt in range(2):
        try:
            resp = _SESSION.get(url, headers=headers, params=params, timeout=REQ_TIMEOUT)
            if resp.status_code == 429 and attempt == 0:
                time.sleep(2)
                continue
            if resp.status_code != 200:
                _LOG.info("api_non_200 path=%s status=%s", path, resp.status_code)
                return None, resp.status_code
            return resp.json(), 200
        except Exception as e:
            _log_error(f"api_request_failed path={path}", e)
            return None, None

    return None, None


def _request_candidates(candidates: list[tuple[str, dict | None]]) -> Any | None:
    for path, params in candidates:
        payload, status = _request_json(path, params=params)
        if payload is not None:
            return payload
        if status == 404:
            continue
    return None


def _find_key(data: Any, key: str) -> Any | None:
    key_l = key.lower()
    if isinstance(data, dict):
        for k, v in data.items():
            if str(k).lower() == key_l:
                return v
        for v in data.values():
            found = _find_key(v, key)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _find_key(item, key)
            if found is not None:
                return found
    return None


def _find_first(data: Any, keys: list[str]) -> Any | None:
    for k in keys:
        found = _find_key(data, k)
        if found is not None:
            return found
    return None


def _to_prob(value: Any) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f < 0:
        return None
    if f > 1:
        f = f / 100.0
    return round(f, 4)


def _to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_data(payload: Any) -> Any:
    if isinstance(payload, dict):
        for k in ("data", "result", "response", "item", "player", "stats"):
            if k in payload and payload[k] is not None:
                return payload[k]
    return payload


def search_player(name) -> str | None:
    try:
        if not name:
            return None
        norm_name = str(name).strip()
        cache_key = f"pid_{norm_name}"
        cached = _cache_get(cache_key)
        if isinstance(cached, str) and cached:
            return cached

        # Step 1: alias lookup in DB
        conn = get_conn()
        cur = conn.execute(
            "SELECT canonical_name FROM player_aliases WHERE alias ILIKE %s LIMIT 1",
            (f"%{norm_name}%",),
        )
        row = cur.fetchone()
        if row and row[0]:
            pid = str(row[0])
            _cache_set(cache_key, pid, PLAYER_ID_CACHE_HOURS)
            return pid

        # Step 2: API fallback
        candidates = [
            ("/tennis/player-search", {"name": norm_name}),
            ("/tennis/players-search", {"name": norm_name}),
            ("/tennis/search-player", {"name": norm_name}),
            ("/tennis/players", {"search": norm_name}),
        ]
        payload = _request_candidates(candidates)
        if payload is None:
            return None
        data = _extract_data(payload)

        player_id = None
        if isinstance(data, list) and data:
            player_id = _find_first(data[0], ["player_id", "playerId", "id", "key"])
        elif isinstance(data, dict):
            players = _find_first(data, ["players", "results", "items"])
            if isinstance(players, list) and players:
                player_id = _find_first(players[0], ["player_id", "playerId", "id", "key"])
            if player_id is None:
                player_id = _find_first(data, ["player_id", "playerId", "id", "key"])

        if player_id is None:
            return None
        player_id = str(player_id)
        _cache_set(cache_key, player_id, PLAYER_ID_CACHE_HOURS)
        return player_id
    except Exception as e:
        _log_error("search_player_failed", e)
        return None


def get_player_stats(player_name, surface=None) -> dict | None:
    try:
        if not player_name:
            return None
        cache_key = f"stats_{str(player_name).strip()}_{surface}"
        cached = _cache_get(cache_key)
        if isinstance(cached, dict):
            return cached

        player_id = search_player(player_name)
        if not player_id:
            return None

        params_variants = [
            {"playerId": player_id},
            {"player_id": player_id},
            {"id": player_id},
        ]
        if surface:
            params_variants.extend(
                [
                    {"playerId": player_id, "surface": surface},
                    {"player_id": player_id, "surface": surface},
                ]
            )

        candidates: list[tuple[str, dict | None]] = []
        for params in params_variants:
            candidates.extend(
                [
                    ("/tennis/player-stats", params),
                    ("/tennis/player-statistics", params),
                    ("/tennis/player/stats", params),
                ]
            )

        payload = _request_candidates(candidates)
        if payload is None:
            return None
        data = _extract_data(payload)

        stats = {
            "first_serve_pct": _to_prob(_find_first(data, ["first_serve_pct", "firstServePct", "first_serve_in_pct", "firstServeInPct"])),
            "first_serve_won_pct": _to_prob(_find_first(data, ["first_serve_won_pct", "firstServeWonPct", "first_serve_points_won_pct"])),
            "second_serve_won_pct": _to_prob(_find_first(data, ["second_serve_won_pct", "secondServeWonPct", "second_serve_points_won_pct"])),
            "break_points_saved_pct": _to_prob(_find_first(data, ["break_points_saved_pct", "breakPointsSavedPct"])),
            "return_points_won_pct": _to_prob(_find_first(data, ["return_points_won_pct", "returnPointsWonPct"])),
            "aces_per_match": _to_float(_find_first(data, ["aces_per_match", "acesPerMatch", "aces"])),
        }
        if all(v is None for v in stats.values()):
            return None

        _cache_set(cache_key, stats, CACHE_TTL_HOURS)
        return stats
    except Exception as e:
        _log_error("get_player_stats_failed", e)
        return None


def get_h2h_stats(player1, player2) -> dict | None:
    try:
        if not player1 or not player2:
            return None
        p1 = str(player1).strip()
        p2 = str(player2).strip()
        a, b = sorted([p1, p2])
        cache_key = f"h2h_{a}_{b}"
        cached = _cache_get(cache_key)
        if isinstance(cached, dict):
            return cached

        id1 = search_player(p1)
        id2 = search_player(p2)
        if not id1 or not id2:
            return None

        candidates = [
            ("/tennis/head-to-head", {"player1Id": id1, "player2Id": id2}),
            ("/tennis/h2h", {"player1Id": id1, "player2Id": id2}),
            ("/tennis/h2h-stats", {"player1Id": id1, "player2Id": id2}),
            ("/tennis/head2head", {"player1": id1, "player2": id2}),
        ]
        payload = _request_candidates(candidates)
        if payload is None:
            return None
        data = _extract_data(payload)

        wins1 = _to_int(_find_first(data, ["player1_wins", "wins_player1", "wins1", "winsA"]))
        wins2 = _to_int(_find_first(data, ["player2_wins", "wins_player2", "wins2", "winsB"]))
        h2h_record = _find_first(data, ["h2h_record", "record", "overall_record"])
        if h2h_record is None and wins1 is not None and wins2 is not None:
            h2h_record = f"{wins1}-{wins2}"

        surface_record = _find_first(data, ["surface_record", "record_on_surface"])
        if surface_record is not None:
            surface_record = str(surface_record)

        last5 = _find_first(data, ["last_5_meetings", "last5", "recent_meetings"])
        if isinstance(last5, list):
            last5 = " ".join(str(x) for x in last5[:5])
        elif last5 is not None:
            last5 = str(last5)

        out = {
            "h2h_record": str(h2h_record) if h2h_record is not None else None,
            "surface_record": surface_record,
            "last_5_meetings": last5,
        }
        if all(v is None for v in out.values()):
            return None

        _cache_set(cache_key, out, CACHE_TTL_HOURS)
        return out
    except Exception as e:
        _log_error("get_h2h_stats_failed", e)
        return None


def get_current_tournament_form(player_name) -> dict | None:
    try:
        if not player_name:
            return None
        cache_key = f"form_{str(player_name).strip()}"
        cached = _cache_get(cache_key)
        if isinstance(cached, dict):
            return cached

        player_id = search_player(player_name)
        if not player_id:
            return None

        candidates = [
            ("/tennis/tournament-form", {"playerId": player_id}),
            ("/tennis/current-tournament-form", {"playerId": player_id}),
            ("/tennis/player-form", {"playerId": player_id}),
        ]
        payload = _request_candidates(candidates)
        if payload is None:
            return None
        data = _extract_data(payload)

        wins_this_tournament = _to_int(
            _find_first(data, ["wins_this_tournament", "winsThisTournament", "tournament_wins"])
        )
        days_since_last_match = _to_int(
            _find_first(data, ["days_since_last_match", "daysSinceLastMatch"])
        )

        out = {
            "wins_this_tournament": wins_this_tournament,
            "days_since_last_match": days_since_last_match,
        }
        if all(v is None for v in out.values()):
            return None

        _cache_set(cache_key, out, CACHE_TTL_HOURS)
        return out
    except Exception as e:
        _log_error("get_current_tournament_form_failed", e)
        return None


def get_player_ranking(player_name) -> dict | None:
    try:
        if not player_name:
            return None
        cache_key = f"rank_{str(player_name).strip()}"
        cached = _cache_get(cache_key)
        if isinstance(cached, dict):
            return cached

        player_id = search_player(player_name)
        if not player_id:
            return None

        candidates = [
            ("/tennis/player-ranking", {"playerId": player_id}),
            ("/tennis/rankings", {"playerId": player_id}),
            ("/tennis/rankings/player", {"playerId": player_id}),
        ]
        payload = _request_candidates(candidates)
        if payload is None:
            return None
        data = _extract_data(payload)

        out = {
            "current_rank": _to_int(_find_first(data, ["current_rank", "currentRank", "rank"])),
            "rank_change_this_week": _to_int(
                _find_first(data, ["rank_change_this_week", "rankChangeThisWeek", "rank_change", "rankChange"])
            ),
        }
        if out["current_rank"] is None and out["rank_change_this_week"] is None:
            return None
        if out["rank_change_this_week"] is None:
            out["rank_change_this_week"] = 0

        _cache_set(cache_key, out, CACHE_TTL_HOURS)
        return out
    except Exception as e:
        _log_error("get_player_ranking_failed", e)
        return None
