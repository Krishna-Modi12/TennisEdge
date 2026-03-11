"""
RapidAPI tennis integration with Postgres-backed response cache.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import re
import time
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import quote

import psycopg2.extras
import requests

from database.db import get_conn
from config import RAPIDAPI_KEY, RAPIDAPI_HOST

_IS_TENNISAPI1 = RAPIDAPI_HOST.strip().lower() == "tennisapi1.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}" if _IS_TENNISAPI1 else f"https://{RAPIDAPI_HOST}/tennis/v2"
CACHE_TTL_HOURS = 24
PLAYER_ID_CACHE_HOURS = 24 * 7
REQ_TIMEOUT = 20
DEFAULT_TOURS = ("atp", "wta")
CACHE_NAMESPACE = f"rapid:{RAPIDAPI_HOST.strip().lower()}"

_SESSION = requests.Session()
_LOG = logging.getLogger("integrations.tennis_api")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

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


def _ns_key(key: str) -> str:
    return f"{CACHE_NAMESPACE}:{key}"


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return _NON_ALNUM_RE.sub("", str(value).strip().lower())


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


def _to_prob(value: Any) -> float | None:
    val = _to_float(value)
    if val is None or val < 0:
        return None
    if val > 1:
        val = val / 100.0
    return round(val, 4)


def _ratio(numerator: Any, denominator: Any) -> float | None:
    n = _to_float(numerator)
    d = _to_float(denominator)
    if n is None or d is None or d <= 0:
        return None
    return _to_prob(n / d)


def _cache_get(key: str):
    try:
        conn = get_conn()
        cache_key = _ns_key(key)
        cur = conn.execute(
            "SELECT value FROM api_cache WHERE key = %s AND expires_at > NOW()",
            (cache_key,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        _log_error("cache_get_failed", e)
        return None


def _cache_set(key: str, value: Any, ttl_hours: int):
    conn = None
    try:
        conn = get_conn()
        cache_key = _ns_key(key)
        expires_at = dt.datetime.utcnow() + dt.timedelta(hours=ttl_hours)
        conn.execute(
            "INSERT INTO api_cache(key, value, expires_at) VALUES (%s, %s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at",
            (cache_key, psycopg2.extras.Json(value), expires_at),
        )
        conn.commit()
    except Exception as e:
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        _log_error("cache_set_failed", e)


def _request_json(path: str, params: dict | None = None) -> tuple[Any | None, int | None]:
    if not RAPIDAPI_KEY or not RAPIDAPI_HOST:
        return None, None

    if not path.startswith("/"):
        path = f"/{path}"

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

            try:
                payload = resp.json()
            except ValueError:
                _LOG.info("api_bad_json path=%s status=%s", path, resp.status_code)
                return None, resp.status_code

            if isinstance(payload, dict) and payload.get("error") is True:
                p_status = _to_int(payload.get("statusCode")) or resp.status_code
                if p_status == 429 and attempt == 0:
                    time.sleep(2)
                    continue
                _LOG.info(
                    "api_payload_error path=%s status=%s message=%s",
                    path,
                    p_status,
                    payload.get("message"),
                )
                return None, p_status

            return payload, 200
        except Exception as e:
            _log_error(f"api_request_failed path={path}", e)
            return None, None

    return None, None


def _extract_data(payload: Any) -> Any:
    if isinstance(payload, dict):
        for key in ("data", "result", "response", "item", "player", "stats"):
            if key in payload and payload[key] is not None:
                return payload[key]
    return payload


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
    for key in keys:
        found = _find_key(data, key)
        if found is not None:
            return found
    return None


def _parse_iso_datetime(value: Any) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        try:
            return dt.datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None


def _player_meta_from_cache(name: str) -> tuple[str | None, str | None]:
    cached = _cache_get(f"pid_{name}")
    if isinstance(cached, dict):
        pid = cached.get("player_id")
        tour = cached.get("tour")
        return (str(pid), str(tour)) if pid and tour else (str(pid), None) if pid else (None, None)
    if isinstance(cached, (int, float, str)) and str(cached):
        return str(cached), None
    return None, None


def _store_player_meta(name: str, player_id: str, tour: str | None):
    if not name or not player_id:
        return
    payload = {"player_id": str(player_id)}
    if tour:
        payload["tour"] = str(tour)
    _cache_set(f"pid_{name}", payload, PLAYER_ID_CACHE_HOURS)
    if tour:
        _cache_set(f"pidmeta_{player_id}", {"tour": str(tour)}, PLAYER_ID_CACHE_HOURS)


def _guess_tour_from_pid(player_id: str) -> str | None:
    cached = _cache_get(f"pidmeta_{player_id}")
    if isinstance(cached, dict) and cached.get("tour"):
        return str(cached["tour"])

    if _IS_TENNISAPI1:
        for tour in DEFAULT_TOURS:
            for row in _t1_get_ranking_rows(tour):
                if str(row.get("id")) == str(player_id):
                    _cache_set(f"pidmeta_{player_id}", {"tour": tour}, PLAYER_ID_CACHE_HOURS)
                    return tour
        return None

    for tour in DEFAULT_TOURS:
        payload, _ = _request_json(f"/{tour}/player/profile/{player_id}")
        data = _extract_data(payload)
        if isinstance(data, dict) and str(data.get("id")) == str(player_id):
            _cache_set(f"pidmeta_{player_id}", {"tour": tour}, PLAYER_ID_CACHE_HOURS)
            return tour
    return None


def _add_player_index(index: dict[str, dict[str, str]], name: Any, player_id: Any):
    pid = _to_int(player_id)
    if pid is None:
        return
    pname = str(name or "").strip()
    if not pname or "/" in pname:
        return
    norm = _norm_text(pname)
    if not norm:
        return
    if norm not in index:
        index[norm] = {"id": str(pid), "name": pname}


def _build_player_index(tour: str) -> dict[str, dict[str, str]]:
    cache_key = f"player_index_{tour}"
    cached = _cache_get(cache_key)
    if isinstance(cached, dict) and cached:
        return cached

    index: dict[str, dict[str, str]] = {}

    ranking_payload, _ = _request_json(f"/{tour}/ranking/singles/")
    ranking_data = _extract_data(ranking_payload)
    if isinstance(ranking_data, list):
        for row in ranking_data:
            if not isinstance(row, dict):
                continue
            player = row.get("player") or {}
            if isinstance(player, dict):
                _add_player_index(index, player.get("name"), player.get("id"))

    fixtures_payload, _ = _request_json(f"/{tour}/fixtures")
    fixtures_data = _extract_data(fixtures_payload)
    if isinstance(fixtures_data, list):
        for match in fixtures_data:
            if not isinstance(match, dict):
                continue
            p1 = match.get("player1") or {}
            p2 = match.get("player2") or {}
            if isinstance(p1, dict):
                _add_player_index(index, p1.get("name"), p1.get("id") or match.get("player1Id"))
            if isinstance(p2, dict):
                _add_player_index(index, p2.get("name"), p2.get("id") or match.get("player2Id"))

    players_payload, _ = _request_json(f"/{tour}/player/")
    players_data = _extract_data(players_payload)
    if isinstance(players_data, list):
        for item in players_data:
            if not isinstance(item, dict):
                continue
            _add_player_index(index, item.get("name"), item.get("id"))

    if index:
        _cache_set(cache_key, index, CACHE_TTL_HOURS)
    return index


def _pick_player_id(name: str, index: dict[str, dict[str, str]]) -> str | None:
    if not index:
        return None
    target = _norm_text(name)
    if not target:
        return None
    if target in index:
        return index[target]["id"]

    best_id = None
    best_score = 0.0

    for norm_name, item in index.items():
        if target in norm_name or norm_name in target:
            score = 0.95
        else:
            score = SequenceMatcher(None, target, norm_name).ratio()
        if score > best_score:
            best_score = score
            best_id = item["id"]

    if best_score >= 0.92:
        return best_id
    return None


def _search_candidates(name: str) -> list[dict[str, Any]]:
    payload, _ = _request_json("/search", {"search": name})
    data = _extract_data(payload)
    if not isinstance(data, list):
        return []

    target = _norm_text(name)
    out: list[dict[str, Any]] = []
    seen = set()

    for group in data:
        if not isinstance(group, dict):
            continue
        category = str(group.get("category", "")).lower()
        if not category.startswith("player_"):
            continue
        tour = category.split("_", 1)[1]
        if tour not in DEFAULT_TOURS:
            continue
        results = group.get("result") or []
        if not isinstance(results, list):
            continue
        for item in results:
            if not isinstance(item, dict):
                continue
            pname = str(item.get("name", "")).strip()
            if not pname:
                continue
            norm_p = _norm_text(pname)
            if not norm_p:
                continue
            if norm_p == target:
                score = 1.0
            elif target in norm_p or norm_p in target:
                score = 0.95
            else:
                score = SequenceMatcher(None, target, norm_p).ratio()
            if score < 0.78:
                continue
            country = str(item.get("countryAcr") or "").strip().upper()
            key = (tour, norm_p, country)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "tour": tour,
                    "name": pname,
                    "country": country,
                    "score": score,
                }
            )

    out.sort(key=lambda x: x.get("score", 0), reverse=True)
    return out


def _search_player_catalog(
    name: str,
    tour: str,
    country: str | None = None,
    max_pages: int = 12,
) -> str | None:
    target = _norm_text(name)
    if not target:
        return None

    filter_parts = ["PlayerGroup:singles"]
    if country and len(country) == 3:
        filter_parts.append(f"PlayerCountry:{country.upper()}")
    filter_value = ";".join(filter_parts)

    best_id = None
    best_score = 0.0
    seen_page_starts = set()

    for page_no in range(1, max_pages + 1):
        params = {"pageSize": 500, "pageNo": page_no, "filter": filter_value}
        payload, _ = _request_json(f"/{tour}/player/", params=params)
        data = _extract_data(payload)
        if not isinstance(data, list) or not data:
            break

        first_name = ""
        if isinstance(data[0], dict):
            first_name = _norm_text(data[0].get("name"))
        if first_name:
            if first_name in seen_page_starts:
                break
            seen_page_starts.add(first_name)

        for item in data:
            if not isinstance(item, dict):
                continue
            player_id = _to_int(item.get("id"))
            player_name = str(item.get("name", "")).strip()
            if player_id is None or not player_name or "/" in player_name:
                continue
            norm_name = _norm_text(player_name)
            if not norm_name:
                continue
            if norm_name == target:
                return str(player_id)
            if target in norm_name or norm_name in target:
                score = 0.95
            else:
                score = SequenceMatcher(None, target, norm_name).ratio()
            if score > best_score:
                best_score = score
                best_id = str(player_id)

    if best_score >= 0.93:
        return best_id
    return None


def _candidate_tours_from_search(name: str) -> list[str]:
    tours: list[str] = []
    for item in _search_candidates(name):
        tour = item.get("tour")
        if tour and tour not in tours:
            tours.append(tour)
    return tours or list(DEFAULT_TOURS)


def _iter_dict_nodes(data: Any):
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from _iter_dict_nodes(value)
    elif isinstance(data, list):
        for item in data:
            yield from _iter_dict_nodes(item)


def _extract_player_rows(data: Any, tour_hint: str | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen = set()

    for node in _iter_dict_nodes(data):
        if not isinstance(node, dict):
            continue

        rank = _to_int(
            _find_first(
                node,
                [
                    "rank",
                    "ranking",
                    "position",
                    "pos",
                    "currentRank",
                    "current_rank",
                ],
            )
        )
        movement = _to_int(
            _find_first(
                node,
                [
                    "movement",
                    "change",
                    "rankChange",
                    "rank_change",
                    "rank_change_this_week",
                ],
            )
        )

        candidate_objs = []
        for key in ("player", "team", "athlete", "competitor"):
            obj = node.get(key)
            if isinstance(obj, dict):
                candidate_objs.append(obj)
        candidate_objs.append(node)

        for obj in candidate_objs:
            pid = _to_int(_find_first(obj, ["playerId", "id", "competitorId", "teamId"]))
            pname = _find_first(obj, ["name", "fullName", "playerName", "shortName"])
            if pid is None or not pname:
                continue

            pname_str = str(pname).strip()
            if not pname_str:
                continue

            row_key = (pid, _norm_text(pname_str))
            if row_key in seen:
                continue
            seen.add(row_key)
            rows.append(
                {
                    "id": str(pid),
                    "name": pname_str,
                    "tour": tour_hint,
                    "rank": rank,
                    "movement": movement,
                }
            )
    return rows


def _pick_best_candidate(name: str, candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    target = _norm_text(name)
    if not target:
        return None

    best = None
    best_score = 0.0
    for candidate in candidates:
        cname = _norm_text(candidate.get("name"))
        if not cname:
            continue
        if cname == target:
            score = 1.0
        elif target in cname or cname in target:
            score = 0.96
        else:
            score = SequenceMatcher(None, target, cname).ratio()

        if score > best_score:
            best_score = score
            best = candidate

    if best_score >= 0.82:
        return best
    return None


def _t1_get_ranking_rows(tour: str) -> list[dict[str, Any]]:
    cache_key = f"t1_rankings_{tour}"
    cached = _cache_get(cache_key)
    if isinstance(cached, list) and cached:
        return cached

    payload, _ = _request_json(f"/api/tennis/rankings/{tour}")
    data = _extract_data(payload)
    rows = _extract_player_rows(data, tour_hint=tour)
    if rows:
        _cache_set(cache_key, rows, CACHE_TTL_HOURS)
    return rows


def _t1_search_candidates(name: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    target = str(name or "").strip()
    if not target:
        return candidates

    for raw in (target, quote(target)):
        payload, _ = _request_json(f"/api/tennis/search/{raw}")
        data = _extract_data(payload)
        candidates.extend(_extract_player_rows(data))

    for tour in DEFAULT_TOURS:
        candidates.extend(_t1_get_ranking_rows(tour))

    for path in ("/api/tennis/events/live", "/api/tennis/events/today"):
        payload, _ = _request_json(path)
        data = _extract_data(payload)
        candidates.extend(_extract_player_rows(data))

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for item in candidates:
        key = (str(item.get("id")), _norm_text(item.get("name")))
        if key not in deduped:
            deduped[key] = item
    return list(deduped.values())


def _t1_fetch_payload(paths: list[str], params: dict | None = None) -> Any | None:
    for path in paths:
        payload, status = _request_json(path, params=params)
        if payload is not None:
            return _extract_data(payload)
        if status == 404:
            continue
    return None


def _t1_get_match_list(player_id: str) -> list[dict[str, Any]]:
    paths = [
        f"/api/tennis/player/{player_id}/events/last/0",
        f"/api/tennis/player/{player_id}/matches",
        f"/api/tennis/player/{player_id}/events",
    ]
    data = _t1_fetch_payload(paths)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        list_data = _find_first(data, ["events", "matches", "items", "data"])
        if isinstance(list_data, list):
            return [row for row in list_data if isinstance(row, dict)]
    return []


def _t1_find_h2h_surface_record(info_data: Any) -> str | None:
    if not isinstance(info_data, list):
        return None

    best = None
    best_total = -1
    for row in info_data:
        if not isinstance(row, dict):
            continue
        w1 = _to_int(_find_first(row, ["player1wins", "wins1", "homeWins"]))
        w2 = _to_int(_find_first(row, ["player2wins", "wins2", "awayWins"]))
        if w1 is None or w2 is None:
            continue
        total = w1 + w2
        if total > best_total:
            best_total = total
            best = (w1, w2, str(_find_first(row, ["court", "surface", "name"]) or "Unknown"))

    if best is None:
        return None
    return f"{best[0]}-{best[1]} on {best[2]}"


def _resolve_player(name: str) -> tuple[str | None, str | None]:
    raw_name = str(name or "").strip()
    if not raw_name:
        return None, None

    player_id, tour = _player_meta_from_cache(raw_name)
    if player_id:
        if not tour:
            tour = _guess_tour_from_pid(player_id)
            if tour:
                _store_player_meta(raw_name, player_id, tour)
        return player_id, tour

    player_id = search_player(raw_name)
    if not player_id:
        return None, None

    player_id, tour = _player_meta_from_cache(raw_name)
    if player_id and tour:
        return player_id, tour

    tour = _guess_tour_from_pid(player_id)
    if tour:
        _store_player_meta(raw_name, player_id, tour)
    return player_id, tour


def _t1_get_player_stats(player_id: str) -> dict | None:
    data = _t1_fetch_payload(
        [
            f"/api/tennis/player/{player_id}/statistics",
            f"/api/tennis/player/{player_id}/stats",
            f"/api/tennis/player/{player_id}",
            f"/api/tennis/players/{player_id}/statistics",
            f"/api/tennis/players/{player_id}",
        ]
    )
    if data is None:
        return None

    out = {
        "first_serve_pct": _to_prob(
            _find_first(
                data,
                ["firstServePct", "first_serve_pct", "firstServePercentage", "firstServeInPct"],
            )
        ),
        "first_serve_won_pct": _to_prob(
            _find_first(
                data,
                [
                    "firstServeWonPct",
                    "first_serve_won_pct",
                    "firstServePointsWonPercentage",
                    "winningOnFirstServePct",
                ],
            )
        ),
        "second_serve_won_pct": _to_prob(
            _find_first(
                data,
                [
                    "secondServeWonPct",
                    "second_serve_won_pct",
                    "secondServePointsWonPercentage",
                    "winningOnSecondServePct",
                ],
            )
        ),
        "break_points_saved_pct": _to_prob(
            _find_first(data, ["breakPointsSavedPct", "break_points_saved_pct", "breakPointSavedPct"])
        ),
        "return_points_won_pct": _to_prob(
            _find_first(
                data,
                ["returnPointsWonPct", "return_points_won_pct", "returnPointsWonPercentage"],
            )
        ),
        "aces_per_match": _to_float(_find_first(data, ["acesPerMatch", "aces_per_match", "aces"])),
    }
    if all(v is None for v in out.values()):
        return None
    return out


def _t1_get_h2h_stats(p1_id: str, p2_id: str) -> dict | None:
    stats_data = _t1_fetch_payload(
        [
            f"/api/tennis/h2h/{p1_id}/{p2_id}",
            f"/api/tennis/head-to-head/{p1_id}/{p2_id}",
            f"/api/tennis/player/{p1_id}/h2h/{p2_id}",
        ]
    )
    info_data = _t1_fetch_payload(
        [
            f"/api/tennis/h2h/info/{p1_id}/{p2_id}",
            f"/api/tennis/head-to-head/info/{p1_id}/{p2_id}",
        ]
    )
    matches_data = _t1_fetch_payload(
        [
            f"/api/tennis/h2h/matches/{p1_id}/{p2_id}",
            f"/api/tennis/head-to-head/matches/{p1_id}/{p2_id}",
        ]
    )

    p1_wins = _to_int(_find_first(stats_data, ["player1wins", "wins1", "homeWins", "matchesWon"]))
    p2_wins = _to_int(_find_first(stats_data, ["player2wins", "wins2", "awayWins", "matchesLost"]))
    if p1_wins is not None and p2_wins is not None:
        h2h_record = f"{p1_wins}-{p2_wins}"
    else:
        h2h_record = None

    surface_record = _t1_find_h2h_surface_record(info_data)

    last_5_meetings = None
    if isinstance(matches_data, list) and matches_data:
        p1_int = _to_int(p1_id)
        p2_int = _to_int(p2_id)
        outcomes: list[str] = []
        for row in matches_data[:5]:
            if not isinstance(row, dict):
                continue
            winner = _to_int(_find_first(row, ["winnerId", "match_winner", "winningPlayerId"]))
            if p1_int is not None and winner == p1_int:
                outcomes.append("W")
            elif p2_int is not None and winner == p2_int:
                outcomes.append("L")
            else:
                outcomes.append("?")
        if outcomes:
            last_5_meetings = " ".join(outcomes)

    out = {
        "h2h_record": h2h_record,
        "surface_record": surface_record,
        "last_5_meetings": last_5_meetings,
    }
    if all(v is None for v in out.values()):
        return None
    return out


def _t1_get_tournament_form(player_id: str) -> dict | None:
    matches = _t1_get_match_list(player_id)
    if not matches:
        return None

    latest_dt = None
    for row in matches:
        latest_dt = _parse_iso_datetime(
            _find_first(row, ["date", "startTimestamp", "startTime", "timestamp", "startDate"])
        )
        if latest_dt is not None:
            break

    days_since_last_match = None
    if latest_dt is not None:
        now_utc = dt.datetime.now(dt.timezone.utc)
        if latest_dt.tzinfo is None:
            latest_dt = latest_dt.replace(tzinfo=dt.timezone.utc)
        days_since_last_match = max(0, (now_utc.date() - latest_dt.date()).days)

    tournament_id = _find_first(matches[0], ["tournamentId", "tourId", "eventId"])
    player_int = _to_int(player_id)
    wins_this_tournament = 0
    for row in matches:
        if not isinstance(row, dict):
            continue
        if tournament_id is not None and _find_first(row, ["tournamentId", "tourId", "eventId"]) != tournament_id:
            continue
        winner = _to_int(_find_first(row, ["winnerId", "match_winner", "winningPlayerId"]))
        if player_int is not None and winner == player_int:
            wins_this_tournament += 1

    out = {
        "wins_this_tournament": int(wins_this_tournament),
        "days_since_last_match": days_since_last_match,
    }
    if out["days_since_last_match"] is None and out["wins_this_tournament"] == 0:
        return None
    return out


def _t1_get_player_ranking(player_name: str, player_id: str | None, tour_hint: str | None) -> dict | None:
    tours_to_try = [tour_hint] if tour_hint else []
    for t in DEFAULT_TOURS:
        if t not in tours_to_try:
            tours_to_try.append(t)

    candidates: list[dict[str, Any]] = []
    for tour in tours_to_try:
        candidates.extend(_t1_get_ranking_rows(tour))

    target_id = str(player_id) if player_id else None
    target_name = _norm_text(player_name)
    best_match = None
    best_score = -1.0

    for item in candidates:
        item_id = str(item.get("id")) if item.get("id") else None
        item_name = _norm_text(item.get("name"))
        if target_id and item_id == target_id:
            best_match = item
            break
        if not target_name or not item_name:
            continue
        if item_name == target_name:
            score = 1.0
        elif target_name in item_name or item_name in target_name:
            score = 0.96
        else:
            score = SequenceMatcher(None, target_name, item_name).ratio()
        if score > best_score:
            best_score = score
            best_match = item

    if not best_match:
        return None

    current_rank = _to_int(best_match.get("rank"))
    if current_rank is None:
        return None
    rank_change = _to_int(best_match.get("movement"))
    if rank_change is None:
        rank_change = 0
    return {
        "current_rank": current_rank,
        "rank_change_this_week": rank_change,
    }


def search_player(name) -> str | None:
    try:
        if not name:
            return None

        original_name = str(name).strip()
        if not original_name:
            return None

        cached_id, _ = _player_meta_from_cache(original_name)
        if cached_id:
            return cached_id

        lookup_name = original_name
        if lookup_name.isdigit():
            _store_player_meta(original_name, lookup_name, None)
            return lookup_name

        conn = get_conn()
        cur = conn.execute(
            "SELECT canonical_name FROM player_aliases WHERE alias ILIKE %s LIMIT 1",
            (f"%{lookup_name}%",),
        )
        row = cur.fetchone()
        if row and row[0]:
            canonical = str(row[0]).strip()
            if canonical:
                lookup_name = canonical
                if canonical.isdigit():
                    _store_player_meta(original_name, canonical, None)
                    _store_player_meta(lookup_name, canonical, None)
                    return canonical

                cached_id, _ = _player_meta_from_cache(lookup_name)
                if cached_id:
                    _store_player_meta(original_name, cached_id, None)
                    return cached_id

        if _IS_TENNISAPI1:
            candidates = _t1_search_candidates(lookup_name)
            best = _pick_best_candidate(lookup_name, candidates)
            if best:
                player_id = str(best.get("id"))
                player_tour = best.get("tour")
                _store_player_meta(original_name, player_id, player_tour)
                if lookup_name != original_name:
                    _store_player_meta(lookup_name, player_id, player_tour)
                return player_id
            return None

        search_candidates = _search_candidates(lookup_name)
        for candidate in search_candidates:
            candidate_tour = candidate.get("tour")
            candidate_name = candidate.get("name")
            candidate_country = candidate.get("country")
            if not candidate_tour or not candidate_name:
                continue
            max_pages = 20 if candidate_country else 8
            player_id = _search_player_catalog(
                str(candidate_name),
                str(candidate_tour),
                str(candidate_country) if candidate_country else None,
                max_pages=max_pages,
            )
            if player_id:
                _store_player_meta(original_name, player_id, str(candidate_tour))
                if lookup_name != original_name:
                    _store_player_meta(lookup_name, player_id, str(candidate_tour))
                return player_id

        tours = _candidate_tours_from_search(lookup_name)
        for tour in tours:
            index = _build_player_index(tour)
            player_id = _pick_player_id(lookup_name, index)
            if player_id:
                _store_player_meta(original_name, player_id, tour)
                if lookup_name != original_name:
                    _store_player_meta(lookup_name, player_id, tour)
                return player_id

            country_hint = None
            for candidate in search_candidates:
                if candidate.get("tour") == tour and candidate.get("country"):
                    country_hint = str(candidate.get("country"))
                    break
            player_id = _search_player_catalog(
                lookup_name,
                tour,
                country_hint,
                max_pages=20 if country_hint else 6,
            )
            if player_id:
                _store_player_meta(original_name, player_id, tour)
                if lookup_name != original_name:
                    _store_player_meta(lookup_name, player_id, tour)
                return player_id

        payload, _ = _request_json("/search", {"search": lookup_name})
        data = _extract_data(payload)
        player_id = _find_first(data, ["player_id", "playerId", "id"])
        if player_id is not None:
            player_id = str(player_id)
            _store_player_meta(original_name, player_id, None)
            return player_id
        return None
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

        player_id, tour = _resolve_player(str(player_name).strip())
        if not player_id:
            return None

        if _IS_TENNISAPI1:
            out = _t1_get_player_stats(player_id)
            if not out:
                return None
            _cache_set(cache_key, out, CACHE_TTL_HOURS)
            if tour:
                _store_player_meta(str(player_name).strip(), player_id, tour)
            return out

        tours_to_try = [tour] if tour else []
        for t in DEFAULT_TOURS:
            if t not in tours_to_try:
                tours_to_try.append(t)

        stats = None
        selected_tour = None
        for candidate_tour in tours_to_try:
            payload, _ = _request_json(f"/{candidate_tour}/player/match-stats/{player_id}")
            data = _extract_data(payload)
            if isinstance(data, dict) and data:
                stats = data
                selected_tour = candidate_tour
                break
        if not isinstance(stats, dict):
            return None

        service = stats.get("serviceStats") or {}
        rtn = stats.get("rtnStats") or {}
        bp_serve = stats.get("breakPointsServeStats") or {}

        first_serve_pct = _ratio(service.get("firstServeGm"), service.get("firstServeOfGm"))
        first_serve_won_pct = _ratio(
            service.get("winningOnFirstServeGm"),
            service.get("winningOnFirstServeOfGm"),
        )
        second_serve_won_pct = _ratio(
            service.get("winningOnSecondServeGm"),
            service.get("winningOnSecondServeOfGm"),
        )
        break_points_saved_pct = _ratio(
            bp_serve.get("breakPointSavedGm"),
            bp_serve.get("breakPointFacedGm"),
        )
        return_points_won_pct = _ratio(
            (_to_float(rtn.get("winningOnFirstServeGm")) or 0.0)
            + (_to_float(rtn.get("winningOnSecondServeGm")) or 0.0),
            (_to_float(rtn.get("winningOnFirstServeOfGm")) or 0.0)
            + (_to_float(rtn.get("winningOnSecondServeOfGm")) or 0.0),
        )

        aces_per_match = _to_float(_find_first(stats, ["aces_per_match", "acesPerMatch"]))
        if aces_per_match is None:
            aces_raw = _to_float(service.get("acesGm"))
            if aces_raw is not None and aces_raw <= 100:
                aces_per_match = round(aces_raw, 2)

        out = {
            "first_serve_pct": first_serve_pct,
            "first_serve_won_pct": first_serve_won_pct,
            "second_serve_won_pct": second_serve_won_pct,
            "break_points_saved_pct": break_points_saved_pct,
            "return_points_won_pct": return_points_won_pct,
            "aces_per_match": aces_per_match,
        }
        if all(value is None for value in out.values()):
            return None

        _cache_set(cache_key, out, CACHE_TTL_HOURS)
        if selected_tour:
            _store_player_meta(str(player_name).strip(), player_id, selected_tour)
        return out
    except Exception as e:
        _log_error("get_player_stats_failed", e)
        return None


def get_h2h_stats(player1, player2) -> dict | None:
    try:
        if not player1 or not player2:
            return None

        p1_name = str(player1).strip()
        p2_name = str(player2).strip()
        if not p1_name or not p2_name:
            return None

        a, b = sorted([p1_name, p2_name])
        cache_key = f"h2h_{a}_{b}"
        cached = _cache_get(cache_key)
        if isinstance(cached, dict):
            return cached

        p1_id, p1_tour = _resolve_player(p1_name)
        p2_id, p2_tour = _resolve_player(p2_name)
        if not p1_id or not p2_id:
            return None

        if _IS_TENNISAPI1:
            out = _t1_get_h2h_stats(p1_id, p2_id)
            if not out:
                return None
            _cache_set(cache_key, out, CACHE_TTL_HOURS)
            if p1_tour:
                _store_player_meta(p1_name, p1_id, p1_tour)
            if p2_tour:
                _store_player_meta(p2_name, p2_id, p2_tour)
            return out

        tours_to_try: list[str] = []
        for t in (p1_tour, p2_tour, *DEFAULT_TOURS):
            if t and t not in tours_to_try:
                tours_to_try.append(t)

        p1_int = _to_int(p1_id)
        p2_int = _to_int(p2_id)

        for tour in tours_to_try:
            stats_payload, _ = _request_json(f"/{tour}/h2h/stats/{p1_id}/{p2_id}/")
            info_payload, _ = _request_json(f"/{tour}/h2h/info/{p1_id}/{p2_id}/")
            matches_payload, _ = _request_json(f"/{tour}/h2h/matches/{p1_id}/{p2_id}/")

            stats_data = _extract_data(stats_payload)
            info_data = _extract_data(info_payload)
            matches_data = _extract_data(matches_payload)

            p1_wins = _to_int(_find_first(stats_data, ["player1Stats.matchesWon", "matchesWon"]))
            p2_wins = _to_int(_find_first(stats_data, ["player2Stats.matchesWon", "matchesWon"]))
            if isinstance(stats_data, dict):
                p1_obj = stats_data.get("player1Stats") or {}
                p2_obj = stats_data.get("player2Stats") or {}
                if p1_wins is None:
                    p1_wins = _to_int(p1_obj.get("matchesWon"))
                if p2_wins is None:
                    p2_wins = _to_int(p2_obj.get("matchesWon"))

            h2h_record = f"{p1_wins}-{p2_wins}" if p1_wins is not None and p2_wins is not None else None

            surface_record = None
            if isinstance(info_data, list) and info_data:
                best = None
                best_total = -1
                for row in info_data:
                    if not isinstance(row, dict):
                        continue
                    w1 = _to_int(row.get("player1wins"))
                    w2 = _to_int(row.get("player2wins"))
                    if w1 is None or w2 is None:
                        continue
                    total = w1 + w2
                    if total > best_total:
                        best_total = total
                        best = row
                if best is not None:
                    w1 = _to_int(best.get("player1wins")) or 0
                    w2 = _to_int(best.get("player2wins")) or 0
                    court = str(best.get("court") or "Unknown").strip()
                    surface_record = f"{w1}-{w2} on {court}"

            last_5_meetings = None
            if isinstance(matches_data, list) and matches_data:
                outcomes: list[str] = []
                for row in matches_data[:5]:
                    if not isinstance(row, dict):
                        continue
                    winner = _to_int(row.get("match_winner"))
                    if p1_int is not None and winner == p1_int:
                        outcomes.append("W")
                    elif p2_int is not None and winner == p2_int:
                        outcomes.append("L")
                    else:
                        outcomes.append("?")
                if outcomes:
                    last_5_meetings = " ".join(outcomes)

            out = {
                "h2h_record": h2h_record,
                "surface_record": surface_record,
                "last_5_meetings": last_5_meetings,
            }
            if any(v is not None for v in out.values()):
                _cache_set(cache_key, out, CACHE_TTL_HOURS)
                return out

        return None
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

        player_id, tour = _resolve_player(str(player_name).strip())
        if not player_id:
            return None

        if _IS_TENNISAPI1:
            out = _t1_get_tournament_form(player_id)
            if not out:
                return None
            _cache_set(cache_key, out, CACHE_TTL_HOURS)
            if tour:
                _store_player_meta(str(player_name).strip(), player_id, tour)
            return out

        tours_to_try = [tour] if tour else []
        for t in DEFAULT_TOURS:
            if t not in tours_to_try:
                tours_to_try.append(t)

        past_matches = None
        selected_tour = None
        for candidate_tour in tours_to_try:
            payload, _ = _request_json(f"/{candidate_tour}/player/past-matches/{player_id}")
            data = _extract_data(payload)
            if isinstance(data, list) and data:
                past_matches = data
                selected_tour = candidate_tour
                break
        if not isinstance(past_matches, list) or not past_matches:
            return None

        player_int = _to_int(player_id)

        latest_dt = None
        for row in past_matches:
            if not isinstance(row, dict):
                continue
            latest_dt = _parse_iso_datetime(row.get("date"))
            if latest_dt is not None:
                break

        days_since_last_match = None
        if latest_dt is not None:
            now_utc = dt.datetime.now(dt.timezone.utc)
            if latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=dt.timezone.utc)
            days_since_last_match = max(0, (now_utc.date() - latest_dt.date()).days)

        current_tournament_id = None
        if selected_tour:
            fixtures_payload, _ = _request_json(f"/{selected_tour}/fixtures/player/{player_id}")
            fixtures_data = _extract_data(fixtures_payload)
            if isinstance(fixtures_data, list):
                for row in fixtures_data:
                    if isinstance(row, dict) and row.get("tournamentId") is not None:
                        current_tournament_id = row.get("tournamentId")
                        break
        if current_tournament_id is None and isinstance(past_matches[0], dict):
            current_tournament_id = past_matches[0].get("tournamentId")

        wins_this_tournament = 0
        if current_tournament_id is not None:
            for row in past_matches:
                if not isinstance(row, dict):
                    continue
                if row.get("tournamentId") != current_tournament_id:
                    continue
                if player_int is not None and _to_int(row.get("match_winner")) == player_int:
                    wins_this_tournament += 1

        out = {
            "wins_this_tournament": int(wins_this_tournament),
            "days_since_last_match": days_since_last_match,
        }
        if out["days_since_last_match"] is None and out["wins_this_tournament"] == 0:
            return None

        _cache_set(cache_key, out, CACHE_TTL_HOURS)
        if selected_tour:
            _store_player_meta(str(player_name).strip(), player_id, selected_tour)
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

        player_id, tour = _resolve_player(str(player_name).strip())
        if not player_id:
            return None

        if _IS_TENNISAPI1:
            out = _t1_get_player_ranking(str(player_name).strip(), player_id, tour)
            if not out:
                return None
            _cache_set(cache_key, out, CACHE_TTL_HOURS)
            if tour:
                _store_player_meta(str(player_name).strip(), player_id, tour)
            return out

        tours_to_try = [tour] if tour else []
        for t in DEFAULT_TOURS:
            if t not in tours_to_try:
                tours_to_try.append(t)

        for candidate_tour in tours_to_try:
            payload, _ = _request_json(f"/{candidate_tour}/ranking/singles/")
            data = _extract_data(payload)
            if not isinstance(data, list):
                continue

            for row in data:
                if not isinstance(row, dict):
                    continue
                player = row.get("player") or {}
                if not isinstance(player, dict):
                    continue
                if str(player.get("id")) != str(player_id):
                    continue

                current_rank = _to_int(row.get("position"))
                rank_change = _to_int(
                    _find_first(
                        row,
                        [
                            "rank_change_this_week",
                            "rankChangeThisWeek",
                            "rank_change",
                            "rankChange",
                            "movement",
                            "change",
                        ],
                    )
                )
                if rank_change is None:
                    rank_change = 0

                out = {
                    "current_rank": current_rank,
                    "rank_change_this_week": rank_change,
                }
                if out["current_rank"] is None:
                    return None

                _cache_set(cache_key, out, CACHE_TTL_HOURS)
                _store_player_meta(str(player_name).strip(), player_id, candidate_tour)
                return out

        for candidate_tour in tours_to_try:
            payload, _ = _request_json(
                f"/{candidate_tour}/player/profile/{player_id}",
                params={"include": "ranking"},
            )
            data = _extract_data(payload)
            if not isinstance(data, dict):
                continue

            cur_rank = data.get("curRank") or data.get("currentRank") or {}
            if isinstance(cur_rank, dict):
                current_rank = _to_int(cur_rank.get("position"))
            else:
                current_rank = _to_int(cur_rank)
            if current_rank is None:
                continue

            out = {
                "current_rank": current_rank,
                "rank_change_this_week": 0,
            }
            _cache_set(cache_key, out, CACHE_TTL_HOURS)
            _store_player_meta(str(player_name).strip(), player_id, candidate_tour)
            return out

        return None
    except Exception as e:
        _log_error("get_player_ranking_failed", e)
        return None
