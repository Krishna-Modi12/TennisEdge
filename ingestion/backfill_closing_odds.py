"""
ingestion/backfill_closing_odds.py
Backfill missing closing_odds for already-resolved signals.

It joins resolved signals (win/loss/push) to AllSports Fixtures/Odds by match_id
and writes closing_odds without changing the stored result.
"""

from __future__ import annotations

import argparse
import datetime as dt

import requests

from config import ODDS_API_KEY
from database.db import get_conn, update_signal_closing_odds
from ingestion.resolve_matches import ALLSPORTS_BASE, _extract_closing_odds


def _parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _fetch_payload(met: str, date_from: str, date_to: str):
    resp = requests.get(
        ALLSPORTS_BASE,
        params={
            "met": met,
            "APIkey": ODDS_API_KEY,
            "from": date_from,
            "to": date_to,
        },
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("success") != 1:
        raise RuntimeError(f"{met} API unsuccessful response: {payload}")
    return payload.get("result", {})


def _load_missing(from_date: dt.date | None, to_date: dt.date | None) -> list[dict]:
    sql = (
        "SELECT id, match_id, bet_on, created_at, result "
        "FROM signals "
        "WHERE result IN ('win', 'loss', 'push') "
        "AND closing_odds IS NULL "
    )
    params: list = []
    if from_date is not None:
        sql += "AND created_at::date >= %s "
        params.append(from_date.isoformat())
    if to_date is not None:
        sql += "AND created_at::date <= %s "
        params.append(to_date.isoformat())
    sql += "ORDER BY created_at ASC"

    conn = get_conn()
    cur = conn.execute(sql, tuple(params) if params else None)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _date_only(v) -> dt.date:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    return _parse_date(str(v)[:10])


def _parse_args():
    p = argparse.ArgumentParser(description="Backfill closing_odds for resolved signals.")
    p.add_argument("--from-date", help="Filter signals created on/after YYYY-MM-DD")
    p.add_argument("--to-date", help="Filter signals created on/before YYYY-MM-DD")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview updates without writing closing_odds",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if not ODDS_API_KEY:
        print("[backfill_closing_odds] ODDS_API_KEY is required.")
        return 1

    from_date = _parse_date(args.from_date) if args.from_date else None
    to_date = _parse_date(args.to_date) if args.to_date else None
    missing = _load_missing(from_date=from_date, to_date=to_date)
    if not missing:
        print("[backfill_closing_odds] No resolved signals with missing closing_odds.")
        return 0

    min_created = min(_date_only(r["created_at"]) for r in missing)
    max_created = max(_date_only(r["created_at"]) for r in missing)
    today = dt.date.today()
    fetch_from = min_created.isoformat()
    fetch_to = max(max_created, today).isoformat()

    fixtures = _fetch_payload("Fixtures", fetch_from, fetch_to) or []
    odds_result = _fetch_payload("Odds", fetch_from, fetch_to) or {}

    fixture_map = {str(f.get("event_key")): f for f in fixtures if f.get("event_key") is not None}
    odds_map = {str(k): v for k, v in odds_result.items()} if isinstance(odds_result, dict) else {}

    updated = 0
    missing_fixture = 0
    missing_price = 0

    for row in missing:
        match_id = str(row.get("match_id", ""))
        fixture = fixture_map.get(match_id)
        if not fixture:
            missing_fixture += 1
            continue

        closing = _extract_closing_odds(odds_map.get(match_id), fixture, row.get("bet_on"))
        if closing is None:
            missing_price += 1
            continue

        if args.dry_run:
            print(f"[DRY-RUN] signal_id={row['id']} match_id={match_id} closing_odds={closing}")
        else:
            update_signal_closing_odds(int(row["id"]), float(closing))
        updated += 1

    print("[backfill_closing_odds] Summary")
    print(f"  candidates      : {len(missing)}")
    print(f"  updated         : {updated}")
    print(f"  missing_fixture : {missing_fixture}")
    print(f"  missing_price   : {missing_price}")
    print(f"  range_fetched   : {fetch_from} -> {fetch_to}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
