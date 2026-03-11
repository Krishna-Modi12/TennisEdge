"""
scheduler/update_elo_job.py
Daily Elo updater using completed match results from AllSportsAPI.

Point-in-time guard:
- Only past dates are allowed (strictly before UTC today).
- Today's or future matches are rejected.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys

import requests

if __package__ in (None, ""):
    # Allow direct script execution:
    #   python scheduler/update_elo_job.py --dry-run
    _repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)

from config import ODDS_API_KEY
from database.db import elo_history_event_exists, record_elo_history_event
from ingestion.fetch_odds import _surface_from_tournament
from models.elo_model import update_elo_after_match

logger = logging.getLogger(__name__)

ALLSPORTS_BASE = "https://apiv2.allsportsapi.com/tennis/"


def _utc_today() -> dt.date:
    return dt.datetime.utcnow().date()


def _parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def _validate_target_date(target_date: dt.date):
    today = _utc_today()
    if target_date >= today:
        raise ValueError(
            f"Point-in-time violation: target date {target_date} must be before UTC today ({today})."
        )


def _fetch_fixtures_for_date(target_date: dt.date) -> list[dict]:
    if not ODDS_API_KEY:
        raise ValueError("ODDS_API_KEY is required for daily Elo updates.")

    day = target_date.strftime("%Y-%m-%d")
    resp = requests.get(
        ALLSPORTS_BASE,
        params={
            "met": "Fixtures",
            "APIkey": ODDS_API_KEY,
            "from": day,
            "to": day,
        },
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("success") != 1:
        raise RuntimeError(f"AllSports Fixtures API unsuccessful response: {payload}")
    return payload.get("result", [])


def _to_finished_singles(fixtures: list[dict]) -> list[dict]:
    rows = []
    for f in fixtures:
        if "Doubles" in str(f.get("country_name", "")):
            continue
        if f.get("event_status") != "Finished":
            continue

        winner_code = f.get("event_winner", "")
        first = (f.get("event_first_player") or "").strip()
        second = (f.get("event_second_player") or "").strip()
        if not first or not second:
            continue

        if winner_code == "First Player":
            winner, loser = first, second
        elif winner_code == "Second Player":
            winner, loser = second, first
        else:
            continue

        event_key = str(f.get("event_key", "")).strip()
        if not event_key:
            continue

        tournament = str(f.get("league_name", "Unknown"))
        surface = _surface_from_tournament(tournament)
        event_date = f.get("event_date")

        rows.append(
            {
                "event_key": event_key,
                "event_date": event_date,
                "winner": winner,
                "loser": loser,
                "surface": surface,
                "tournament": tournament,
            }
        )
    return rows


def run_daily_elo_update(
    target_date: dt.date | None = None,
    dry_run: bool = False,
) -> dict:
    if target_date is None:
        target_date = _utc_today() - dt.timedelta(days=1)

    _validate_target_date(target_date)

    if not ODDS_API_KEY and dry_run:
        summary = {
            "ok": True,
            "date": target_date.strftime("%Y-%m-%d"),
            "fixtures_fetched": 0,
            "finished_singles": 0,
            "updated": 0,
            "duplicates": 0,
            "errors": 0,
            "dry_run": True,
            "skipped_reason": "ODDS_API_KEY not set",
        }
        print(f"[EloDaily] {summary}")
        return summary

    fixtures = _fetch_fixtures_for_date(target_date)
    finished = _to_finished_singles(fixtures)

    updated = 0
    duplicates = 0
    errors = 0

    for m in finished:
        if elo_history_event_exists(m["event_key"]):
            duplicates += 1
            continue

        if dry_run:
            updated += 1
            continue

        try:
            update_elo_after_match(m["winner"], m["loser"], m["surface"])
            event_date = m["event_date"] or target_date.strftime("%Y-%m-%d")
            record_elo_history_event(
                event_key=m["event_key"],
                event_date=event_date,
                winner=m["winner"],
                loser=m["loser"],
                surface=m["surface"],
                tournament=m["tournament"],
            )
            updated += 1
        except Exception as e:
            errors += 1
            logger.error("[EloDaily] Failed %s: %s", m["event_key"], e)

    summary = {
        "ok": errors == 0,
        "date": target_date.strftime("%Y-%m-%d"),
        "fixtures_fetched": len(fixtures),
        "finished_singles": len(finished),
        "updated": updated,
        "duplicates": duplicates,
        "errors": errors,
        "dry_run": dry_run,
    }
    print(f"[EloDaily] {summary}")
    return summary


def _parse_args():
    p = argparse.ArgumentParser(description="Run daily Elo update from finished match results.")
    p.add_argument(
        "--date",
        help="Target match date in YYYY-MM-DD (must be before UTC today). Default: yesterday UTC.",
    )
    p.add_argument("--dry-run", action="store_true", help="Do not write Elo or history updates.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    target = _parse_date(args.date) if args.date else None
    result = run_daily_elo_update(target_date=target, dry_run=args.dry_run)
    raise SystemExit(0 if result.get("ok", False) else 1)
