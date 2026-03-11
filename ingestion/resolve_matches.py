import requests
import datetime
import argparse
import sys
from config import ODDS_API_KEY
from database.db import get_pending_signals, update_signal_result, get_signal_accuracy
import logging

logger = logging.getLogger(__name__)

ALLSPORTS_BASE = "https://apiv2.allsportsapi.com/tennis/"

def parse_args():
    parser = argparse.ArgumentParser(
        description="Resolve pending tennis signals as WON, LOST, or VOID."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show which signals would be resolved without writing to database"
    )
    parser.add_argument(
        "--force", metavar="SIGNAL_ID",
        help="Manually resolve a specific signal ID"
    )
    parser.add_argument(
        "--outcome", choices=["WON", "LOST", "VOID"],
        help="Outcome to set when using --force (required with --force)"
    )
    return parser.parse_args()


def _abort(message: str, cli_mode: bool):
    if cli_mode:
        print(f"ERROR: {message}")
        sys.exit(1)
    logger.error(message)


def resolve_pending_signals(
    dry_run: bool = False,
    force_signal_id: str | int | None = None,
    forced_outcome: str | None = None,
    cli_mode: bool = False,
):
    """Fetch pending signals and resolve them if their match is finished.

    In scheduler/runtime usage this function must not exit the process.
    In CLI usage (`cli_mode=True`) validation failures exit with code 1.
    """
    forced_outcome = forced_outcome.upper() if isinstance(forced_outcome, str) else None

    if force_signal_id and not forced_outcome:
        _abort("--force requires --outcome (WON, LOST, or VOID)", cli_mode)
        return {"ok": False}
    if forced_outcome and not force_signal_id:
        _abort("--outcome requires --force SIGNAL_ID", cli_mode)
        return {"ok": False}

    if force_signal_id:
        try:
            sig_id = int(force_signal_id)
        except (TypeError, ValueError):
            _abort("--force SIGNAL_ID must be an integer", cli_mode)
            return {"ok": False}

        if forced_outcome not in {"WON", "LOST", "VOID"}:
            _abort("--outcome must be one of WON, LOST, VOID", cli_mode)
            return {"ok": False}

        # manual override
        print(f"Forcing resolution for SIGNAL_ID {sig_id} to {forced_outcome}...")
        outcome_map = {"WON": "win", "LOST": "loss", "VOID": "push"}
        if not dry_run:
            update_signal_result(sig_id, outcome_map[forced_outcome], "Manual Override")
            print(f"Successfully resolved SIGNAL_ID {sig_id}")
        else:
            print(f"[DRY-RUN] Would have resolved SIGNAL_ID {sig_id} as {forced_outcome}")
        return {"ok": True, "resolved": 1, "won": 0, "lost": 0, "voided": 0, "pending": 0}

    if not ODDS_API_KEY:
        msg = "ODDS_API_KEY not set."
        if cli_mode:
            _abort(msg, cli_mode=True)
        else:
            logger.warning(f"{msg} Skipping pending signal resolution.")
        return {"ok": False}
        
    pending = get_pending_signals(max_age_days=7)
    if not pending:
        print("No pending signals to resolve.")
        return {"ok": True, "resolved": 0, "won": 0, "lost": 0, "voided": 0, "pending": 0}

    print(f"Checking results for {len(pending)} pending signals...")
    
    dates = []
    for p in pending:
        try:
            # created_at might be string or datetime depending on db driver
            if isinstance(p['created_at'], str):
                dt = datetime.datetime.fromisoformat(p['created_at']).strftime("%Y-%m-%d")
            else:
                dt = p['created_at'].strftime("%Y-%m-%d")
            if dt not in dates:
                dates.append(dt)
        except Exception:
            pass
            
    if not dates:
        dates = [datetime.date.today().strftime("%Y-%m-%d")]
        
    min_date = min(dates)
    max_date = datetime.date.today().strftime("%Y-%m-%d")

    try:
        resp = requests.get(ALLSPORTS_BASE, params={
            "met": "Fixtures",
            "APIkey": ODDS_API_KEY,
            "from": min_date,
            "to": max_date,
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") == 1:
            fixtures = data.get("result", [])
        else:
            print("API did not return success for Fixtures.")
            return
    except Exception as e:
        print(f"Error fetching fixtures for resolution: {e}")
        return

    fixture_map = {str(f["event_key"]): f for f in fixtures}
    won, lost, voided = 0, 0, 0
    resolved_ids = set()

    for sig in pending:
        match_id = str(sig["match_id"])
        if match_id in fixture_map:
            f = fixture_map[match_id]
            status = f.get("event_status", "")
            if status == "Finished":
                winner_code = f.get("event_winner", "")
                actual_winner = None
                if winner_code == "First Player":
                    actual_winner = f.get("event_first_player")
                elif winner_code == "Second Player":
                    actual_winner = f.get("event_second_player")
                
                if actual_winner:
                    from utils import normalize_player_name
                    bet_on = sig["bet_on"]
                    norm_winner = normalize_player_name(actual_winner)
                    norm_bet = normalize_player_name(bet_on)
                    
                    if norm_winner == norm_bet:
                        result = "win"
                        won += 1
                    else:
                        result = "loss"
                        lost += 1
                        
                    resolved_ids.add(sig["id"])
                    if not dry_run:
                        update_signal_result(sig["id"], result, actual_winner)
                    else:
                        print(f"[DRY-RUN] SIGNAL_ID {sig['id']}: Match {match_id} Finished -> {result.upper()} (Winner: {actual_winner})")
                        
            elif status in ("Cancelled", "Retired"):
                resolved_ids.add(sig["id"])
                if not dry_run:
                    update_signal_result(sig["id"], "push", None)
                else:
                    print(f"[DRY-RUN] SIGNAL_ID {sig['id']}: Match {match_id} {status} -> VOID")
                voided += 1

    total_resolved = won + lost + voided
    still_pending = len(pending) - total_resolved
    
    print(f"\nResolved this run : {total_resolved} signals")
    print(f"  WON             : {won}")
    print(f"  LOST            : {lost}")
    print(f"  VOID            : {voided}")
    print(f"Still pending     : {still_pending} signals\n")

    # Audit for 72+ hour stale signals
    now = datetime.datetime.utcnow()
    for sig in pending:
        if sig["id"] in resolved_ids:
            continue
        try:
            if isinstance(sig['created_at'], str):
                cdt = datetime.datetime.fromisoformat(sig['created_at'])
            else:
                cdt = sig['created_at']
                
            age_hours = (now - cdt).total_seconds() / 3600.0
            if age_hours > 72:
                print(f"⚠️  Signal {sig['id']} has been pending 72+ hours.")
                print(f"    Match may have been postponed or cancelled.")
                print(f"    Run: python resolve_matches.py --force {sig['id']} --outcome VOID")
        except Exception:
            pass

    return {
        "ok": True,
        "resolved": total_resolved,
        "won": won,
        "lost": lost,
        "voided": voided,
        "pending": still_pending,
    }

if __name__ == "__main__":
    args = parse_args()
    resolve_pending_signals(
        dry_run=args.dry_run,
        force_signal_id=args.force,
        forced_outcome=args.outcome,
        cli_mode=True,
    )
