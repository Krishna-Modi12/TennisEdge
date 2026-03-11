# audit_deliveries.py
"""
Audits signal_deliveries table for delivery integrity issues.

Checks performed:
  1. Duplicate deliveries — same user + same signal_id delivered more than once
  2. Stale unresolved signals — signals pending resolution for 48+ hours
  3. Overall delivery summary — totals by outcome

Note: This script does NOT check credit ledger consistency because
the credits table schema is not available here. Add that check manually
if you add a credits audit function later.

Run from: project root
Run after every deployment to verify the delivery pipeline is intact.
"""
import sqlite3
import os
import sys
from datetime import datetime, timedelta

DB_PATH = os.getenv("DB_PATH", "database/signals.db")


def run_audit() -> int:
    """Run all delivery audit checks. Returns number of issues found."""

    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at: {DB_PATH}")
        print("   Set DB_PATH env var or confirm the path is correct.")
        return 1

    conn = None
    issues_found = 0

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print("\n" + "=" * 55)
        print("  SIGNAL DELIVERY AUDIT")
        print(f"  {datetime.utcnow().isoformat()} UTC")
        print("=" * 55)

        # ── Check 1: Duplicate deliveries ──────────────────────────
        print("\n[1] Checking for duplicate deliveries...")
        cursor.execute("""
            SELECT user_id, signal_id, COUNT(*) as cnt
            FROM signal_deliveries
            GROUP BY user_id, signal_id
            HAVING cnt > 1
        """)
        dupes = cursor.fetchall()
        if dupes:
            # Count total excess deliveries (not just groups)
            excess_total = sum(d['cnt'] - 1 for d in dupes)
            print(f"  ❌ Found {len(dupes)} duplicate group(s) "
                  f"({excess_total} excess deliveries):")
            for d in dupes:
                print(f"     user_id={d['user_id']}  "
                      f"signal_id={d['signal_id']}  "
                      f"delivered={d['cnt']}x")
            issues_found += excess_total
        else:
            print("  ✅ No duplicate deliveries")

        # ── Check 2: Stale unresolved signals ──────────────────────
        print("\n[2] Checking for stale unresolved signals (48h+)...")
        cutoff = (datetime.utcnow() - timedelta(hours=48)).isoformat()
        cursor.execute("""
            SELECT signal_id, user_id, created_at
            FROM signal_deliveries
            WHERE outcome IS NULL
              AND created_at < ?
        """, (cutoff,))
        stale = cursor.fetchall()
        if stale:
            print(f"  ⚠️  Found {len(stale)} signal(s) unresolved after 48h:")
            for s in stale[:10]:
                print(f"     signal_id={s['signal_id']}  "
                      f"created={s['created_at']}")
            if len(stale) > 10:
                print(f"     ... and {len(stale) - 10} more")
            print(f"  Tip: python resolve_matches.py --dry-run")
            issues_found += len(stale)
        else:
            print("  ✅ No stale unresolved signals")

        # ── Check 3: Delivery summary ───────────────────────────────
        print("\n[3] Delivery summary...")
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN outcome = 'WON'  THEN 1 ELSE 0 END) as won,
                SUM(CASE WHEN outcome = 'LOST' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN outcome = 'VOID' THEN 1 ELSE 0 END) as voided,
                SUM(CASE WHEN outcome IS NULL  THEN 1 ELSE 0 END) as pending
            FROM signal_deliveries
        """)
        summary = cursor.fetchone()
        if summary and summary['total']:
            print(f"  Total signals delivered : {summary['total']:,}")
            print(f"  Won                     : {summary['won']  or 0}")
            print(f"  Lost                    : {summary['lost'] or 0}")
            print(f"  Void                    : {summary['voided'] or 0}")
            print(f"  Pending resolution      : {summary['pending'] or 0}")
        else:
            print("  No delivery records found yet.")

        # ── Verdict ─────────────────────────────────────────────────
        print("\n" + "-" * 55)
        if issues_found == 0:
            print("✅ AUDIT PASSED — no delivery integrity issues found")
        else:
            print(f"❌ AUDIT FAILED — {issues_found} issue(s) require attention")
        print("-" * 55 + "\n")

    except Exception as e:
        print(f"\n❌ Audit error: {e}")
        issues_found += 1

    finally:
        if conn:
            conn.close()

    return issues_found


if __name__ == "__main__":
    exit_code = run_audit()
    sys.exit(0 if exit_code == 0 else 1)
