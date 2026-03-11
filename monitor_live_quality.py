# monitor_live_quality.py
"""
Compares live production signal performance against the backtest baseline.
Reads from the signal_deliveries table in the production database.

Run from: project root
Run weekly or whenever you want a live vs backtest comparison.
"""
import sqlite3
import os
import sys
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "database/signals.db")

# Backtest baseline — do not change during paper trading validation
BACKTEST_ROI      = 14.8
BACKTEST_WIN_RATE = 45.4
FLAT_STAKE        = 100   # ₹ — used if stake column not available

GAP_OK          = 10    # pp — within this = OK
GAP_MONITOR     = 20    # pp — within this = MONITOR
# above GAP_MONITOR = INVESTIGATE


def gap_label(live_val: float, baseline: float) -> str:
    gap = abs(live_val - baseline)
    if gap <= GAP_OK:
        return "OK ✅"
    elif gap <= GAP_MONITOR:
        return "MONITOR ⚠️"
    else:
        return "INVESTIGATE ❌"


def run_monitor():

    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at: {DB_PATH}")
        sys.exit(1)

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Confirm signal_deliveries table exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='signal_deliveries'
        """)
        if not cursor.fetchone():
            print("❌ signal_deliveries table not found in database.")
            sys.exit(1)

        # Fetch all resolved signals
        cursor.execute("""
            SELECT *
            FROM signal_deliveries
            WHERE outcome IN ('WON', 'LOST', 'VOID')
        """)
        resolved = cursor.fetchall()

        # Fetch total (including pending)
        cursor.execute("SELECT COUNT(*) as cnt FROM signal_deliveries")
        total_delivered = cursor.fetchone()['cnt']

        n = len(resolved)
        pending = total_delivered - n

        print("\n" + "=" * 50)
        print("  LIVE SIGNAL QUALITY MONITOR")
        print(f"  As of: {datetime.utcnow().isoformat()} UTC")
        print("=" * 50)
        print(f"\nSignals delivered : {total_delivered:,}")
        print(f"Resolved          : {n}  (Pending: {pending})")

        if n < 50:
            print(f"\n⚠️  Only {n} resolved signal(s) — too few for statistical")
            print(f"   conclusions. Do not make decisions on fewer than 50.")
            print(f"   Continue collecting data.")
            print("\n" + "=" * 50 + "\n")
            return

        # Calculate performance
        won   = sum(1 for r in resolved if r['outcome'] == 'WON')
        total_non_void = sum(1 for r in resolved if r['outcome'] != 'VOID')

        if total_non_void == 0:
            print("⚠️  All resolved signals are VOID — no performance data.")
            return

        win_rate = (won / total_non_void) * 100

        # Calculate profit — use profit column if it exists, else flat stake
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        # Re-fetch to get column names properly
        cursor.execute("PRAGMA table_info(signal_deliveries)")
        col_names = [row['name'] for row in cursor.fetchall()]

        total_profit = 0.0
        for r in resolved:
            if r['outcome'] == 'VOID':
                continue
            if 'profit' in col_names and r['profit'] is not None:
                total_profit += float(r['profit'])
            elif 'decimal_odds' in col_names and r['decimal_odds'] is not None:
                if r['outcome'] == 'WON':
                    total_profit += (float(r['decimal_odds']) - 1) * FLAT_STAKE
                else:
                    total_profit -= FLAT_STAKE
            else:
                # No odds column — estimate from win/loss only
                if r['outcome'] == 'WON':
                    total_profit += FLAT_STAKE * 1.5   # rough estimate
                else:
                    total_profit -= FLAT_STAKE

        roi = (total_profit / (total_non_void * FLAT_STAKE)) * 100

        print(f"\n--- LIVE PERFORMANCE ---")
        print(f"Win Rate  : {win_rate:.1f}%    (Backtest: {BACKTEST_WIN_RATE}%)")
        print(f"ROI       : {roi:.1f}%    (Backtest: {BACKTEST_ROI}%)")

        wr_gap  = win_rate - BACKTEST_WIN_RATE
        roi_gap = roi - BACKTEST_ROI

        print(f"\n--- GAP TO BACKTEST ---")
        print(f"Win rate gap : {wr_gap:+.1f}pp  [{gap_label(win_rate, BACKTEST_WIN_RATE)}]")
        print(f"ROI gap      : {roi_gap:+.1f}pp  [{gap_label(roi, BACKTEST_ROI)}]")

        print(f"""
Gap thresholds:
  OK          : within {GAP_OK}pp of backtest
  MONITOR     : {GAP_OK}–{GAP_MONITOR}pp gap — watch closely
  INVESTIGATE : {GAP_MONITOR}+pp gap — pause new user acquisition, diagnose""")

        print(f"\n--- STATISTICAL CONFIDENCE ---")
        print(f"Sample size : {n} resolved signals")
        if n < 100:
            print(f"Note: Below 100 signals, results have high variance.")
            print(f"      Continue collecting before drawing firm conclusions.")

        # Verdict
        wr_status  = gap_label(win_rate, BACKTEST_WIN_RATE)
        roi_status = gap_label(roi, BACKTEST_ROI)

        print(f"\n--- VERDICT ---")
        if "INVESTIGATE" in wr_status or "INVESTIGATE" in roi_status:
            print("❌ REQUIRES INVESTIGATION")
            print("   Live performance is 20+pp below backtest on at least one metric.")
            print("   Recommended: pause new user acquisition and diagnose.")
        elif "MONITOR" in wr_status or "MONITOR" in roi_status:
            print("⚠️  NEEDS MONITORING")
            print("   Live performance is diverging from backtest.")
            print("   Continue but review again at next 10-signal interval.")
        else:
            print("✅ TRACKING WELL")
            print("   Live performance is within expected range of backtest.")

        print("\n" + "=" * 50 + "\n")

        # Save report
        report_lines = [
            f"LIVE SIGNAL QUALITY MONITOR — {datetime.utcnow().isoformat()}",
            f"Signals delivered : {total_delivered}",
            f"Resolved          : {n}  (Pending: {pending})",
            f"Win Rate          : {win_rate:.1f}%  (Backtest: {BACKTEST_WIN_RATE}%)",
            f"ROI               : {roi:.1f}%  (Backtest: {BACKTEST_ROI}%)",
            f"Win rate gap      : {wr_gap:+.1f}pp  [{wr_status}]",
            f"ROI gap           : {roi_gap:+.1f}pp  [{roi_status}]",
        ]
        with open("live_quality_report.txt", "w") as f:
            f.write("\n".join(report_lines))
        print("Saved to live_quality_report.txt")

    except Exception as e:
        print(f"\n❌ Monitor error: {e}")
        raise

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    run_monitor()
