"""
report_model_performance.py
CLI script to report model ROI and CLV metrics.
"""

from database.db import get_conn
from monitoring.model_monitor import get_parameter_safety_report

def report():
    conn = get_conn()
    cur = conn.execute("""
        SELECT 
            COUNT(*) AS total_signals,
            SUM(CASE WHEN is_win THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN is_win = FALSE THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN is_win THEN (taken_odds - 1) ELSE -1 END) AS total_profit,
            AVG(clv_ratio) AS avg_clv
        FROM signal_performance
        WHERE is_win IS NOT NULL
    """)
    
    row = cur.fetchone()
    if not row or row[0] == 0:
        print("No resolved signals found in signal_performance table.")
        return

    total, wins, losses, profit, avg_clv = row
    
    # ROI calculation (assuming flat 1 unit stake per signal)
    total_staked = total
    roi = (profit / total_staked) * 100 if total_staked > 0 else 0
    
    print("MODEL PERFORMANCE REPORT")
    print("========================")
    print(f"Signals:     {total}")
    print(f"Wins:        {wins}")
    print(f"Losses:      {losses}")
    print(f"Win Rate:    {(wins/total*100):.1f}%")
    print(f"Total Profit:{profit:+.2f} units")
    print(f"ROI:         {roi:+.1f}%")
    print(f"Average CLV: {avg_clv:.3f}" if avg_clv else "Average CLV: N/A")
    print("")
    safety = get_parameter_safety_report()
    print("ADAPTIVE PARAMETER SAFETY")
    print("-------------------------")
    print(f"dynamic_edge_base: {safety['edge_threshold_base']:.4f} (in-bounds={safety['edge_in_bounds']})")
    print(f"kelly_multiplier:  {safety['kelly_multiplier']:.4f} (in-bounds={safety['kelly_in_bounds']})")
    print(f"Overall safe:      {safety['safe']}")
    print("========================")

if __name__ == "__main__":
    report()
