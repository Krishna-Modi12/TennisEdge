
import io
import math
import pandas as pd
import requests
import argparse
from scipy import stats

def download_challenger_data(year: int) -> pd.DataFrame:
    """Download Challenger Excel file from tennis-data.co.uk."""
    url = f"http://www.tennis-data.co.uk/{year}/{year}ch.xlsx"
    print(f"Downloading {url}...", end=" ", flush=True)
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            df = pd.read_excel(io.BytesIO(resp.content))
            df.columns = [c.strip().lower() for c in df.columns]
            print(f"Done ({len(df)} rows)")
            return df
        else:
            # Try .xls if .xlsx fails
            url_xls = f"http://www.tennis-data.co.uk/{year}/{year}ch.xls"
            resp = requests.get(url_xls, timeout=30)
            if resp.status_code == 200:
                df = pd.read_excel(io.BytesIO(resp.content))
                df.columns = [c.strip().lower() for c in df.columns]
                print(f"Done ({len(df)} rows)")
                return df
            print(f"Failed (Status {resp.status_code})")
    except Exception as e:
        print(f"Error: {e}")
    return pd.DataFrame()

def strip_margin(psw, psl):
    """Strip overround from Pinnacle odds to get implied probabilities."""
    if pd.isna(psw) or pd.isna(psl) or psw <= 1.0 or psl <= 1.0:
        return None, None
    
    implied_w = 1.0 / psw
    implied_l = 1.0 / psl
    total_implied = implied_w + implied_l
    
    prob_w = implied_w / total_implied
    prob_l = implied_l / total_implied
    return prob_w, prob_l

def calculate_p_value(returns):
    """Calculate p-value using a one-sample t-test (H0: mean return = 0)."""
    if len(returns) < 2:
        return 1.0
    t_stat, p_val = stats.ttest_1samp(returns, 0)
    # We want one-sided p-value if we want to test for positive ROI?
    # But usually p-value in these reports is two-sided unless specified.
    # The prompt says 0.55 on 20 bets was "statistically indistinguishable from luck".
    return p_val

def run_backtest(years, edge_threshold=0.05, stake=100.0):
    all_signals = []
    
    for year in years:
        df = download_challenger_data(year)
        if df.empty:
            continue
            
        required_cols = ['winner', 'loser', 'psw', 'psl', 'b365w', 'b365l', 'date']
        # Check if they exist, case insensitive check was done by .lower()
        if not all(col in df.columns for col in ['winner', 'loser', 'psw', 'psl', 'b365w', 'b365l']):
            print(f"Skipping year {year}: Missing required odds columns")
            continue
            
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        else:
            df['date'] = pd.Timestamp(f"{year}-01-01")

        for _, row in df.iterrows():
            # Get Pinnacle Implied Probs
            prob_w, prob_l = strip_margin(row['psw'], row['psl'])
            if prob_w is None:
                continue
                
            # Check Bet365 Edge
            b365w = row['b365w']
            b365l = row['b365l']
            
            if pd.isna(b365w) or pd.isna(b365l) or b365w <= 1.0 or b365l <= 1.0:
                continue
            
            # Winner side
            edge_w = (prob_w * b365w) - 1.0
            if edge_w >= edge_threshold:
                all_signals.append({
                    'date': row['date'],
                    'year': year,
                    'player': row['winner'],
                    'opponent': row['loser'],
                    'odds': b365w,
                    'pinnacle_prob': prob_w,
                    'edge': edge_w,
                    'won': 1,
                    'profit': (b365w - 1) * stake
                })
                
            # Loser side
            edge_l = (prob_l * b365l) - 1.0
            if edge_l >= edge_threshold:
                all_signals.append({
                    'date': row['date'],
                    'year': year,
                    'player': row['loser'],
                    'opponent': row['winner'],
                    'odds': b365l,
                    'pinnacle_prob': prob_l,
                    'edge': edge_l,
                    'won': 0,
                    'profit': -stake
                })
                
    if not all_signals:
        print("No signals found.")
        return
        
    results_df = pd.DataFrame(all_signals)
    
    # Global Stats
    total_bets = len(results_df)
    wins = results_df['won'].sum()
    win_rate = (wins / total_bets) * 100
    total_profit = results_df['profit'].sum()
    total_staked = total_bets * stake
    roi = (total_profit / total_staked) * 100
    p_value = calculate_p_value(results_df['profit'])
    
    print("\n" + "="*50)
    print(f"BACKTEST RESULTS: CHALLENGER DATA ({years[0]}-{years[-1]})")
    print(f"MODEL B: Pinnacle Implied Prob vs Bet365 Edge (threshold={edge_threshold*100}%)")
    print("="*50)
    print(f"Total Bets:    {total_bets}")
    print(f"Win%:          {win_rate:.2f}%")
    print(f"Total Profit:  {total_profit:.2f}")
    print(f"ROI:           {roi:.2f}%")
    print(f"P-value:       {p_value:.4f}")
    
    # Yearly breakdown
    print("\nYearly Breakdown:")
    yearly = results_df.groupby('year').agg(
        bets=('won', 'count'),
        wins=('won', 'sum'),
        profit=('profit', 'sum')
    )
    yearly['roi'] = (yearly['profit'] / (yearly['bets'] * stake)) * 100
    print(yearly.to_string())
    
    # Export signals to CSV for inspection
    results_df.to_csv('challenger_signals.csv', index=False)
    print("\nSignals saved to challenger_signals.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--threshold", type=float, default=0.05)
    args = parser.parse_args()
    
    years = list(range(args.start_year, args.end_year + 1))
    run_backtest(years, edge_threshold=args.threshold)
