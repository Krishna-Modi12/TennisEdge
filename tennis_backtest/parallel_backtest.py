#!/usr/bin/env python3
"""
tennis_backtest/parallel_backtest.py

Parallel A/B backtest engine for TennisEdge.

MODEL A (current):
    Prob source:  Point-in-time Elo (surface-blended)
    Odds source:  B365 (B365W/B365L)
    Edge:         model_prob - (1/B365_odds)
    Threshold:    edge >= 8%

MODEL B (Pinnacle-first):
    Prob source:  Pinnacle implied prob, margin-stripped
                  true_prob = (1/PSW) / (1/PSW + 1/PSL)
    Filter:       Skip if Elo disagrees on winner direction
    Odds source:  B365 (B365W/B365L)
    Edge:         pinnacle_prob - (1/B365_odds)
    Threshold:    edge >= 5%

Data source: tennis-data.co.uk Excel files (2015-2024)
No DB dependency — fully self-contained.
"""

import argparse
import io
import math
import sys
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests
from scipy import stats as scipy_stats


# ═══════════════════════════════════════════════════════════════════════════════
# DEFAULTS
# ═══════════════════════════════════════════════════════════════════════════════

ELO_DEFAULT  = 1500.0
ELO_K        = 32
SURFACE_W    = 0.60

DEFAULT_MODEL_A_EDGE = 0.08
DEFAULT_MODEL_A_PROB = 0.35
DEFAULT_MODEL_B_EDGE = 0.05
DEFAULT_MODEL_B_PROB = 0.30

MIN_ODDS = 1.20
MAX_ODDS = 10.0
STAKE = 100.0


# ═══════════════════════════════════════════════════════════════════════════════
# DATA DOWNLOAD
# ═══════════════════════════════════════════════════════════════════════════════

def download_year(year: int, tour: str) -> pd.DataFrame:
    """Download a year's match data. Support for 'atp', 'wta', and 'challenger'."""
    if tour == "challenger":
        url = f"http://www.tennis-data.co.uk/{year}c/{year}.xlsx"
    elif tour == "wta":
        url = f"http://www.tennis-data.co.uk/{year}w/{year}.xlsx"
    else:
        url = f"http://www.tennis-data.co.uk/{year}/{year}.xlsx"
    
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            df = pd.read_excel(io.BytesIO(resp.content))
            df.columns = [str(c).strip() for c in df.columns]
            return df
        # Fallback for older years or different naming
        if tour != "challenger":
            suffix = "w" if tour == "wta" else ""
            for ext in ["xlsx", "xls"]:
                alt_url = f"http://www.tennis-data.co.uk/{year}{suffix}/{year}.{ext}"
                resp = requests.get(alt_url, timeout=30)
                if resp.status_code == 200:
                    df = pd.read_excel(io.BytesIO(resp.content))
                    df.columns = [str(c).strip() for c in df.columns]
                    return df
    except Exception:
        pass
    return pd.DataFrame()

def normalize_surface(s) -> str:
    if not isinstance(s, str): return "hard"
    s = s.strip().lower()
    return {"hard": "hard", "clay": "clay", "grass": "grass",
            "carpet": "hard", "indoor": "hard"}.get(s, "hard")

def parse_matches(df: pd.DataFrame) -> list[dict]:
    cols_lower = {c.lower(): c for c in df.columns}
    def get_col(name): return cols_lower.get(name.lower())
    
    winner_col = get_col("Winner"); loser_col = get_col("Loser")
    if not winner_col or not loser_col: return []
    
    b365w_col = get_col("B365W"); b365l_col = get_col("B365L")
    psw_col = get_col("PSW"); psl_col = get_col("PSL")
    date_col = get_col("Date"); surf_col = get_col("Surface")
    
    if not b365w_col or not b365l_col: return []
    
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
        
    matches = []
    for _, row in df.iterrows():
        try:
            winner = str(row[winner_col]).strip()
            loser = str(row[loser_col]).strip()
            if not winner or not loser or winner == "nan" or loser == "nan": continue
            
            b365w = float(row[b365w_col]); b365l = float(row[b365l_col])
            if pd.isna(b365w) or pd.isna(b365l): continue
            
            psw = float(row[psw_col]) if psw_col and not pd.isna(row[psw_col]) else None
            psl = float(row[psl_col]) if psl_col and not pd.isna(row[psl_col]) else None
            
            surface = normalize_surface(row.get(surf_col, "hard") if surf_col else "hard")
            date_str = row[date_col].strftime("%Y-%m-%d") if date_col and not pd.isna(row[date_col]) else "2000-01-01"
            
            matches.append({
                "date": date_str, "winner": winner, "loser": loser, 
                "surface": surface, "b365w": b365w, "b365l": b365l, 
                "psw": psw, "psl": psl
            })
        except Exception: continue
    return matches


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINES
# ═══════════════════════════════════════════════════════════════════════════════

class EloEngine:
    def __init__(self):
        self.ratings = {}
    def _get(self, p, s): return self.ratings.get((p, s), ELO_DEFAULT)
    def predict(self, p1, p2, s):
        def _blend(p, s): return 0.6 * self._get(p, s) + 0.4 * self._get(p, "overall")
        ra, rb = _blend(p1, s), _blend(p2, s)
        prob = 1.0 / (1.0 + 10**((rb - ra) / 400.0))
        return {"prob_a": prob, "prob_b": 1.0 - prob}
    def update(self, w, l, s):
        for surf in ("overall", s):
            ra, rb = self._get(w, surf), self._get(l, surf)
            ea = 1.0 / (1.0 + 10**((rb - ra) / 400.0))
            self.ratings[(w, surf)] = ra + ELO_K * (1.0 - ea)
            self.ratings[(l, surf)] = rb + ELO_K * (0.0 - (1.0 - ea))

def strip_pin(psw, psl):
    if not psw or not psl: return None, None
    total = (1/psw) + (1/psl)
    return (1/psw)/total, (1/psl)/total

@dataclass
class BetResult:
    date: str; player: str; opponent: str; surface: str; odds: float
    model_prob: float; edge: float; won: bool; profit: float; year: int = 0
    def __post_init__(self): self.year = int(self.date[:4])


# ═══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def run_backtest(from_yr, to_yr, tour, stake, a_edge_t, a_prob_t, b_edge_t, b_prob_t):
    all_m = []
    tours = ["atp", "wta"] if tour == "both" else [tour]
    for t in tours:
        for y in range(from_yr, to_yr + 1):
            print(f"  Fetching {t.upper()} {y}...", end=" ", flush=True)
            df = download_year(y, t)
            if not df.empty:
                m = parse_matches(df)
                all_m.extend(m)
                print(f"{len(m)} matches")
            else: print("failed")
            time.sleep(0.2)
            
    all_m.sort(key=lambda x: x["date"])
    elo = EloEngine()
    res_a, res_b = [], []
    
    for m in all_m:
        ep = elo.predict(m["winner"], m["loser"], m["surface"])
        
        # Model A
        for p, o, w, prob in [(m["winner"], m["b365w"], True, ep["prob_a"]), 
                               (m["loser"], m["b365l"], False, ep["prob_b"])]:
            if MIN_ODDS <= o <= MAX_ODDS:
                edge = prob - (1/o)
                if edge >= a_edge_t and prob >= a_prob_t:
                    res_a.append(BetResult(m["date"], p, m["loser"] if w else m["winner"], 
                                           m["surface"], o, prob, edge, w, stake*(o-1) if w else -stake))
        
        # Model B
        pw, pl = strip_pin(m["psw"], m["psl"])
        if pw:
            for p, o, w, prob, ep_p in [(m["winner"], m["b365w"], True, pw, ep["prob_a"]),
                                         (m["loser"], m["b365l"], False, pl, ep["prob_b"])]:
                if MIN_ODDS <= o <= MAX_ODDS:
                    if (prob > 0.5 and ep_p < 0.5) or (prob < 0.5 and ep_p > 0.5): continue
                    edge = prob - (1/o)
                    if edge >= b_edge_t and prob >= b_prob_t:
                        res_b.append(BetResult(m["date"], p, m["loser"] if w else m["winner"], 
                                               m["surface"], o, prob, edge, w, stake*(o-1) if w else -stake))
        
        elo.update(m["winner"], m["loser"], m["surface"])
    return res_a, res_b

def build_summary(results, stake):
    if not results: return {"total": 0}
    df = pd.DataFrame([vars(r) for r in results])
    total = len(df); wins = int(df["won"].sum())
    profit = df["profit"].sum(); avg_o = df["odds"].mean()
    p_val = scipy_stats.binomtest(wins, total, 1/avg_o if avg_o > 0 else 0.5, alternative="greater").pvalue
    
    yearly = {}
    for y, g in df.groupby("year"):
        yearly[y] = {"bets": len(g), "roi": (g["profit"].sum() / (len(g)*stake))*100, "profit": g["profit"].sum()}
    
    surf = {}
    for s, g in df.groupby("surface"):
        surf[s] = {"bets": len(g), "roi": (g["profit"].sum() / (len(g)*stake))*100, "profit": g["profit"].sum()}
        
    return {"total": total, "wins": wins, "profit": profit, "roi": (profit/(total*stake))*100, 
            "p_val": p_val, "yearly": yearly, "surf": surf}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from-year", type=int, default=2015)
    p.add_argument("--to-year", type=int, default=2024)
    p.add_argument("--tour", type=str, default="both")
    p.add_argument("--stake", type=float, default=100.0)
    a = p.parse_args()
    
    res_a, res_b = run_backtest(a.from_year, a.to_year, a.tour, a.stake, 
                                DEFAULT_MODEL_A_EDGE, DEFAULT_MODEL_A_PROB, 
                                DEFAULT_MODEL_B_EDGE, DEFAULT_MODEL_B_PROB)
    
    sa = build_summary(res_a, a.stake); sb = build_summary(res_b, a.stake)
    
    print("\n" + "═"*80)
    print(f"{'Metric':<20} {'Model A (Elo)':>28} {'Model B (Pinnacle)':>28}")
    print("─"*80)
    for k, label in [("total", "Total Bets"), ("profit", "Profit/Loss"), ("roi", "ROI (%)")]:
        va = f"{sa.get(k, 0):.2f}" if isinstance(sa.get(k), float) else sa.get(k, 0)
        vb = f"{sb.get(k, 0):.2f}" if isinstance(sb.get(k), float) else sb.get(k, 0)
        print(f"{label:<20} {va:>28} {vb:>28}")
    print(f"{'P-value':<20} {sa.get('p_val', 1):>28.4f} {sb.get('p_val', 1):>28.4f}")
    
    print("\nYEARLY (A vs B)")
    yrs = sorted(set(list(sa.get("yearly", {}).keys()) + list(sb.get("yearly", {}).keys())))
    for y in yrs:
        da, db = sa.get("yearly", {}).get(y, {}), sb.get("yearly", {}).get(y, {})
        print(f"{y}: A({da.get('bets',0):>4}, {da.get('roi',0):>6.1f}%) | B({db.get('bets',0):>4}, {db.get('roi',0):>6.1f}%)")

    print("\nSURFACE (A vs B)")
    sfs = sorted(set(list(sa.get("surf", {}).keys()) + list(sb.get("surf", {}).keys())))
    for s in sfs:
        da, db = sa.get("surf", {}).get(s, {}), sb.get("surf", {}).get(s, {})
        print(f"{s:<8}: A({da.get('bets',0):>4}, {da.get('roi',0):>6.1f}%) | B({db.get('bets',0):>4}, {db.get('roi',0):>6.1f}%)")

if __name__ == "__main__": main()
