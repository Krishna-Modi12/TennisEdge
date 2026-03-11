"""
backtest/engine.py
Historical backtesting engine for the TennisEdge dual-validation signal system.

Stages:
  1. Historical Backtest — simulate on past data from tennis-data.co.uk
  2. Walk-Forward Test  — train on old data, test on newer unseen data
  3. Paper Trading      — run live but don't bet real money

Usage:
    python -m backtest.engine --atp-from 2022 --atp-to 2024 --stake 100
"""

import argparse
import datetime
import io
import sys
from collections import defaultdict

import pandas as pd
import requests

from models.advanced_model import advanced_predict
from database.db import init_schema


# ── Core edge calculation (mirrors edge_detector.py logic) ───────────────────

def calculate_true_edge(model_prob: float, decimal_odds: float,
                        min_value_edge: float = 0.10,
                        min_model_prob: float = 0.40) -> dict:
    implied_prob = 1.0 / decimal_odds
    value_edge = (model_prob * decimal_odds) - 1.0
    confidence = model_prob / (1.0 - model_prob) if model_prob < 1.0 else 99.0
    true_edge_score = value_edge * confidence
    signal_valid = (value_edge >= min_value_edge and model_prob >= min_model_prob)

    return {
        "implied_prob":    round(implied_prob, 4),
        "value_edge":      round(value_edge, 4),
        "confidence":      round(confidence, 3),
        "true_edge_score": round(true_edge_score, 4),
        "signal_valid":    signal_valid,
    }


# ── Data download ────────────────────────────────────────────────────────────

def _download_year(year: int, tour: str) -> pd.DataFrame:
    suffix = "w" if tour == "wta" else ""
    for ext in ["xlsx", "xls"]:
        url = f"http://www.tennis-data.co.uk/{year}{suffix}/{year}.{ext}"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                df = pd.read_excel(io.BytesIO(resp.content))
                df.columns = [c.strip().lower() for c in df.columns]
                return df
        except Exception:
            continue
    return pd.DataFrame()


def _normalize_surface(s) -> str:
    if not isinstance(s, str):
        return "hard"
    s = s.strip().lower()
    return {"hard": "hard", "clay": "clay", "grass": "grass",
            "carpet": "hard", "indoor": "hard"}.get(s, "hard")


# ── Backtest Runner ──────────────────────────────────────────────────────────

def backtest(atp_from=2023, atp_to=2024, wta_from=2023, wta_to=2024,
             stake=100.0, min_value_edge=0.10, min_model_prob=0.40) -> dict:
    """
    Run historical backtest using dual-validation edge system.
    WARNING: This currently uses present-day database snapshots for form/H2H, 
    introducing look-ahead bias. Performance metrics will be optimistic.
    """
    print("\n" + "!" * 60)
    print("WARNING: METHODOLOGICAL LEAK DETECTED")
    print("This backtest uses the current DB-backed model which")
    print("computes a present-day snapshot of player stats.")
    print("This introduces look-ahead bias. ROI is optimistic.")
    print("!" * 60 + "\n")
    
    all_matches = []

    for tour, yr_from, yr_to in [("atp", atp_from, atp_to), ("wta", wta_from, wta_to)]:
        for year in range(yr_from, yr_to + 1):
            print(f"Downloading {tour.upper()} {year}...", end=" ", flush=True)
            df = _download_year(year, tour)
            if df.empty:
                print("no data")
                continue
            matches = _parse_matches(df)
            all_matches.extend(matches)
            print(f"{len(matches)} matches")

    print(f"\nTotal matches with odds: {len(all_matches)}")
    if not all_matches:
        return {"message": "No match data found"}

    # Run each match through the model
    results = []
    skipped = 0
    errors = 0

    for i, m in enumerate(all_matches):
        try:
            model = advanced_predict(m["winner"], m["loser"], m["surface"])

            # Check both sides: winner and loser
            for player, odds, is_winner, prob in [
                (m["winner"], m["odds_winner"], 1, model["prob_a"]),
                (m["loser"],  m["odds_loser"],  0, model["prob_b"]),
            ]:
                if odds <= 1.0 or odds > 20.0:
                    continue

                te = calculate_true_edge(prob, odds, min_value_edge, min_model_prob)

                if not te["signal_valid"]:
                    skipped += 1
                    continue

                if is_winner:
                    profit = stake * (odds - 1)
                else:
                    profit = -stake

                results.append({
                    "date":            m["date"],
                    "player":          player,
                    "opponent":        m["loser"] if player == m["winner"] else m["winner"],
                    "surface":         m["surface"],
                    "odds":            odds,
                    "model_prob":      round(prob, 4),
                    "value_edge":      te["value_edge"],
                    "confidence":      te["confidence"],
                    "true_edge_score": te["true_edge_score"],
                    "won":             is_winner,
                    "profit":          round(profit, 2),
                })

        except Exception as e:
            errors += 1
            continue

        if (i + 1) % 500 == 0:
            print(f"  ...processed {i+1}/{len(all_matches)} matches", flush=True)

    if not results:
        return {
            "message": "No valid signals generated",
            "total_matches": len(all_matches),
            "skipped": skipped,
            "errors": errors,
        }

    df = pd.DataFrame(results)

    # Summary statistics
    total_bets   = len(df)
    wins         = int(df["won"].sum())
    win_rate     = wins / total_bets
    total_profit = df["profit"].sum()
    total_staked = total_bets * stake
    roi          = (total_profit / total_staked) * 100
    avg_odds     = df["odds"].mean()
    avg_edge     = df["true_edge_score"].mean()

    # Monthly breakdown
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M")
    monthly = df.groupby("month").agg(
        bets=("won", "count"),
        wins=("won", "sum"),
        profit=("profit", "sum"),
    ).reset_index()
    monthly["roi"] = (monthly["profit"] / (monthly["bets"] * stake) * 100).round(1)

    # Surface breakdown
    by_surface = df.groupby("surface").agg(
        bets=("won", "count"),
        wins=("won", "sum"),
        profit=("profit", "sum"),
        avg_odds=("odds", "mean"),
    ).reset_index()
    by_surface["win_rate"] = (by_surface["wins"] / by_surface["bets"] * 100).round(1)
    by_surface["roi"] = (by_surface["profit"] / (by_surface["bets"] * stake) * 100).round(1)

    # Max drawdown
    cumulative = df["profit"].cumsum()
    peak = cumulative.cummax()
    drawdown = (cumulative - peak).min()

    return {
        "total_matches":    len(all_matches),
        "total_bets":       total_bets,
        "wins":             wins,
        "losses":           total_bets - wins,
        "win_rate":         f"{win_rate*100:.1f}%",
        "total_profit":     round(total_profit, 2),
        "total_staked":     round(total_staked, 2),
        "roi":              f"{roi:.2f}%",
        "avg_odds":         round(avg_odds, 2),
        "avg_true_edge":    f"{avg_edge*100:.1f}%",
        "max_drawdown":     round(drawdown, 2),
        "skipped_signals":  skipped,
        "errors":           errors,
        "by_surface":       by_surface.to_dict("records"),
        "monthly":          monthly.to_dict("records") if len(monthly) <= 36 else None,
    }


def _parse_matches(df: pd.DataFrame) -> list:
    """Extract matches with winner/loser and odds from tennis-data.co.uk format."""
    matches = []

    required = ["winner", "loser"]
    for col in required:
        if col not in df.columns:
            return matches

    # Detect odds columns — tennis-data uses B365W/B365L, PSW/PSL, etc.
    odds_winner_col = None
    odds_loser_col = None
    for prefix in ["b365", "ps", "max", "avg"]:
        wc = f"{prefix}w"
        lc = f"{prefix}l"
        if wc in df.columns and lc in df.columns:
            odds_winner_col = wc
            odds_loser_col = lc
            break

    if not odds_winner_col:
        return matches

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    else:
        df["date"] = pd.Timestamp.today()

    for _, row in df.iterrows():
        try:
            winner = str(row["winner"]).strip()
            loser = str(row["loser"]).strip()
            if not winner or not loser or winner == "nan" or loser == "nan":
                continue

            odds_w = float(row[odds_winner_col])
            odds_l = float(row[odds_loser_col])
            if pd.isna(odds_w) or pd.isna(odds_l) or odds_w <= 1.0 or odds_l <= 1.0:
                continue

            surface = _normalize_surface(row.get("surface", "hard"))
            match_date = row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"])

            matches.append({
                "date":        match_date,
                "winner":      winner,
                "loser":       loser,
                "surface":     surface,
                "odds_winner": odds_w,
                "odds_loser":  odds_l,
            })
        except Exception:
            continue

    return matches


def format_backtest_report(result: dict) -> str:
    """Format backtest results for Telegram display."""
    if "message" in result:
        return f"⚠️ {result['message']}"

    msg = (
        f"⚠️ *WARNING: METHODOLOGICAL LEAK* ⚠️\n"
        f"_This backtest uses present-day database snapshots rather than point-in-time logic._\n"
        f"_Results suffer from look-ahead bias and are inherently optimistic._\n\n"
        f"📊 *BACKTEST RESULTS*\n"
        f"━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎯 *Total Bets:* {result['total_bets']}\n"
        f"✅ *Wins:* {result['wins']} | ❌ *Losses:* {result['losses']}\n"
        f"📈 *Win Rate:* {result['win_rate']}\n\n"
        f"💰 *Total Profit:* ₹{result['total_profit']:,.2f}\n"
        f"💵 *Total Staked:* ₹{result['total_staked']:,.2f}\n"
        f"📊 *ROI:* {result['roi']}\n\n"
        f"🎲 *Avg Odds:* {result['avg_odds']}\n"
        f"🔥 *Avg True Edge:* {result['avg_true_edge']}\n"
        f"📉 *Max Drawdown:* ₹{result['max_drawdown']:,.2f}\n\n"
    )

    # Surface breakdown
    if result.get("by_surface"):
        msg += f"🌍 *By Surface:*\n"
        for s in result["by_surface"]:
            msg += (
                f"  {s['surface'].title()}: {s['bets']} bets, "
                f"WR {s['win_rate']}%, ROI {s['roi']}%\n"
            )
        msg += "\n"

    msg += (
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"_Matches analyzed: {result['total_matches']}_\n"
        f"_Signals rejected: {result['skipped_signals']}_"
    )
    return msg


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest TennisEdge signal system")
    parser.add_argument("--atp-from", type=int, default=2023)
    parser.add_argument("--atp-to", type=int, default=2024)
    parser.add_argument("--wta-from", type=int, default=2023)
    parser.add_argument("--wta-to", type=int, default=2024)
    parser.add_argument("--stake", type=float, default=100.0)
    parser.add_argument("--min-value-edge", type=float, default=0.10)
    parser.add_argument("--min-model-prob", type=float, default=0.40)
    args = parser.parse_args()

    print("Initializing database...")
    init_schema()

    result = backtest(
        atp_from=args.atp_from, atp_to=args.atp_to,
        wta_from=args.wta_from, wta_to=args.wta_to,
        stake=args.stake,
        min_value_edge=args.min_value_edge,
        min_model_prob=args.min_model_prob,
    )

    print("\n" + "=" * 50)
    print("BACKTEST RESULTS")
    print("=" * 50)
    for k, v in result.items():
        if k not in ("by_surface", "monthly"):
            print(f"  {k}: {v}")

    if result.get("by_surface"):
        print("\nBy Surface:")
        for s in result["by_surface"]:
            print(f"  {s['surface']}: {s['bets']} bets, WR {s['win_rate']}%, ROI {s['roi']}%")
