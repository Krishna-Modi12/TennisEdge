"""
scripts/backtest.py
Back-testing engine for TennisEdge value betting signals.

Replays historical match data chronologically, builds Elo ratings
incrementally, and simulates edge-detection signals to measure ROI.

Runs TWO or THREE models in parallel on every match:
  • Elo-Only   – surface-blended Elo probability (original model)
  • Blended    – 0.35×Elo + 0.25×form + 0.20×H2H + 0.20×surface_wr
  • Calibrated – blended probability adjusted by isotonic regression (--calibrated)

Usage:
    cd tennisedge
    python -m scripts.backtest --from-year 2015 --to-year 2024 --threshold 0.07
    python -m scripts.backtest --calibrated --threshold 0.10
"""

import argparse
import io
import logging
import math
import os
import pickle
import sys
from collections import defaultdict, deque

import pandas as pd
import requests

from utils import normalize_player_name

logger = logging.getLogger(__name__)

# ── Constants (mirrored from config / models) ─────────────────────────────────

ELO_DEFAULT_RATING = 1500.0
ELO_K_FACTOR = 32
SURFACE_ELO_WEIGHT = 0.60
MIN_ODDS = 1.40
MAX_ODDS = 6.00

# Blend weights for blended model (must sum to 1.0)
W_ELO = 0.35
W_FORM = 0.15
W_H2H = 0.30
W_SURFACE = 0.20

FORM_DECAY = 0.9      # exponential decay per match for recent form
FORM_N = 10            # number of recent matches for form
MIN_H2H = 3           # minimum H2H meetings to use real data
SURFACE_WR_YEARS = 2   # years of history for surface win rate
NEUTRAL = 0.5

BASE_URL = "http://www.tennis-data.co.uk"

SURFACE_MAP = {
    "hard": "hard",
    "clay": "clay",
    "grass": "grass",
    "carpet": "carpet",
    "indoor": "hard",
    "outdoor": "hard",
}

ODDS_COLUMNS = {
    "avg":  ("AvgW",  "AvgL"),
    "max":  ("MaxW",  "MaxL"),
    "b365": ("B365W", "B365L"),
    "ps":   ("PSW",   "PSL"),
}

# ── Calibration model ─────────────────────────────────────────────────────────

_calibration_model = None  # loaded on demand


def _load_calibration():
    """Load the isotonic regression model from disk. Returns True on success."""
    global _calibration_model
    cal_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "models", "calibration_model.pkl")
    if not os.path.exists(cal_path):
        logger.error("Calibration model not found at %s — run models.calibration first", cal_path)
        return False
    with open(cal_path, "rb") as f:
        _calibration_model = pickle.load(f)
    logger.info("Loaded calibration model from %s", cal_path)
    return True


def _calibrate(raw_prob: float) -> float:
    """Apply isotonic calibration to a raw blended probability."""
    return float(_calibration_model.predict([raw_prob])[0])

# ── Elo engine (self-contained, no DB dependency) ─────────────────────────────

_elo: dict[tuple[str, str], float] = {}


def _get_elo(player: str, surface: str) -> float:
    return _elo.get((player, surface), ELO_DEFAULT_RATING)


def _set_elo(player: str, surface: str, rating: float) -> None:
    _elo[(player, surface)] = rating


def _win_probability(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + math.pow(10, (rating_b - rating_a) / 400.0))


def _blended_rating(player: str, surface: str) -> float:
    overall = _get_elo(player, "overall")
    surface_r = _get_elo(player, surface)
    return SURFACE_ELO_WEIGHT * surface_r + (1 - SURFACE_ELO_WEIGHT) * overall


def _predict(player_a: str, player_b: str, surface: str) -> dict:
    elo_a = _blended_rating(player_a, surface)
    elo_b = _blended_rating(player_b, surface)
    prob_a = _win_probability(elo_a, elo_b)
    return {"prob_a": prob_a, "prob_b": 1.0 - prob_a, "elo_a": elo_a, "elo_b": elo_b}


def _update_elo(winner: str, loser: str, surface: str) -> None:
    for surf in ["overall", surface]:
        ra = _get_elo(winner, surf)
        rb = _get_elo(loser, surf)
        ea = _win_probability(ra, rb)
        _set_elo(winner, surf, ra + ELO_K_FACTOR * (1 - ea))
        _set_elo(loser, surf, rb + ELO_K_FACTOR * (0 - (1 - ea)))


# ── In-memory model components (no DB, no look-ahead bias) ────────────────────
#
# Accumulated chronologically during the backtest. Each function reads ONLY
# matches processed so far — exactly what a live model would see.

# Per-player recent matches: deque of (winner, surface, date_str)
_player_matches: dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

# H2H records: (sorted_p1, sorted_p2, surface) → list of winner names
_h2h_records: dict[tuple[str, str, str], list] = defaultdict(list)

# Surface records: (player, surface) → [total, wins] accumulated per year
# Structure: (player, surface) → deque of (date_str, won_bool)
_surface_records: dict[tuple[str, str], deque] = defaultdict(lambda: deque(maxlen=500))

# Fatigue: chronological list of (date, p1, p2, round, surface) for density calc
_fatigue_history: list = []


def _record_match(winner: str, loser: str, surface: str, date_str: str,
                  round_label: str = "") -> None:
    """Append a completed match to all in-memory trackers."""
    # Recent form
    _player_matches[winner].append((winner, surface, date_str))
    _player_matches[loser].append((winner, surface, date_str))

    # H2H
    a, b = (winner, loser) if winner <= loser else (loser, winner)
    _h2h_records[(a, b, surface)].append(winner)

    # Surface records
    _surface_records[(winner, surface)].append((date_str, True))
    _surface_records[(loser, surface)].append((date_str, False))

    # Fatigue history
    _fatigue_history.append((date_str, winner, loser, round_label, surface))


def _recent_form(player: str, surface: str) -> float:
    """Decay-weighted form from last FORM_N matches (any surface)."""
    matches = list(_player_matches[player])
    if not matches:
        return NEUTRAL

    recent = matches[-FORM_N:]  # last N
    recent.reverse()  # most recent first

    weighted_wins = 0.0
    weighted_total = 0.0

    for i, (winner, msurface, _) in enumerate(recent):
        w = FORM_DECAY ** i
        if msurface and msurface == surface:
            w += 0.05
        weighted_total += w
        if winner == player:
            weighted_wins += w

    return (weighted_wins / weighted_total) if weighted_total > 0 else NEUTRAL


def _h2h_edge(p1: str, p2: str, surface: str) -> float:
    """H2H win rate of p1 vs p2 on the given surface. Neutral if < MIN_H2H."""
    a, b = (p1, p2) if p1 <= p2 else (p2, p1)
    records = _h2h_records.get((a, b, surface), [])

    if len(records) < MIN_H2H:
        return NEUTRAL

    p1_wins = sum(1 for w in records if w == p1)
    return p1_wins / len(records)


def _surface_winrate(player: str, surface: str, current_date: str) -> float:
    """Win percentage on surface over the last SURFACE_WR_YEARS years."""
    records = _surface_records.get((player, surface))
    if not records:
        return NEUTRAL

    try:
        cutoff_year = int(current_date[:4]) - SURFACE_WR_YEARS
        cutoff = f"{cutoff_year}{current_date[4:]}"
    except (ValueError, IndexError):
        return NEUTRAL

    total = 0
    wins = 0
    for date_str, won in records:
        if date_str >= cutoff:
            total += 1
            if won:
                wins += 1

    return (wins / total) if total > 0 else NEUTRAL


def _blended_predict(player_a: str, player_b: str, surface: str,
                     elo_prob_a: float, current_date: str) -> dict:
    """Compute blended probability using Elo + form + H2H + surface winrate."""
    form_a = _recent_form(player_a, surface)
    form_b = _recent_form(player_b, surface)
    form_total = form_a + form_b
    form_prob = (form_a / form_total) if form_total > 0 else NEUTRAL

    h2h_prob = _h2h_edge(player_a, player_b, surface)

    sw_a = _surface_winrate(player_a, surface, current_date)
    sw_b = _surface_winrate(player_b, surface, current_date)
    sw_total = sw_a + sw_b
    surface_prob = (sw_a / sw_total) if sw_total > 0 else NEUTRAL

    blended_a = (
        W_ELO * elo_prob_a
        + W_FORM * form_prob
        + W_H2H * h2h_prob
        + W_SURFACE * surface_prob
    )
    blended_a = max(0.01, min(0.99, blended_a))

    return {
        "prob_a": blended_a,
        "prob_b": 1.0 - blended_a,
        "form": form_prob,
        "h2h": h2h_prob,
        "sw": surface_prob,
    }


# ── Surface / odds helpers ────────────────────────────────────────────────────

def _norm_surface(raw) -> str:
    if not raw or pd.isna(raw):
        return "hard"
    return SURFACE_MAP.get(str(raw).strip().lower(), "hard")


def _odds_to_prob(odds: float) -> float:
    if odds <= 0:
        return 0.0
    return 1.0 / odds


# ── Data download ─────────────────────────────────────────────────────────────

def _download_year(year: int, tour: str) -> pd.DataFrame | None:
    ext = "xlsx" if year >= 2013 else "xls"
    suffix = "w" if tour == "wta" else ""
    url = f"{BASE_URL}/{year}{suffix}/{year}.{ext}"
    logger.info("Downloading %s ...", url)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Download failed for %s %d: %s", tour.upper(), year, exc)
        return None
    try:
        return pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
    except Exception:
        try:
            return pd.read_excel(io.BytesIO(resp.content), engine="xlrd")
        except Exception as exc2:
            logger.warning("Parse failed for %s %d: %s", tour.upper(), year, exc2)
            return None


# ── Signal evaluation helper ──────────────────────────────────────────────────

# Movement threshold for backtest simulation
MOVEMENT_THRESHOLD_BT = 0.03  # Same as live: 3% minimum change to classify


def _bt_line_movement(odds_open: float, odds_close: float) -> str:
    """Backtest version of line_movement (self-contained, no DB)."""
    if odds_open is None or odds_close is None or odds_open <= 0:
        return "neutral"
    pct_change = (odds_close - odds_open) / odds_open
    if pct_change < -MOVEMENT_THRESHOLD_BT:
        return "with"
    elif pct_change > MOVEMENT_THRESHOLD_BT:
        return "against"
    return "neutral"


def _evaluate_signals(prob_w: float, prob_l: float,
                      market_w: float, market_l: float,
                      odds_w: float, odds_l: float,
                      threshold: float) -> tuple:
    """Find best edge signal.  Returns (edge, bet_on_winner, odds) or (None, None, None)."""
    edge_w = prob_w - market_w
    edge_l = prob_l - market_l

    best_edge = None
    bet_on_winner = None
    bet_odds = None

    if edge_w >= threshold and MIN_ODDS <= odds_w <= MAX_ODDS:
        best_edge = edge_w
        bet_on_winner = True
        bet_odds = odds_w

    if edge_l >= threshold and MIN_ODDS <= odds_l <= MAX_ODDS:
        if best_edge is None or edge_l > best_edge:
            best_edge = edge_l
            bet_on_winner = False
            bet_odds = odds_l

    return best_edge, bet_on_winner, bet_odds


# ── Core back-test loop ──────────────────────────────────────────────────────

def _make_stats():
    return {"signals": 0, "wins": 0, "losses": 0, "profit": 0.0, "total_edge": 0.0}


def _record_signal(stats: dict, edge: float, bet_on_winner: bool, bet_odds: float):
    stats["signals"] += 1
    stats["total_edge"] += edge
    if bet_on_winner:
        stats["wins"] += 1
        stats["profit"] += (bet_odds - 1.0)
    else:
        stats["losses"] += 1
        stats["profit"] -= 1.0


def _backtest_year(
    df: pd.DataFrame,
    threshold: float,
    odds_col: str,
    use_calibration: bool = False,
    use_movement: bool = False,
    use_fatigue: bool = False,
) -> dict:
    """Process one year's DataFrame. Returns stats for all models."""

    df.columns = [str(c).strip().lower() for c in df.columns]

    win_col, lose_col = ODDS_COLUMNS[odds_col]
    win_col_lower = win_col.lower()
    lose_col_lower = lose_col.lower()

    empty = {"elo": _make_stats(), "blended": _make_stats(),
             "calibrated": _make_stats(), "cal_movement": _make_stats(),
             "cal_fatigue": _make_stats()}

    if win_col_lower not in df.columns or lose_col_lower not in df.columns:
        logger.warning("Odds columns %s/%s not found — skipping.", win_col, lose_col)
        return empty

    # For movement simulation: use B365 as "opening" odds
    has_b365 = "b365w" in df.columns and "b365l" in df.columns

    # Sort chronologically
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date", na_position="last").reset_index(drop=True)

    elo_stats = _make_stats()
    blend_stats = _make_stats()
    cal_stats = _make_stats()
    mvmt_stats = _make_stats()
    fatigue_stats = _make_stats()

    # Import fatigue module only when needed
    _fatigue_adj_fn = None
    if use_fatigue:
        from models.fatigue import fatigue_adjustment as _fatigue_adj_fn

    for _, row in df.iterrows():
        raw_winner = row.get("winner")
        raw_loser = row.get("loser")
        if pd.isna(raw_winner) or pd.isna(raw_loser):
            continue

        winner = normalize_player_name(str(raw_winner).strip())
        loser = normalize_player_name(str(raw_loser).strip())
        if not winner or not loser:
            continue

        surface = _norm_surface(row.get("surface"))

        # Round label for fatigue tracking
        raw_round = row.get("round", "")
        round_label = str(raw_round).strip() if raw_round and not pd.isna(raw_round) else ""

        # Current date for surface winrate cutoff
        date_val = row.get("date")
        if pd.notna(date_val):
            try:
                current_date = pd.Timestamp(date_val).strftime("%Y-%m-%d")
            except Exception:
                current_date = "2020-01-01"
        else:
            current_date = "2020-01-01"

        # Read odds (closing: avg or selected column)
        odds_w = row.get(win_col_lower)
        odds_l = row.get(lose_col_lower)

        has_odds = (
            odds_w is not None
            and odds_l is not None
            and not pd.isna(odds_w)
            and not pd.isna(odds_l)
        )

        if has_odds:
            try:
                odds_w = float(odds_w)
                odds_l = float(odds_l)
            except (ValueError, TypeError):
                has_odds = False

        # Read B365 as "opening" odds for movement simulation
        open_w = open_l = None
        if use_movement and has_b365 and has_odds:
            raw_bw = row.get("b365w")
            raw_bl = row.get("b365l")
            if raw_bw is not None and raw_bl is not None and not pd.isna(raw_bw) and not pd.isna(raw_bl):
                try:
                    open_w = float(raw_bw)
                    open_l = float(raw_bl)
                except (ValueError, TypeError):
                    pass

        if has_odds and odds_w > 0 and odds_l > 0:
            # --- Signal detection BEFORE Elo/history update ---
            elo_model = _predict(winner, loser, surface)
            elo_prob_w = elo_model["prob_a"]
            elo_prob_l = elo_model["prob_b"]

            market_w = _odds_to_prob(odds_w)
            market_l = _odds_to_prob(odds_l)

            # 1) Elo-only signals
            edge, bow, bodds = _evaluate_signals(
                elo_prob_w, elo_prob_l, market_w, market_l,
                odds_w, odds_l, threshold,
            )
            if edge is not None:
                _record_signal(elo_stats, edge, bow, bodds)

            # 2) Blended model signals
            blend = _blended_predict(winner, loser, surface, elo_prob_w, current_date)
            edge_b, bow_b, bodds_b = _evaluate_signals(
                blend["prob_a"], blend["prob_b"], market_w, market_l,
                odds_w, odds_l, threshold,
            )
            if edge_b is not None:
                _record_signal(blend_stats, edge_b, bow_b, bodds_b)

            # 3) Calibrated blended signals
            if use_calibration:
                cal_p = _calibrate(blend["prob_a"])
                edge_c, bow_c, bodds_c = _evaluate_signals(
                    cal_p, 1.0 - cal_p, market_w, market_l,
                    odds_w, odds_l, threshold,
                )
                if edge_c is not None:
                    _record_signal(cal_stats, edge_c, bow_c, bodds_c)

                    # 4) Calibrated + Movement filter
                    if use_movement and open_w is not None:
                        # Determine which player was bet on
                        if bow_c:  # bet on winner
                            mvmt = _bt_line_movement(open_w, odds_w)
                        else:      # bet on loser
                            mvmt = _bt_line_movement(open_l, odds_l)

                        if mvmt != "against":
                            _record_signal(mvmt_stats, edge_c, bow_c, bodds_c)

                    # 5) Calibrated + Fatigue adjustment
                    if use_fatigue and _fatigue_adj_fn is not None:
                        adj = _fatigue_adj_fn(
                            winner, loser, current_date, _fatigue_history
                        )
                        fat_p = max(0.01, min(0.99, cal_p + adj))
                        edge_f, bow_f, bodds_f = _evaluate_signals(
                            fat_p, 1.0 - fat_p, market_w, market_l,
                            odds_w, odds_l, threshold,
                        )
                        if edge_f is not None:
                            _record_signal(fatigue_stats, edge_f, bow_f, bodds_f)

        # --- Update Elo + in-memory history AFTER signal evaluation ---
        _update_elo(winner, loser, surface)
        _record_match(winner, loser, surface, current_date, round_label)

    return {"elo": elo_stats, "blended": blend_stats,
            "calibrated": cal_stats, "cal_movement": mvmt_stats,
            "cal_fatigue": fatigue_stats}


# ── Main orchestrator ─────────────────────────────────────────────────────────

def run_backtest(
    from_year: int = 2015,
    to_year: int = 2024,
    threshold: float = 0.07,
    odds_col: str = "avg",
    tour: str = "both",
    use_calibration: bool = False,
    use_movement: bool = False,
    use_fatigue: bool = False,
) -> dict:
    """
    Run the full back-test across the requested year range.
    Returns a dict keyed by year, each with model stats plus a "TOTAL" key.
    """
    results: dict[int | str, dict] = {}
    tours = []
    if tour in ("atp", "both"):
        tours.append("atp")
    if tour in ("wta", "both"):
        tours.append("wta")

    model_keys = ["elo", "blended"]
    if use_calibration:
        model_keys.append("calibrated")
    if use_movement:
        model_keys.append("cal_movement")
    if use_fatigue:
        model_keys.append("cal_fatigue")

    for year in range(from_year, to_year + 1):
        year_stats = {k: _make_stats() for k in model_keys}

        for t in tours:
            df = _download_year(year, t)
            if df is None:
                continue
            stats = _backtest_year(df, threshold, odds_col, use_calibration,
                                   use_movement, use_fatigue)
            for mk in model_keys:
                for key in ("signals", "wins", "losses", "profit", "total_edge"):
                    year_stats[mk][key] += stats[mk][key]

        for mk in model_keys:
            year_stats[mk]["profit"] = round(year_stats[mk]["profit"], 2)
        results[year] = year_stats

        parts = []
        for mk in model_keys:
            parts.append(f"{mk}={year_stats[mk]['signals']}({year_stats[mk]['profit']:.1f})")
        logger.info("Year %d: %s", year, " | ".join(parts))

    # Totals
    totals = {k: _make_stats() for k in model_keys}
    for ydata in results.values():
        for mk in model_keys:
            for key in ("signals", "wins", "losses", "profit", "total_edge"):
                totals[mk][key] += ydata[mk][key]
    for mk in model_keys:
        totals[mk]["profit"] = round(totals[mk]["profit"], 2)
    results["TOTAL"] = totals

    return results


# ── Pretty-print results (side-by-side) ──────────────────────────────────────

def print_results(results: dict, threshold: float, odds_col: str, tour: str,
                  use_calibration: bool = False, use_movement: bool = False,
                  use_fatigue: bool = False) -> None:
    if use_fatigue:
        _print_results_fatigue(results, threshold, odds_col, tour)
    elif use_movement:
        _print_results_movement(results, threshold, odds_col, tour)
    elif use_calibration:
        _print_results_3col(results, threshold, odds_col, tour)
    else:
        _print_results_2col(results, threshold, odds_col, tour)


def _print_results_movement(results: dict, threshold: float, odds_col: str, tour: str) -> None:
    """3-way comparison: Elo-Only / Calibrated / Calibrated+Movement."""
    header = (
        f"TennisEdge Backtest — Movement Filter  "
        f"(threshold={threshold * 100:.1f}%, odds={odds_col}, tour={tour})"
    )
    print(f"\n{header}")
    print("=" * len(header))
    print(
        f"{'':6}│{'─ Elo-Only ─':^25}│{'─ Calibrated ─':^25}│{'─ Cal+Movement ─':^25}"
    )
    print(
        f"{'Year':6}│ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
        f" │ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
        f" │ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
    )
    print(f"{'─' * 6}┼{'─' * 25}┼{'─' * 25}┼{'─' * 25}")

    for key, ydata in results.items():
        if key == "TOTAL":
            continue
        _print_row_3(str(key), ydata["elo"], ydata["calibrated"], ydata["cal_movement"])

    print(f"{'─' * 6}┼{'─' * 25}┼{'─' * 25}┼{'─' * 25}")
    _print_row_3("TOTAL", results["TOTAL"]["elo"],
                 results["TOTAL"]["calibrated"], results["TOTAL"]["cal_movement"])

    # Delta summary
    te = results["TOTAL"]["elo"]
    tc = results["TOTAL"]["calibrated"]
    tm = results["TOTAL"]["cal_movement"]
    e_roi = (te["profit"] / te["signals"] * 100) if te["signals"] else 0
    c_roi = (tc["profit"] / tc["signals"] * 100) if tc["signals"] else 0
    m_roi = (tm["profit"] / tm["signals"] * 100) if tm["signals"] else 0
    e_wr = (te["wins"] / te["signals"] * 100) if te["signals"] else 0
    c_wr = (tc["wins"] / tc["signals"] * 100) if tc["signals"] else 0
    m_wr = (tm["wins"] / tm["signals"] * 100) if tm["signals"] else 0

    print(f"\n  vs Elo-Only:")
    print(f"    Calibrated:     ROI {c_roi-e_roi:+.1f}pp  Win% {c_wr-e_wr:+.1f}pp")
    print(f"    Cal+Movement:   ROI {m_roi-e_roi:+.1f}pp  Win% {m_wr-e_wr:+.1f}pp")
    print(f"\n  Movement filter impact (Cal vs Cal+Mvmt):")
    if tc["signals"] > 0 and tm["signals"] > 0:
        pct_kept = tm["signals"] / tc["signals"] * 100
        print(f"    Signals kept:  {tm['signals']}/{tc['signals']} ({pct_kept:.0f}%)")
        print(f"    ROI change:    {m_roi-c_roi:+.1f}pp")
        print(f"    Win% change:   {m_wr-c_wr:+.1f}pp")
    print()


def _print_results_fatigue(results: dict, threshold: float, odds_col: str, tour: str) -> None:
    """Side-by-side: Calibrated vs Calibrated+Fatigue."""
    header = (
        f"TennisEdge Backtest — Fatigue Model  "
        f"(threshold={threshold * 100:.1f}%, odds={odds_col}, tour={tour})"
    )
    print(f"\n{header}")
    print("=" * len(header))
    print(
        f"{'':6}│{'─ Elo-Only ─':^25}│{'─ Calibrated ─':^25}│{'─ Cal+Fatigue ─':^25}"
    )
    print(
        f"{'Year':6}│ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
        f" │ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
        f" │ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
    )
    print(f"{'─' * 6}┼{'─' * 25}┼{'─' * 25}┼{'─' * 25}")

    for key, ydata in results.items():
        if key == "TOTAL":
            continue
        _print_row_3(str(key), ydata["elo"], ydata["calibrated"], ydata["cal_fatigue"])

    print(f"{'─' * 6}┼{'─' * 25}┼{'─' * 25}┼{'─' * 25}")
    _print_row_3("TOTAL", results["TOTAL"]["elo"],
                 results["TOTAL"]["calibrated"], results["TOTAL"]["cal_fatigue"])

    # Delta summary
    te = results["TOTAL"]["elo"]
    tc = results["TOTAL"]["calibrated"]
    tf = results["TOTAL"]["cal_fatigue"]
    e_roi = (te["profit"] / te["signals"] * 100) if te["signals"] else 0
    c_roi = (tc["profit"] / tc["signals"] * 100) if tc["signals"] else 0
    f_roi = (tf["profit"] / tf["signals"] * 100) if tf["signals"] else 0
    e_wr = (te["wins"] / te["signals"] * 100) if te["signals"] else 0
    c_wr = (tc["wins"] / tc["signals"] * 100) if tc["signals"] else 0
    f_wr = (tf["wins"] / tf["signals"] * 100) if tf["signals"] else 0

    print(f"\n  vs Elo-Only:")
    print(f"    Calibrated:     ROI {c_roi-e_roi:+.1f}pp  Win% {c_wr-e_wr:+.1f}pp")
    print(f"    Cal+Fatigue:    ROI {f_roi-e_roi:+.1f}pp  Win% {f_wr-e_wr:+.1f}pp")
    print(f"\n  Fatigue adjustment impact (Cal vs Cal+Fatigue):")
    if tc["signals"] > 0 and tf["signals"] > 0:
        print(f"    Calibrated sigs:  {tc['signals']}  Cal+Fatigue sigs: {tf['signals']}")
        print(f"    ROI change:    {f_roi-c_roi:+.1f}pp")
        print(f"    Win% change:   {f_wr-c_wr:+.1f}pp")
    print()


def _print_results_3col(results: dict, threshold: float, odds_col: str, tour: str) -> None:
    header = (
        f"TennisEdge Backtest — Elo vs Blended vs Calibrated  "
        f"(threshold={threshold * 100:.1f}%, odds={odds_col}, tour={tour})"
    )
    print(f"\n{header}")
    print("=" * len(header))
    print(
        f"{'':6}│{'─ Elo-Only ─':^25}│{'─ Raw Blended ─':^25}│{'─ Calibrated ─':^25}"
    )
    print(
        f"{'Year':6}│ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
        f" │ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
        f" │ {'Sigs':>5} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
    )
    print(f"{'─' * 6}┼{'─' * 25}┼{'─' * 25}┼{'─' * 25}")

    for key, ydata in results.items():
        if key == "TOTAL":
            continue
        _print_row_3(str(key), ydata["elo"], ydata["blended"], ydata["calibrated"])

    print(f"{'─' * 6}┼{'─' * 25}┼{'─' * 25}┼{'─' * 25}")
    _print_row_3("TOTAL", results["TOTAL"]["elo"],
                 results["TOTAL"]["blended"], results["TOTAL"]["calibrated"])

    # Delta summary
    te = results["TOTAL"]["elo"]
    tb = results["TOTAL"]["blended"]
    tc = results["TOTAL"]["calibrated"]
    e_roi = (te["profit"] / te["signals"] * 100) if te["signals"] else 0
    b_roi = (tb["profit"] / tb["signals"] * 100) if tb["signals"] else 0
    c_roi = (tc["profit"] / tc["signals"] * 100) if tc["signals"] else 0
    e_wr = (te["wins"] / te["signals"] * 100) if te["signals"] else 0
    b_wr = (tb["wins"] / tb["signals"] * 100) if tb["signals"] else 0
    c_wr = (tc["wins"] / tc["signals"] * 100) if tc["signals"] else 0

    print(f"\n  vs Elo-Only:")
    print(f"    Blended:    ROI {b_roi-e_roi:+.1f}pp  Win% {b_wr-e_wr:+.1f}pp")
    print(f"    Calibrated: ROI {c_roi-e_roi:+.1f}pp  Win% {c_wr-e_wr:+.1f}pp")
    print()


def _print_row_3(label: str, elo: dict, blend: dict, cal: dict) -> None:
    def _fmt(s: dict) -> str:
        n = s["signals"]
        if n > 0:
            wr = s["wins"] / n * 100
            roi = s["profit"] / n * 100
        else:
            wr = roi = 0.0
        return f" {n:>5} {wr:>4.1f}% {roi:>+6.1f}% {s['profit']:>+6.1f}"

    print(f"{label:6}│{_fmt(elo)} │{_fmt(blend)} │{_fmt(cal)}")


def _print_results_2col(results: dict, threshold: float, odds_col: str, tour: str) -> None:
    header = (
        f"TennisEdge Backtest — Elo-Only vs Blended Model  "
        f"(threshold={threshold * 100:.1f}%, odds={odds_col}, tour={tour})"
    )
    print(f"\n{header}")
    print("=" * len(header))
    print(
        f"{'':6}│{'── Elo-Only ──':^37}│{'── Blended (Elo+Form+H2H+SW) ──':^37}"
    )
    print(
        f"{'Year':6}│ {'Sigs':>5} {'W':>4} {'L':>4} {'Win%':>5} {'ROI%':>7} {'P&L':>7} "
        f"│ {'Sigs':>5} {'W':>4} {'L':>4} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
    )
    print(f"{'─' * 6}┼{'─' * 37}┼{'─' * 37}")

    for key, ydata in results.items():
        if key == "TOTAL":
            continue
        _print_row(str(key), ydata["elo"], ydata["blended"])

    print(f"{'─' * 6}┼{'─' * 37}┼{'─' * 37}")
    _print_row("TOTAL", results["TOTAL"]["elo"], results["TOTAL"]["blended"])

    te = results["TOTAL"]["elo"]
    tb = results["TOTAL"]["blended"]
    elo_roi = (te["profit"] / te["signals"] * 100) if te["signals"] else 0
    blend_roi = (tb["profit"] / tb["signals"] * 100) if tb["signals"] else 0
    delta = blend_roi - elo_roi
    arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "─")
    print(f"\n  Blended vs Elo-Only ROI: {arrow} {delta:+.1f} pp")
    elo_wr = (te["wins"] / te["signals"] * 100) if te["signals"] else 0
    blend_wr = (tb["wins"] / tb["signals"] * 100) if tb["signals"] else 0
    delta_wr = blend_wr - elo_wr
    arrow_wr = "▲" if delta_wr > 0 else ("▼" if delta_wr < 0 else "─")
    print(f"  Blended vs Elo-Only Win%: {arrow_wr} {delta_wr:+.1f} pp")
    print()


def _print_row(label: str, elo: dict, blend: dict) -> None:
    def _fmt(s: dict) -> str:
        sig = s["signals"]
        w = s["wins"]
        lo = s["losses"]
        pnl = s["profit"]
        if sig > 0:
            wr = w / sig * 100
            roi = pnl / sig * 100
        else:
            wr = 0.0
            roi = 0.0
        return f" {sig:>5} {w:>4} {lo:>4} {wr:>4.1f}% {roi:>+6.1f}% {pnl:>+6.1f}"

    print(f"{label:6}│{_fmt(elo)} │{_fmt(blend)}")


# ── CLI entry point ──────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="TennisEdge back-tester — compare Elo-Only vs Blended model ROI.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.07,
        help="Minimum edge to fire a signal (default: 0.07 = 7%%)",
    )
    parser.add_argument(
        "--from-year",
        type=int,
        default=2015,
        help="First year to include (default: 2015)",
    )
    parser.add_argument(
        "--to-year",
        type=int,
        default=2024,
        help="Last year to include (default: 2024)",
    )
    parser.add_argument(
        "--odds-column",
        type=str,
        default="avg",
        choices=["avg", "max", "b365", "ps"],
        help="Which bookmaker odds to use (default: avg)",
    )
    parser.add_argument(
        "--tour",
        type=str,
        default="both",
        choices=["atp", "wta", "both"],
        help="Which tour(s) to include (default: both)",
    )
    parser.add_argument(
        "--calibrated",
        action="store_true",
        help="Apply isotonic calibration to blended probabilities",
    )
    parser.add_argument(
        "--odds-movement",
        action="store_true",
        help="Add movement filter: only fire when B365→Avg shows 'with' or 'neutral' (requires --calibrated)",
    )
    parser.add_argument(
        "--fatigue",
        action="store_true",
        help="Apply fatigue adjustment based on recent match density and round depth (requires --calibrated)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    # --odds-movement and --fatigue imply --calibrated
    if args.odds_movement or args.fatigue:
        args.calibrated = True

    if args.calibrated:
        if not _load_calibration():
            sys.exit(1)

    logger.info(
        "Starting backtest: years %d–%d, threshold=%.2f, odds=%s, tour=%s, "
        "calibrated=%s, movement=%s, fatigue=%s",
        args.from_year,
        args.to_year,
        args.threshold,
        args.odds_column,
        args.tour,
        args.calibrated,
        args.odds_movement,
        args.fatigue,
    )

    results = run_backtest(
        from_year=args.from_year,
        to_year=args.to_year,
        threshold=args.threshold,
        odds_col=args.odds_column,
        tour=args.tour,
        use_calibration=args.calibrated,
        use_movement=args.odds_movement,
        use_fatigue=args.fatigue,
    )

    print_results(results, args.threshold, args.odds_column, args.tour,
                  args.calibrated, args.odds_movement, args.fatigue)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBacktest interrupted by user.", file=sys.stderr)
        sys.exit(130)
