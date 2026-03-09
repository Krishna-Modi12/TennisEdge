"""
models/calibration.py — Probability calibration for TennisEdge.

Diagnoses and corrects overconfidence in model probabilities using
isotonic regression fitted on historical data.

Workflow:
    1. Replay matches chronologically, collecting (predicted_prob, outcome)
    2. Fit IsotonicRegression on 2015–2019 (training set)
    3. Evaluate on 2020–2024 (out-of-sample)
    4. Export calibrate(raw_prob) for live use
    5. Plot calibration curve + re-run backtest raw vs calibrated

Usage:
    cd tennisedge
    python -m models.calibration                          # full run
    python -m models.calibration --plot-only              # just the plot
    python -m models.calibration --threshold 0.07         # backtest at 7%
"""

import argparse
import io
import json
import logging
import math
import os
import pickle
import sys
from collections import defaultdict, deque

import numpy as np
import pandas as pd
import requests
from sklearn.isotonic import IsotonicRegression

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CALIBRATION_PATH = os.path.join(BASE_DIR, "models", "calibration_model.pkl")

# ── Constants (same as backtest.py) ───────────────────────────────────────────

ELO_DEFAULT = 1500.0
ELO_K = 32
SURFACE_W = 0.60
MIN_ODDS = 1.40
MAX_ODDS = 6.00

W_ELO = 0.35
W_FORM = 0.25
W_H2H = 0.20
W_SURFACE = 0.20
FORM_DECAY = 0.9
FORM_N = 10
MIN_H2H = 3
SW_YEARS = 2
NEUTRAL = 0.5

BASE_URL = "http://www.tennis-data.co.uk"
SURFACE_MAP = {"hard": "hard", "clay": "clay", "grass": "grass",
               "carpet": "carpet", "indoor": "hard", "outdoor": "hard"}

# ── In-memory Elo engine ─────────────────────────────────────────────────────

_elo: dict[tuple[str, str], float] = {}


def _get(p, s):
    return _elo.get((p, s), ELO_DEFAULT)


def _winp(ra, rb):
    return 1.0 / (1.0 + math.pow(10, (rb - ra) / 400.0))


def _predict_elo(pa, pb, surface):
    ea = SURFACE_W * _get(pa, surface) + (1 - SURFACE_W) * _get(pa, "overall")
    eb = SURFACE_W * _get(pb, surface) + (1 - SURFACE_W) * _get(pb, "overall")
    return _winp(ea, eb)


def _update_elo(winner, loser, surface):
    for s in ["overall", surface]:
        ra, rb = _get(winner, s), _get(loser, s)
        ea = _winp(ra, rb)
        _elo[(winner, s)] = ra + ELO_K * (1 - ea)
        _elo[(loser, s)] = rb + ELO_K * (0 - (1 - ea))


# ── In-memory model components (same as backtest.py) ─────────────────────────

_player_matches: dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
_h2h_records: dict[tuple[str, str, str], list] = defaultdict(list)
_surface_records: dict[tuple[str, str], deque] = defaultdict(lambda: deque(maxlen=500))


def _record(winner, loser, surface, date_str):
    _player_matches[winner].append((winner, surface, date_str))
    _player_matches[loser].append((winner, surface, date_str))
    a, b = (winner, loser) if winner <= loser else (loser, winner)
    _h2h_records[(a, b, surface)].append(winner)
    _surface_records[(winner, surface)].append((date_str, True))
    _surface_records[(loser, surface)].append((date_str, False))


def _form(player, surface):
    matches = list(_player_matches[player])
    if not matches:
        return NEUTRAL
    recent = matches[-FORM_N:]
    recent.reverse()
    ww, wt = 0.0, 0.0
    for i, (w, ms, _) in enumerate(recent):
        weight = FORM_DECAY ** i + (0.05 if ms == surface else 0)
        wt += weight
        if w == player:
            ww += weight
    return (ww / wt) if wt > 0 else NEUTRAL


def _h2h(p1, p2, surface):
    a, b = (p1, p2) if p1 <= p2 else (p2, p1)
    recs = _h2h_records.get((a, b, surface), [])
    if len(recs) < MIN_H2H:
        return NEUTRAL
    return sum(1 for w in recs if w == p1) / len(recs)


def _sw(player, surface, cur_date):
    recs = _surface_records.get((player, surface))
    if not recs:
        return NEUTRAL
    try:
        cutoff = f"{int(cur_date[:4]) - SW_YEARS}{cur_date[4:]}"
    except Exception:
        return NEUTRAL
    total = sum(1 for d, _ in recs if d >= cutoff)
    wins = sum(1 for d, w in recs if d >= cutoff and w)
    return (wins / total) if total > 0 else NEUTRAL


def _blended_prob(pa, pb, surface, elo_p, cur_date):
    f_a, f_b = _form(pa, surface), _form(pb, surface)
    ft = f_a + f_b
    fp = (f_a / ft) if ft > 0 else NEUTRAL

    hp = _h2h(pa, pb, surface)

    s_a, s_b = _sw(pa, surface, cur_date), _sw(pb, surface, cur_date)
    st = s_a + s_b
    sp = (s_a / st) if st > 0 else NEUTRAL

    return max(0.01, min(0.99,
        W_ELO * elo_p + W_FORM * fp + W_H2H * hp + W_SURFACE * sp))


# ── Data download ─────────────────────────────────────────────────────────────

def _norm_surface(raw):
    if not raw or pd.isna(raw):
        return "hard"
    return SURFACE_MAP.get(str(raw).strip().lower(), "hard")


def _download(year, tour="atp"):
    ext = "xlsx" if year >= 2013 else "xls"
    suffix = "w" if tour == "wta" else ""
    url = f"{BASE_URL}/{year}{suffix}/{year}.{ext}"
    logger.info("Downloading %s", url)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logger.warning("Download failed %s %d: %s", tour, year, e)
        return None
    for engine in ("openpyxl", "xlrd"):
        try:
            return pd.read_excel(io.BytesIO(resp.content), engine=engine)
        except Exception:
            continue
    return None


def _normalize_name(name):
    """Minimal name normalizer (mirrors utils.normalize_player_name)."""
    if not name or not isinstance(name, str):
        return ""
    name = " ".join(name.split()).strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"
    return name.strip().title()


# ── Phase 1: Collect (predicted_prob, actual_outcome) pairs ───────────────────

def collect_predictions(from_year=2015, to_year=2024, tour="atp"):
    """Replay history chronologically, returning list of
    (year, pred_prob_winner, blended_prob_winner, actual=1) tuples.

    We always predict for the "winner" column (who actually won),
    so actual outcome is always 1. To get calibration pairs for both
    directions, we also add the loser's predicted prob with outcome=0.
    """
    pairs = []  # (year, elo_prob, blended_prob, outcome)

    for year in range(from_year, to_year + 1):
        df = _download(year, tour)
        if df is None:
            continue

        df.columns = [str(c).strip().lower() for c in df.columns]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date", na_position="last").reset_index(drop=True)

        count = 0
        for _, row in df.iterrows():
            rw, rl = row.get("winner"), row.get("loser")
            if pd.isna(rw) or pd.isna(rl):
                continue

            winner = _normalize_name(str(rw).strip())
            loser = _normalize_name(str(rl).strip())
            if not winner or not loser:
                continue

            surface = _norm_surface(row.get("surface"))
            dv = row.get("date")
            cur_date = pd.Timestamp(dv).strftime("%Y-%m-%d") if pd.notna(dv) else f"{year}-06-15"

            # Predict BEFORE update
            elo_p = _predict_elo(winner, loser, surface)
            blend_p = _blended_prob(winner, loser, surface, elo_p, cur_date)

            # Winner outcome=1 → (prob_for_winner, 1)
            pairs.append((year, elo_p, blend_p, 1))
            # Loser outcome=0 → (prob_for_winner, 0) viewed from loser
            pairs.append((year, 1 - elo_p, 1 - blend_p, 1))
            # (We flip it: the loser's predicted prob = 1-elo_p, and they also won
            #  their implied side — wait, that's wrong. Let me think.)
            # Actually: we want calibration pairs in both directions.
            # For the winner: predicted=blend_p, outcome=1
            # For the loser: predicted=1-blend_p, outcome=0
            # But since isotonic needs (pred, outcome), we can just do:
            pairs[-1] = (year, 1 - elo_p, 1 - blend_p, 0)

            # Update AFTER prediction
            _update_elo(winner, loser, surface)
            _record(winner, loser, surface, cur_date)
            count += 1

        logger.info("Year %d: %d matches processed (%d pairs)", year, count, count * 2)

    return pairs


# ── Phase 2: Fit calibration ─────────────────────────────────────────────────

def fit_calibration(pairs, train_years=(2015, 2019)):
    """Fit IsotonicRegression on train years, return model + split data."""
    train = [(e, b, o) for (y, e, b, o) in pairs
             if train_years[0] <= y <= train_years[1]]
    test = [(e, b, o) for (y, e, b, o) in pairs
            if y > train_years[1]]

    if not train:
        raise ValueError("No training data found")

    X_train = np.array([b for _, b, o in train])
    y_train = np.array([o for _, b, o in train])
    X_test = np.array([b for _, b, o in test])
    y_test = np.array([o for _, b, o in test])

    # Also collect elo-only for comparison
    X_train_elo = np.array([e for e, _, o in train])
    X_test_elo = np.array([e for e, _, o in test])

    iso = IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds="clip")
    iso.fit(X_train, y_train)

    # Also fit one for elo-only
    iso_elo = IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds="clip")
    iso_elo.fit(X_train_elo, y_train)

    return {
        "iso_blend": iso,
        "iso_elo": iso_elo,
        "train": {"X_blend": X_train, "X_elo": X_train_elo, "y": y_train},
        "test": {"X_blend": X_test, "X_elo": X_test_elo, "y": y_test},
    }


def save_model(iso_model):
    """Persist the calibration model for live use."""
    with open(CALIBRATION_PATH, "wb") as f:
        pickle.dump(iso_model, f)
    logger.info("Saved calibration model → %s", CALIBRATION_PATH)


def load_model():
    """Load persisted calibration model."""
    with open(CALIBRATION_PATH, "rb") as f:
        return pickle.load(f)


# ── Exported calibrate() function ─────────────────────────────────────────────

_cached_model = None


def calibrate(raw_prob: float) -> float:
    """Adjust a raw blended probability using the fitted isotonic model.

    Loads the model lazily from disk on first call.
    Returns raw_prob unchanged if no model file exists.
    """
    global _cached_model
    if _cached_model is None:
        if not os.path.exists(CALIBRATION_PATH):
            return raw_prob
        _cached_model = load_model()

    return float(_cached_model.predict([raw_prob])[0])


# ── Phase 3: Diagnostics ─────────────────────────────────────────────────────

def calibration_table(X, y, label="", n_bins=10):
    """Print a calibration table: predicted decile vs actual win rate."""
    bins = np.linspace(0, 1, n_bins + 1)
    print(f"\n{'─'*60}")
    print(f"  Calibration Table: {label}")
    print(f"{'─'*60}")
    print(f"  {'Pred Range':>14}  {'Count':>6}  {'Avg Pred':>8}  {'Actual':>8}  {'Gap':>8}")
    print(f"  {'─'*14}  {'─'*6}  {'─'*8}  {'─'*8}  {'─'*8}")

    for i in range(n_bins):
        lo, hi = bins[i], bins[i + 1]
        mask = (X >= lo) & (X < hi) if i < n_bins - 1 else (X >= lo) & (X <= hi)
        if mask.sum() == 0:
            continue
        avg_pred = X[mask].mean()
        actual_rate = y[mask].mean()
        gap = actual_rate - avg_pred
        arrow = "▲" if gap > 0.02 else ("▼" if gap < -0.02 else "≈")
        print(f"  {lo:.1f}–{hi:.1f}      {mask.sum():>6}  {avg_pred:>7.3f}   {actual_rate:>7.3f}  {gap:>+7.3f} {arrow}")

    # Overall
    print(f"  {'─'*14}  {'─'*6}  {'─'*8}  {'─'*8}  {'─'*8}")
    print(f"  {'OVERALL':>14}  {len(X):>6}  {X.mean():>7.3f}   {y.mean():>7.3f}  {y.mean()-X.mean():>+7.3f}")


def brier_score(X, y):
    """Mean squared error between predicted probs and outcomes."""
    return float(np.mean((X - y) ** 2))


def plot_calibration(fit_data, save_path=None):
    """Generate a calibration curve plot."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for ax_idx, (split, title) in enumerate([("train", "Train 2015–2019"), ("test", "Test 2020–2024")]):
        ax = axes[ax_idx]
        d = fit_data[split]
        n_bins = 10
        bins = np.linspace(0, 1, n_bins + 1)

        for X, label, color, marker in [
            (d["X_blend"], "Raw Blended", "#e74c3c", "o"),
        ]:
            # Calibrated version
            iso = fit_data["iso_blend"]
            X_cal = iso.predict(X)

            bin_pred_raw, bin_actual_raw = [], []
            bin_pred_cal, bin_actual_cal = [], []

            for i in range(n_bins):
                lo, hi = bins[i], bins[i + 1]
                mask = (X >= lo) & (X < hi) if i < n_bins - 1 else (X >= lo) & (X <= hi)
                if mask.sum() < 20:
                    continue
                bin_pred_raw.append(X[mask].mean())
                bin_actual_raw.append(d["y"][mask].mean())
                bin_pred_cal.append(X_cal[mask].mean())
                bin_actual_cal.append(d["y"][mask].mean())

            ax.plot(bin_pred_raw, bin_actual_raw, f"{marker}--",
                    color=color, label=f"Raw Blended", markersize=8, alpha=0.8)
            ax.plot(bin_pred_cal, bin_actual_cal, f"{marker}-",
                    color="#2ecc71", label=f"Calibrated", markersize=8, alpha=0.8)

        # Elo-only for reference
        X_elo = d["X_elo"]
        bin_pred_elo, bin_actual_elo = [], []
        for i in range(n_bins):
            lo, hi = bins[i], bins[i + 1]
            mask = (X_elo >= lo) & (X_elo < hi) if i < n_bins - 1 else (X_elo >= lo) & (X_elo <= hi)
            if mask.sum() < 20:
                continue
            bin_pred_elo.append(X_elo[mask].mean())
            bin_actual_elo.append(d["y"][mask].mean())
        ax.plot(bin_pred_elo, bin_actual_elo, "s:",
                color="#3498db", label="Elo-Only", markersize=7, alpha=0.7)

        ax.plot([0, 1], [0, 1], "k--", alpha=0.3, label="Perfect")
        ax.set_xlabel("Predicted Probability")
        ax.set_ylabel("Actual Win Rate")
        ax.set_title(title)
        ax.legend(loc="upper left", fontsize=9)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.grid(alpha=0.2)

    plt.suptitle("TennisEdge Model Calibration", fontsize=14, fontweight="bold")
    plt.tight_layout()

    out = save_path or os.path.join(BASE_DIR, "data", "calibration_curve.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Calibration plot saved → %s", out)
    return out


# ── Phase 4: Calibrated backtest ──────────────────────────────────────────────

def _backtest_with_calibration(pairs, iso_model, threshold=0.07,
                               test_years=(2020, 2024)):
    """Re-run signal evaluation on test-set pairs, comparing raw vs calibrated."""
    # We need odds data too — re-download test years and replay
    # But pairs don't carry odds. Instead, do a fresh replay.
    pass  # Handled by the full replay below


def replay_backtest_calibrated(from_year=2020, to_year=2024,
                                threshold=0.07, iso_model=None, tour="atp"):
    """Fresh chronological replay with calibration applied.
    
    Resets Elo/history state — call collect_predictions(2015, to_year-1) first
    to warm up the model, or accept cold-start for simplicity.
    
    Returns dict with 'raw' and 'calibrated' stats per year.
    """

    # We need a SEPARATE pass since the global state was already consumed.
    # Reset everything.
    global _elo, _player_matches, _h2h_records, _surface_records
    _elo = {}
    _player_matches = defaultdict(lambda: deque(maxlen=200))
    _h2h_records = defaultdict(list)
    _surface_records = defaultdict(lambda: deque(maxlen=500))

    # Warm up on years before from_year
    logger.info("Warming up model on 2015–%d ...", from_year - 1)
    for year in range(2015, from_year):
        df = _download(year, tour)
        if df is None:
            continue
        df.columns = [str(c).strip().lower() for c in df.columns]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date", na_position="last").reset_index(drop=True)
        for _, row in df.iterrows():
            rw, rl = row.get("winner"), row.get("loser")
            if pd.isna(rw) or pd.isna(rl):
                continue
            w = _normalize_name(str(rw).strip())
            l = _normalize_name(str(rl).strip())
            if not w or not l:
                continue
            s = _norm_surface(row.get("surface"))
            dv = row.get("date")
            d = pd.Timestamp(dv).strftime("%Y-%m-%d") if pd.notna(dv) else f"{year}-06-15"
            _update_elo(w, l, s)
            _record(w, l, s, d)

    # Now backtest on test years
    results = {}

    def mk():
        return {"signals": 0, "wins": 0, "losses": 0, "profit": 0.0, "edge_sum": 0.0}

    for year in range(from_year, to_year + 1):
        df = _download(year, tour)
        if df is None:
            results[year] = {"raw": mk(), "calibrated": mk()}
            continue

        df.columns = [str(c).strip().lower() for c in df.columns]

        # Find odds columns (try avg, then b365, then ps)
        odds_w_col, odds_l_col = None, None
        for wc, lc in [("avgw", "avgl"), ("b365w", "b365l"), ("psw", "psl")]:
            if wc in df.columns and lc in df.columns:
                odds_w_col, odds_l_col = wc, lc
                break

        if odds_w_col is None:
            results[year] = {"raw": mk(), "calibrated": mk()}
            continue

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date", na_position="last").reset_index(drop=True)

        raw_s = mk()
        cal_s = mk()

        for _, row in df.iterrows():
            rw, rl = row.get("winner"), row.get("loser")
            if pd.isna(rw) or pd.isna(rl):
                continue

            winner = _normalize_name(str(rw).strip())
            loser = _normalize_name(str(rl).strip())
            if not winner or not loser:
                continue

            surface = _norm_surface(row.get("surface"))
            dv = row.get("date")
            cur_date = pd.Timestamp(dv).strftime("%Y-%m-%d") if pd.notna(dv) else f"{year}-06-15"

            ow = row.get(odds_w_col)
            ol = row.get(odds_l_col)
            if pd.isna(ow) or pd.isna(ol):
                _update_elo(winner, loser, surface)
                _record(winner, loser, surface, cur_date)
                continue
            try:
                ow, ol = float(ow), float(ol)
            except (ValueError, TypeError):
                _update_elo(winner, loser, surface)
                _record(winner, loser, surface, cur_date)
                continue
            if ow <= 0 or ol <= 0:
                _update_elo(winner, loser, surface)
                _record(winner, loser, surface, cur_date)
                continue

            # Predict BEFORE update
            elo_p = _predict_elo(winner, loser, surface)
            raw_p = _blended_prob(winner, loser, surface, elo_p, cur_date)
            cal_p = float(iso_model.predict([raw_p])[0]) if iso_model else raw_p

            market_w = 1.0 / ow
            market_l = 1.0 / ol

            # Raw model signals
            for stats, prob_w in [(raw_s, raw_p), (cal_s, cal_p)]:
                prob_l = 1.0 - prob_w
                edge_w = prob_w - market_w
                edge_l = prob_l - market_l

                best_edge, bow, bodds = None, None, None
                if edge_w >= threshold and MIN_ODDS <= ow <= MAX_ODDS:
                    best_edge, bow, bodds = edge_w, True, ow
                if edge_l >= threshold and MIN_ODDS <= ol <= MAX_ODDS:
                    if best_edge is None or edge_l > best_edge:
                        best_edge, bow, bodds = edge_l, False, ol

                if best_edge is not None:
                    stats["signals"] += 1
                    stats["edge_sum"] += best_edge
                    if bow:  # bet on winner column (actual winner)
                        stats["wins"] += 1
                        stats["profit"] += (bodds - 1.0)
                    else:
                        stats["losses"] += 1
                        stats["profit"] -= 1.0

            # Update AFTER
            _update_elo(winner, loser, surface)
            _record(winner, loser, surface, cur_date)

        raw_s["profit"] = round(raw_s["profit"], 2)
        cal_s["profit"] = round(cal_s["profit"], 2)
        results[year] = {"raw": raw_s, "calibrated": cal_s}
        logger.info("Year %d: raw=%d sigs (%.1f P&L) | cal=%d sigs (%.1f P&L)",
                     year, raw_s["signals"], raw_s["profit"],
                     cal_s["signals"], cal_s["profit"])

    # Totals
    total_raw, total_cal = mk(), mk()
    for ydata in results.values():
        for k in ("signals", "wins", "losses", "profit", "edge_sum"):
            total_raw[k] += ydata["raw"][k]
            total_cal[k] += ydata["calibrated"][k]
    total_raw["profit"] = round(total_raw["profit"], 2)
    total_cal["profit"] = round(total_cal["profit"], 2)
    results["TOTAL"] = {"raw": total_raw, "calibrated": total_cal}

    return results


def print_backtest(results, threshold):
    """Side-by-side raw vs calibrated results."""
    print(f"\n{'='*80}")
    print(f"  Backtest: Raw Blended vs Calibrated  (threshold={threshold*100:.0f}%)")
    print(f"{'='*80}")
    print(f"{'':6}│{'── Raw Blended ──':^30}│{'── Calibrated ──':^30}")
    print(f"{'Year':6}│ {'Sigs':>5} {'W':>4} {'L':>4} {'Win%':>5} {'ROI%':>7} {'P&L':>7}"
          f" │ {'Sigs':>5} {'W':>4} {'L':>4} {'Win%':>5} {'ROI%':>7} {'P&L':>7}")
    print(f"{'─'*6}┼{'─'*30}┼{'─'*30}")

    for key, ydata in results.items():
        if key == "TOTAL":
            continue
        _pr(str(key), ydata["raw"], ydata["calibrated"])

    print(f"{'─'*6}┼{'─'*30}┼{'─'*30}")
    _pr("TOTAL", results["TOTAL"]["raw"], results["TOTAL"]["calibrated"])

    tr = results["TOTAL"]["raw"]
    tc = results["TOTAL"]["calibrated"]
    r_roi = (tr["profit"] / tr["signals"] * 100) if tr["signals"] else 0
    c_roi = (tc["profit"] / tc["signals"] * 100) if tc["signals"] else 0
    r_wr = (tr["wins"] / tr["signals"] * 100) if tr["signals"] else 0
    c_wr = (tc["wins"] / tc["signals"] * 100) if tc["signals"] else 0
    d_roi = c_roi - r_roi
    d_wr = c_wr - r_wr
    print(f"\n  ROI:  {d_roi:+.1f} pp  ({'▲' if d_roi > 0 else '▼'})")
    print(f"  Win%: {d_wr:+.1f} pp  ({'▲' if d_wr > 0 else '▼'})")
    print()


def _pr(label, raw, cal):
    def fmt(s):
        n = s["signals"]
        if n > 0:
            wr = s["wins"] / n * 100
            roi = s["profit"] / n * 100
        else:
            wr, roi = 0, 0
        return f" {n:>5} {s['wins']:>4} {s['losses']:>4} {wr:>4.1f}% {roi:>+6.1f}% {s['profit']:>+6.1f}"
    print(f"{label:6}│{fmt(raw)} │{fmt(cal)}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TennisEdge probability calibration")
    parser.add_argument("--threshold", type=float, default=0.07)
    parser.add_argument("--tour", default="atp", choices=["atp", "wta", "both"])
    parser.add_argument("--plot-only", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        stream=sys.stderr)

    if args.plot_only and os.path.exists(CALIBRATION_PATH):
        logger.info("Loading existing model for plot-only mode")
        # Would need cached pairs — skip for now
        print("Use full run (without --plot-only) to generate fresh data + plot.")
        return

    # Phase 1: Collect predictions
    logger.info("Phase 1: Collecting predictions 2015–2024 ...")
    pairs = collect_predictions(2015, 2024, tour=args.tour)
    logger.info("Collected %d prediction pairs", len(pairs))

    # Phase 2: Fit calibration
    logger.info("Phase 2: Fitting isotonic regression (train=2015–2019) ...")
    fit_data = fit_calibration(pairs, train_years=(2015, 2019))

    iso = fit_data["iso_blend"]
    save_model(iso)

    # Phase 3: Diagnostics
    print("\n" + "=" * 60)
    print("  CALIBRATION DIAGNOSTICS")
    print("=" * 60)

    for split, label in [("train", "TRAIN 2015–2019"), ("test", "TEST 2020–2024 (out-of-sample)")]:
        d = fit_data[split]
        calibration_table(d["X_blend"], d["y"], f"Raw Blended — {label}")
        X_cal = iso.predict(d["X_blend"])
        calibration_table(X_cal, d["y"], f"Calibrated — {label}")

        bs_raw = brier_score(d["X_blend"], d["y"])
        bs_cal = brier_score(X_cal, d["y"])
        bs_elo = brier_score(d["X_elo"], d["y"])
        print(f"\n  Brier Score ({label}):")
        print(f"    Elo-Only:   {bs_elo:.4f}")
        print(f"    Raw Blend:  {bs_raw:.4f}")
        print(f"    Calibrated: {bs_cal:.4f}  ({(bs_cal-bs_raw)/bs_raw*100:+.1f}%)")

    # Plot
    plot_path = plot_calibration(fit_data)
    print(f"\n  📊 Plot saved: {plot_path}")

    # Phase 4: Calibrated backtest
    logger.info("Phase 4: Re-running backtest with calibration ...")
    bt_results = replay_backtest_calibrated(
        from_year=2020, to_year=2024,
        threshold=args.threshold, iso_model=iso, tour=args.tour,
    )
    print_backtest(bt_results, args.threshold)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
