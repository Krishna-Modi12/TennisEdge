"""
signals/calculator.py
Hardened signal calculator with Pinnacle de-vig as primary probability source.

Signal fires ONLY when ALL conditions are met:
  1. Tournament is allowed (tier label or tour keyword in name)
  2. Match is within timing window
  3. Odds are within sanity bounds
  4. Pinnacle margin is valid (1.02–1.07)
  5. Pinnacle odds ratio is not suspicious (≤20x)
  6. Value Edge ≥ MIN_VALUE_EDGE
  7. Model Prob ≥ MIN_MODEL_PROB
  8. Elo agrees (secondary filter)

True Edge Score = Value Edge × Confidence
where Confidence = model_prob / (1 - model_prob)

DO NOT change confidence or true_edge_score formula — backtest validated (+7.28% ROI).
ev_score = value_edge × model_prob is an ADDITIONAL field only.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

from config import DEFAULT_BANKROLL, get_surface
from database.db import get_model_parameter, signal_exists, save_signal
from integrations.tennis_api import search_player
from models.ensemble_model import combine_ensemble_probability
from models.advanced_model import advanced_predict
from models.player_strength import predict_strength_prob
from models.simulator import simulate_match_probability
from ml.predict_model import predict_match_ml_probability

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG CONSTANTS — All thresholds in one place
# ══════════════════════════════════════════════════════════════════════════════

MIN_VALUE_EDGE          = 0.04      # 4% value edge minimum
MIN_MODEL_PROB          = 0.35      # model must give ≥35% win probability
ELO_MAX_DISAGREE        = 0.15      # max gap between Pinnacle & Elo probs
MIN_ODDS                = 1.01      # below this is not valid decimal odds
MAX_ODDS                = 100.0     # above this is API corruption
MAX_ODDS_RATIO          = 20.0      # max ratio between player/opponent odds
PINNACLE_MARGIN_MIN     = 1.02      # tighter valid range for Pinnacle-like two-way markets
PINNACLE_MARGIN_MAX     = 1.07
MAX_SIGNALS_PER_SCAN    = int(os.getenv("MAX_SIGNALS_PER_SCAN", "10"))
MAX_HOURS_BEFORE_MATCH  = int(os.getenv("MAX_HOURS_BEFORE_MATCH", "24"))
CALIBRATION_MODEL_WEIGHT = 0.75
CALIBRATION_MARKET_WEIGHT = 0.25
CALIBRATION_MAX_GAP = 0.25
MC_SIMULATION_RUNS = int(os.getenv("MC_SIMULATION_RUNS", "3000"))
EDGE_BASE_MIN = 0.03
EDGE_BASE_MAX = 0.06
EDGE_THRESHOLD_MAX = 0.08
KELLY_MULT_MIN = 0.25
KELLY_MULT_MAX = 1.25

# ── Tournament filter keywords ───────────────────────────────────────────────

BLOCKED_TIER_KEYWORDS = [
    "m15", "m25", "w15", "w25", "w40", "w60", "w75", "w80", "w100",
    "itf", "futures",
]

ALLOWED_TIER_LABELS = {
    "atp", "wta", "challenger", "masters", "grand slam", "grand_slam",
    "atp 250", "atp 500", "atp 1000",
    "wta 250", "wta 500", "wta 1000",
}

ALLOWED_TOUR_KEYWORDS = [
    # Tour identifiers
    "atp",
    "wta",
    "challenger",
    "masters",

    # Grand Slams
    "grand slam",
    "grand_slam",
    "roland garros",
    "wimbledon",
    "us open",
    "australian open",

    # ATP Masters 1000 events (10 keywords)
    # City names included because some APIs omit the "ATP" prefix
    "indian wells",
    "miami",
    "madrid",
    "rome",
    "montreal",
    "toronto",
    "cincinnati",
    "shanghai",
    "paris",
    "bercy",

    # Major tour events sometimes returned without tier label (10 keywords)
    "dubai",
    "doha",
    "qatar",
    "barcelona",
    "hamburg",
    "washington",
    "beijing",
    "tokyo",
    "vienna",
    "basel",
    "rotterdam",

    # WTA 1000 events (4 keywords)
    "chicago",
    "wuhan",
    "guadalajara",
    "ostrava",
]


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def safe_float(v) -> Optional[float]:
    """Convert value to float safely. Returns None on failure."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def is_allowed_tournament(tournament_name: str, tier: Optional[str] = None) -> bool:
    """
    Returns True if the tournament should receive signal evaluation.

    Logic (in order):
      1. Always check structural block keywords first (m15, m25, etc.)
      2. If API provides a tier label → exact membership check against
         ALLOWED_TIER_LABELS. Unknown tier → REJECT.
      3. If no tier label:
         a. Allow ONLY if name contains a known ALLOWED_TOUR_KEYWORD
         b. If neither blocked nor recognised → REJECT (safe default)

    This prevents unnamed ITF events (Monastir 3, Sharm El Sheikh 4, etc.)
    from slipping through when the API omits the tier field.
    """
    name_lower = tournament_name.lower().strip()

    # Always check structural block keywords first
    for keyword in BLOCKED_TIER_KEYWORDS:
        if keyword in name_lower:
            log.debug("Tournament blocked by keyword '%s': %s",
                      keyword, tournament_name)
            return False

    # Tier label provided — exact match only
    if tier:
        tier_lower = tier.lower().strip()
        if tier_lower in ALLOWED_TIER_LABELS:
            return True
        log.debug("Unknown tier '%s' rejected: %s", tier, tournament_name)
        return False

    # No tier label — require a recognised tour keyword in the name
    for keyword in ALLOWED_TOUR_KEYWORDS:
        if keyword in name_lower:
            return True

    # Name not blocked but also not recognised → reject (safe default)
    log.debug(
        "Tournament '%s' has no tier label and no recognised tour keyword — rejected",
        tournament_name
    )
    return False


def calculate_pinnacle_prob(pin_player: float, pin_opponent: float) -> float:
    """
    Remove Pinnacle overround to get fair probability for the player.
    Validates margin is within expected Pinnacle range before de-vigging.

    Raises:
        ValueError: if odds are <= 1.0 or margin is outside expected range
    """
    if pin_player <= 1.0 or pin_opponent <= 1.0:
        raise ValueError(
            f"Invalid Pinnacle odds: player={pin_player}, "
            f"opponent={pin_opponent}. Both must be > 1.0"
        )

    raw_p  = 1.0 / pin_player
    raw_o  = 1.0 / pin_opponent
    margin = raw_p + raw_o

    if margin < PINNACLE_MARGIN_MIN:
        raise ValueError(
            f"Pinnacle margin {margin:.4f} < {PINNACLE_MARGIN_MIN} — "
            f"too low for a valid Pinnacle market pair. "
            f"player={pin_player}, opponent={pin_opponent}"
        )

    if margin > PINNACLE_MARGIN_MAX:
        raise ValueError(
            f"Pinnacle margin {margin:.4f} > {PINNACLE_MARGIN_MAX} — "
            f"not a Pinnacle odds pair (too high). "
            f"player={pin_player}, opponent={pin_opponent}"
        )

    prob = raw_p / margin
    return max(0.001, min(0.999, prob))


def is_margin_valid(pinnacle_odds_a: float, pinnacle_odds_b: float) -> bool:
    """Return True only when two-way Pinnacle margin is within the safe range."""
    if pinnacle_odds_a is None or pinnacle_odds_b is None:
        return False
    if pinnacle_odds_a <= 1.0 or pinnacle_odds_b <= 1.0:
        return False
    try:
        margin = (1.0 / pinnacle_odds_a) + (1.0 / pinnacle_odds_b)
    except ZeroDivisionError:
        return False
    return PINNACLE_MARGIN_MIN <= margin <= PINNACLE_MARGIN_MAX


def calibrate_probability(model_prob: float, market_fair_prob: float) -> float:
    """
    Calibrate model probability toward market fair probability.
    Default: 75% model + 25% market.
    Safety: when disagreement is extreme (> 0.25), use simple average.
    """
    mp = max(0.001, min(0.999, float(model_prob)))
    mk = max(0.001, min(0.999, float(market_fair_prob)))
    if abs(mp - mk) > CALIBRATION_MAX_GAP:
        return (mp + mk) / 2.0
    return (CALIBRATION_MODEL_WEIGHT * mp) + (CALIBRATION_MARKET_WEIGHT * mk)


def compute_dynamic_edge_threshold(volatility: float) -> float:
    """Volatility-based edge thresholding."""
    try:
        base_low = float(get_model_parameter("dynamic_edge_base", 0.04))
    except Exception:
        base_low = 0.04
    base_low = max(EDGE_BASE_MIN, min(EDGE_BASE_MAX, base_low))
    v = max(0.0, float(volatility or 0.0))
    if v < 0.03:
        threshold = base_low
    elif v <= 0.08:
        threshold = base_low + 0.01
    else:
        threshold = base_low + 0.02
    return min(EDGE_THRESHOLD_MAX, threshold)


def compute_kelly_fraction(probability: float, odds: float) -> float:
    """
    Half-Kelly fraction with safety cap.
    f_full = (b*p - q) / b where b = odds - 1, q = 1-p
    return min(0.05, 0.5 * max(0, f_full))
    """
    p = float(probability or 0.0)
    o = float(odds or 0.0)
    b = o - 1.0
    if b <= 0:
        return 0.0
    q = 1.0 - p
    full_kelly = ((b * p) - q) / b
    if full_kelly <= 0:
        return 0.0
    try:
        kelly_multiplier = float(get_model_parameter("kelly_multiplier", 1.0))
    except Exception:
        kelly_multiplier = 1.0
    kelly_multiplier = max(KELLY_MULT_MIN, min(KELLY_MULT_MAX, kelly_multiplier))
    return min(0.05, (0.5 * full_kelly) * kelly_multiplier)


def _compute_volatility(current_odds: Optional[float], opening_odds: Optional[float]) -> float:
    if not current_odds or not opening_odds or opening_odds <= 0:
        return 0.0
    return abs(float(current_odds) - float(opening_odds)) / float(opening_odds)


def _extract_odds(match: dict, keys: list[str]) -> Optional[float]:
    """
    Try each key in order. Return first valid float within odds bounds.
    Uses safe_float() — never raises on bad API data.
    """
    for key in keys:
        v = safe_float(match.get(key))
        if v is not None and MIN_ODDS <= v <= MAX_ODDS:
            return v
    return None


def is_match_within_window(match: dict) -> bool:
    """
    Returns True if the match starts within MAX_HOURS_BEFORE_MATCH hours.
    Returns True (fails open) if no match time is available — we cannot
    penalise a match for missing timing data.
    """
    match_time = None
    for key in ["match_time", "start_time", "commence_time", "event_time",
                "event_date"]:
        raw = match.get(key)
        if raw:
            try:
                # Handle Unix timestamp (int or float)
                if isinstance(raw, (int, float)):
                    match_time = datetime.fromtimestamp(raw, tz=timezone.utc)
                    break
                # Handle ISO string
                raw_str = str(raw).replace("Z", "+00:00")
                match_time = datetime.fromisoformat(raw_str)
                # Ensure timezone-aware
                if match_time.tzinfo is None:
                    match_time = match_time.replace(tzinfo=timezone.utc)
                break
            except (ValueError, TypeError, OSError):
                continue

    if match_time is None:
        log.debug("No match time found — failing open for timing check")
        return True   # fail open

    now = datetime.now(tz=timezone.utc)
    hours_until_match = (match_time - now).total_seconds() / 3600

    if hours_until_match > MAX_HOURS_BEFORE_MATCH:
        log.debug(
            "Match too far in future (%.1fh > %dh window): %s",
            hours_until_match, MAX_HOURS_BEFORE_MATCH,
            match.get("player_a", match.get("player_name", "unknown"))
        )
        return False

    if hours_until_match < 0:
        log.debug(
            "Match already started or passed (%.1fh ago): %s",
            abs(hours_until_match),
            match.get("player_a", match.get("player_name", "unknown"))
        )
        return False

    return True


# ══════════════════════════════════════════════════════════════════════════════
# CORE SIGNAL CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════

def calculate_signal(match: dict) -> Optional[dict]:
    """
    Evaluate a single match for signal quality on both sides (A and B).
    Returns the best valid enriched match dict if any, None otherwise.
    """
    match_id = match.get("match_id", "unknown")
    if match_id != "unknown" and signal_exists(match_id):
        match["_rejection"] = "duplicate"
        return None

    player_a      = match.get("player_a", "Unknown")
    player_b      = match.get("player_b", "Unknown")
    tournament    = match.get("tournament", "Unknown")
    raw_surface   = str(match.get("surface", "") or "").strip().lower()
    if raw_surface in {"hard", "clay", "grass", "carpet"}:
        surface = raw_surface
    else:
        surface = get_surface(tournament)
    tier          = match.get("tier")

    # ── Guard: Tournament filter ──────────────────────────────────────────
    if not is_allowed_tournament(tournament, tier):
        match["_rejection"] = "tournament"
        return None

    # ── Guard: Match timing window ────────────────────────────────────────
    if not is_match_within_window(match):
        return None

    # ── Extract Pinnacle & Target odds ────────────────────────────────────
    pin_a = _extract_odds(match, ["pinny_odds_a", "PSW", "pinnacle_odds_a"])
    pin_b = _extract_odds(match, ["pinny_odds_b", "PSL", "pinnacle_odds_b"])
    odd_a = _extract_odds(match, ["odds_a", "B365W", "best_odds_a"])
    odd_b = _extract_odds(match, ["odds_b", "B365L", "best_odds_b"])

    if pin_a is None or pin_b is None:
        return None

    open_a = _extract_odds(match, ["opening_odds_a", "open_odds_a", "odds_open_a", "opening_a"])
    open_b = _extract_odds(match, ["opening_odds_b", "open_odds_b", "odds_open_b", "opening_b"])
    vol_a = _compute_volatility(odd_a, open_a)
    vol_b = _compute_volatility(odd_b, open_b)

    # ── Advanced Model Factors ────────────────────────────────────────────
    model_data = advanced_predict(player_a, player_b, surface)

    # Pinnacle margin check
    if not is_margin_valid(pin_a, pin_b):
        return None

    # Ratio check
    if (max(pin_a, pin_b) / min(pin_a, pin_b)) > MAX_ODDS_RATIO:
        return None

    # Market fair probabilities from Pinnacle de-vig.
    market_prob_a = calculate_pinnacle_prob(pin_a, pin_b)
    market_prob_b = 1.0 - market_prob_a

    strength_data = None
    try:
        strength_data = predict_strength_prob(player_a, player_b, surface)
    except Exception:
        strength_data = None

    strength_prob_a = strength_data["prob_a"] if strength_data else model_data.get("strength_prob_a")
    mc_prob_a = None
    if strength_data:
        try:
            mc_prob_a = simulate_match_probability(
                serve_strength_a=strength_data["serve_a"],
                return_strength_a=strength_data["return_a"],
                serve_strength_b=strength_data["serve_b"],
                return_strength_b=strength_data["return_b"],
                simulations=MC_SIMULATION_RUNS,
            )
        except Exception:
            mc_prob_a = None

    ml_prob_a = None
    try:
        ml_prob_a = predict_match_ml_probability(
            {
                "surface": surface,
                "model_prob": model_data.get("prob_a"),
                "market_prob": market_prob_a,
                "calibrated_prob": model_data.get("prob_a"),
                "raw_model_prob": model_data.get("prob_a"),
                "edge": ((model_data.get("prob_a", 0.5) * odd_a) - 1.0) if odd_a else 0.0,
                "odds": odd_a if odd_a else 2.0,
                "volatility": vol_a,
                "edge_threshold": compute_dynamic_edge_threshold(vol_a),
                "kelly_fraction": compute_kelly_fraction(model_data.get("prob_a", 0.5), odd_a or 2.0),
                "elo_component_prob": model_data.get("elo_prob_a"),
                "strength_component_prob": strength_prob_a,
                "mc_component_prob": mc_prob_a,
                "market_component_prob": market_prob_a,
                "ensemble_prob": model_data.get("prob_a"),
            }
        )
    except Exception:
        ml_prob_a = None

    raw_model_prob_a, ensemble_payload = combine_ensemble_probability(
        elo_prob=model_data.get("elo_prob_a", model_data["prob_a"]),
        strength_prob=strength_prob_a,
        mc_prob=mc_prob_a,
        market_prob=market_prob_a,
        ml_prob=ml_prob_a,
    )
    raw_model_prob_b = 1.0 - raw_model_prob_a
    calibrated_prob_a = calibrate_probability(raw_model_prob_a, market_prob_a)
    calibrated_prob_b = calibrate_probability(raw_model_prob_b, market_prob_b)

    # ── Evaluate Sides ────────────────────────────────────────────────────
    def get_side_signal(prob, raw_prob, market_prob, odds, player_name, side_id, volatility, components):
        if not odds or odds < MIN_ODDS: return None
        edge_threshold = compute_dynamic_edge_threshold(volatility)
        value_edge = (prob * odds) - 1.0
        if value_edge < edge_threshold: return None
        if prob < MIN_MODEL_PROB: return None
        
        # Elo check (using model factors)
        f_elo = model_data.get("elo_prob_a") if side_id == "a" else (1.0 - model_data.get("elo_prob_a", 0.5))
        if f_elo is not None:
            if abs(prob - f_elo) > ELO_MAX_DISAGREE: return None
            # Direction check
            if (prob > 0.5 and f_elo < 0.5) or (prob < 0.5 and f_elo > 0.5):
                return None
        
        confidence = prob / (1.0 - prob) if prob < 1.0 else 99.0
        true_edge = value_edge * confidence
        return {
            "bet_on": player_name,
            "odds": odds,
            "model_prob": prob,
            "raw_model_prob": raw_prob,
            "calibrated_prob": prob,
            "value_edge": value_edge,
            "true_edge_score": true_edge,
            "market_prob": market_prob,
            "volatility": volatility,
            "edge_threshold": edge_threshold,
            "elo_component_prob": components.get("elo_prob"),
            "strength_component_prob": components.get("strength_prob"),
            "mc_component_prob": components.get("mc_prob"),
            "market_component_prob": components.get("market_prob"),
            "ml_component_prob": components.get("ml_prob"),
            "ensemble_prob": components.get("ensemble_prob"),
        }

    prob_a = calibrated_prob_a
    prob_b = calibrated_prob_b

    sig_a = get_side_signal(
        prob_a, raw_model_prob_a, market_prob_a, odd_a, player_a, "a", vol_a, ensemble_payload
    )
    components_b = {
        "elo_prob": 1.0 - ensemble_payload.get("elo_prob", 0.5),
        "strength_prob": (1.0 - ensemble_payload["strength_prob"]) if ensemble_payload.get("strength_prob") is not None else None,
        "mc_prob": (1.0 - ensemble_payload["mc_prob"]) if ensemble_payload.get("mc_prob") is not None else None,
        "market_prob": 1.0 - ensemble_payload.get("market_prob", 0.5),
        "ml_prob": (1.0 - ensemble_payload["ml_prob"]) if ensemble_payload.get("ml_prob") is not None else None,
        "ensemble_prob": 1.0 - ensemble_payload.get("ensemble_prob", 0.5),
    }
    sig_b = get_side_signal(
        prob_b, raw_model_prob_b, market_prob_b, odd_b, player_b, "b", vol_b, components_b
    )

    best = None
    if sig_a and sig_b:
        best = sig_a if sig_a["true_edge_score"] > sig_b["true_edge_score"] else sig_b
    elif sig_a:
        best = sig_a
    elif sig_b:
        best = sig_b

    if not best:
        return None

    # Match factors to formatter expectations
    is_a = (best["bet_on"] == player_a)
    factors = {
        "elo_prob":     model_data.get("elo_prob_a") if is_a else (1.0 - model_data.get("elo_prob_a", 0.5)),
        "form_prob":    model_data.get("form_prob_a") if is_a else (1.0 - model_data.get("form_prob_a", 0.5)),
        "surface_prob": model_data.get("surface_prob_a") if is_a else (1.0 - model_data.get("surface_prob_a", 0.5)),
        "h2h_prob":     model_data.get("h2h_prob_a") if is_a else (1.0 - model_data.get("h2h_prob_a", 0.5)),
        "strength_prob": model_data.get("strength_prob_a") if is_a else (1.0 - model_data.get("strength_prob_a", 0.5)),
        "h2h_wins_a":   model_data.get("h2h_wins_a", 0),
        "h2h_wins_b":   model_data.get("h2h_wins_b", 0),
        "data_quality": model_data.get("data_quality", "elo_only")
    }

    # enriched result
    res = {
        **match,
        **best,
        **factors,
        "edge": best["true_edge_score"],
        "ev_score": round(best["value_edge"] * best["model_prob"], 4),
        "confidence": best["true_edge_score"] / best["value_edge"]
                      if best["value_edge"] > 0 else 1.0,
        "probability_source": "pinnacle_devig",
        "bankroll_assumed": DEFAULT_BANKROLL,
        "recommended_bet_fraction": round(compute_kelly_fraction(best["model_prob"], best["odds"]), 4),
    }
    res["recommended_bet_size"] = round(res["bankroll_assumed"] * res["recommended_bet_fraction"], 2)
    
    log.info("SIGNAL [%s] %s @ %.2f | Edge: %.1f%%", match_id, best["bet_on"], best["odds"], best["true_edge_score"]*100)
    return res


# ══════════════════════════════════════════════════════════════════════════════
# BATCH PROCESSING WITH RATE PROTECTION & METRICS
# ══════════════════════════════════════════════════════════════════════════════

def process_matches(matches: list[dict]) -> list[dict]:
    """
    Process a batch of matches through the signal calculator.
    Includes pre-resolution, rate protection, and DB persistence.
    """
    # 1. Pre-resolve players for aliases
    players = set()
    for m in matches:
        if m.get("player_a"): players.add(m["player_a"])
        if m.get("player_b"): players.add(m["player_b"])
    
    if players:
        log.info("Pre-resolving %d players...", len(players))
        for p_name in players:
            try:
                search_player(p_name)
            except Exception:
                continue

    signals = []
    rejection_counts = {
        "tournament":   0,
        "edge":         0,
        "prob":         0,
        "invalid_odds": 0,
        "elo":          0,
        "timing":       0,
        "total":        0,
    }

    for i, match in enumerate(matches):
        if len(signals) >= MAX_SIGNALS_PER_SCAN:
            log.warning("MAX_SIGNALS_PER_SCAN reached — stopping scan.")
            break

        result = calculate_signal(match)
        if result:
            # Save to DB before returning
            try:
                save_signal(
                    match_id=result["match_id"],
                    tournament=result["tournament"],
                    surface=result["surface"],
                    player_a=result["player_a"],
                    player_b=result["player_b"],
                    bet_on=result["bet_on"],
                    model_prob=result["model_prob"],
                    market_prob=result["market_prob"],
                    calibrated_prob=result.get("calibrated_prob"),
                    raw_model_prob=result.get("raw_model_prob"),
                    elo_component_prob=result.get("elo_component_prob"),
                    strength_component_prob=result.get("strength_component_prob"),
                    mc_component_prob=result.get("mc_component_prob"),
                    market_component_prob=result.get("market_component_prob"),
                    ml_component_prob=result.get("ml_component_prob"),
                    ensemble_prob=result.get("ensemble_prob"),
                    edge=result["true_edge_score"],
                    odds=result["odds"],
                    volatility=result.get("volatility"),
                    edge_threshold=result.get("edge_threshold"),
                    kelly_fraction=result.get("recommended_bet_fraction"),
                    recommended_bet_size=result.get("recommended_bet_size"),
                )
            except Exception as e:
                log.warning("save_signal skipped for %s: %s",
                            result.get("match_id"), e)
            signals.append(result)
        else:
            reason = match.get("_rejection", "unknown")
            if reason in rejection_counts:
                rejection_counts[reason] += 1
            rejection_counts["total"] += 1

    return signals, rejection_counts
