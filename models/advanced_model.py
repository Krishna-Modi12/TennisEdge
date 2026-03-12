"""
models/advanced_model.py
4-Factor blended probability model:
    40% Surface-Adjusted Elo
    25% Recent Form (weighted win rate, last 20 matches)
    20% Surface Win Rate (last 2 years)
    15% Head-to-Head on surface (all time)

Graceful fallback: if stats are missing, falls back to pure Elo.
"""

from models.elo_model import predict as elo_predict
from database.db import get_player_stats, get_h2h_record
from models.player_strength import predict_strength_prob

# Minimum sample sizes to avoid small-sample distortion
MIN_FORM_MATCHES = 5
MIN_SURFACE_MATCHES = 10
MIN_H2H_MATCHES = 3

# Weights
W_ELO = 0.40
W_FORM = 0.25
W_SURFACE = 0.20
W_H2H = 0.15


def advanced_predict(player_a: str, player_b: str, surface: str) -> dict:
    """
    Returns blended probability using 4 factors.
    Always returns at minimum prob_a and prob_b (compatible with elo predict).
    """
    # Factor 1: Elo (always available)
    elo = elo_predict(player_a, player_b, surface)
    elo_prob_a = elo["prob_a"]

    # Factor 1b: Serve/Return matchup strength (optional, blended into Elo factor)
    strength_data = _get_strength_probs(player_a, player_b, surface)
    has_strength = strength_data is not None
    strength_prob_a = strength_data["prob_a"] if has_strength else 0.5
    elo_prob_component_a = (
        (0.75 * elo_prob_a) + (0.25 * strength_prob_a)
        if has_strength
        else elo_prob_a
    )

    # Factor 2: Recent Form
    form_a, form_b = _get_form_scores(player_a, player_b)
    has_form = form_a is not None and form_b is not None

    # Factor 3: Surface Win Rate
    swr_a, swr_b = _get_surface_win_rates(player_a, player_b, surface)
    has_surface = swr_a is not None and swr_b is not None

    # Factor 4: H2H
    h2h_a = _get_h2h_prob(player_a, player_b, surface)
    has_h2h = h2h_a is not None

    # Determine data quality
    factors_available = sum([has_form, has_surface, has_h2h])
    if factors_available == 3:
        data_quality = "full"
    elif factors_available > 0:
        data_quality = "partial"
    else:
        data_quality = "elo_only"

    # Blend probabilities with dynamic weighting
    if data_quality == "elo_only":
        prob_a = elo_prob_component_a
        form_prob_a = 0.5
        surf_prob_a = 0.5
        h2h_prob_a = 0.5
    else:
        # Normalize form scores to probability
        if has_form:
            form_total = form_a + form_b
            form_prob_a = form_a / form_total if form_total > 0 else 0.5
        else:
            form_prob_a = 0.5

        # Normalize surface win rates to probability
        if has_surface:
            surf_total = swr_a + swr_b
            surf_prob_a = swr_a / surf_total if surf_total > 0 else 0.5
        else:
            surf_prob_a = 0.5

        # H2H is already a probability
        h2h_prob_a = h2h_a if has_h2h else 0.5

        # Weighted blend — redistribute weight of missing factors to Elo
        w_elo = W_ELO
        w_form = W_FORM if has_form else 0.0
        w_surf = W_SURFACE if has_surface else 0.0
        w_h2h = W_H2H if has_h2h else 0.0
        total_w = w_elo + w_form + w_surf + w_h2h
        # Normalize weights to sum to 1.0
        w_elo /= total_w
        w_form /= total_w
        w_surf /= total_w
        w_h2h /= total_w

        prob_a = (
            w_elo * elo_prob_component_a +
            w_form * form_prob_a +
            w_surf * surf_prob_a +
            w_h2h * h2h_prob_a
        )

    prob_a = round(max(0.01, min(0.99, prob_a)), 4)
    prob_b = round(1.0 - prob_a, 4)

    # Get raw H2H wins for display
    h2h_wins_a, h2h_wins_b = 0, 0
    h2h_rec = get_h2h_record(player_a, player_b, surface)
    if h2h_rec:
        h2h_wins_a = h2h_rec["wins_a"]
        h2h_wins_b = h2h_rec["wins_b"]
    else:
        h2h_rec_overall = get_h2h_record(player_a, player_b, "overall")
        if h2h_rec_overall:
            h2h_wins_a = h2h_rec_overall["wins_a"]
            h2h_wins_b = h2h_rec_overall["wins_b"]

    return {
        "prob_a":         prob_a,
        "prob_b":         prob_b,
        "elo_a":          elo["elo_a"],
        "elo_b":          elo["elo_b"],
        "elo_prob_a":     round(elo_prob_a, 4),
        "strength_prob_a": round(strength_prob_a, 4),
        "form_prob_a":    round(form_prob_a, 4),
        "surface_prob_a": round(surf_prob_a, 4),
        "h2h_prob_a":     round(h2h_prob_a, 4),
        "form_a":         form_a if form_a is not None else 0.5,
        "form_b":         form_b if form_b is not None else 0.5,
        "h2h_wins_a":     h2h_wins_a,
        "h2h_wins_b":     h2h_wins_b,
        "data_quality":   data_quality,
    }


def _get_strength_probs(player_a: str, player_b: str, surface: str):
    """Returns strength model output dict or None when unavailable."""
    try:
        return predict_strength_prob(player_a, player_b, surface)
    except Exception:
        return None


def _get_form_scores(player_a: str, player_b: str):
    """Returns (form_a, form_b) or (None, None) if insufficient data."""
    try:
        stats_a = get_player_stats(player_a, "overall")
        stats_b = get_player_stats(player_b, "overall")
        fa = None
        fb = None
        if stats_a and stats_a.get("form_score") is not None:
            if stats_a.get("matches_counted", 0) >= MIN_FORM_MATCHES:
                fa = stats_a["form_score"]
        if stats_b and stats_b.get("form_score") is not None:
            if stats_b.get("matches_counted", 0) >= MIN_FORM_MATCHES:
                fb = stats_b["form_score"]
        if fa is not None and fb is not None:
            return fa, fb
        return None, None
    except Exception:
        return None, None


def _get_surface_win_rates(player_a: str, player_b: str, surface: str):
    """Returns (swr_a, swr_b) or (None, None) if insufficient data."""
    try:
        stats_a = get_player_stats(player_a, surface)
        stats_b = get_player_stats(player_b, surface)

        swr_a = None
        swr_b = None

        if stats_a and stats_a.get("surface_win_rate") is not None:
            if stats_a.get("matches_counted", 0) >= MIN_SURFACE_MATCHES:
                swr_a = stats_a["surface_win_rate"]
        if stats_b and stats_b.get("surface_win_rate") is not None:
            if stats_b.get("matches_counted", 0) >= MIN_SURFACE_MATCHES:
                swr_b = stats_b["surface_win_rate"]

        # Fall back to overall win rate if surface data insufficient
        if swr_a is None:
            overall_a = get_player_stats(player_a, "overall")
            if overall_a and overall_a.get("surface_win_rate") is not None:
                swr_a = overall_a["surface_win_rate"]
        if swr_b is None:
            overall_b = get_player_stats(player_b, "overall")
            if overall_b and overall_b.get("surface_win_rate") is not None:
                swr_b = overall_b["surface_win_rate"]

        if swr_a is not None and swr_b is not None:
            return swr_a, swr_b
        return None, None
    except Exception:
        return None, None


def _get_h2h_prob(player_a: str, player_b: str, surface: str):
    """Returns H2H win probability for player_a, or None if insufficient data."""
    try:
        # Try surface-specific first
        rec = get_h2h_record(player_a, player_b, surface)
        if rec:
            total = rec["wins_a"] + rec["wins_b"]
            if total >= MIN_H2H_MATCHES:
                return rec["wins_a"] / total

        # Fall back to overall
        rec_overall = get_h2h_record(player_a, player_b, "overall")
        if rec_overall:
            total = rec_overall["wins_a"] + rec_overall["wins_b"]
            if total >= MIN_H2H_MATCHES:
                return rec_overall["wins_a"] / total

        return None
    except Exception:
        return None
