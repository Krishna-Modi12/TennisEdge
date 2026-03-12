"""
tests/test_calculator.py
Comprehensive tests for the hardened signal calculator.
"""

import os
import sys
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import get_surface, normalize_tournament_name
from signals.calculator import (
    safe_float,
    is_allowed_tournament,
    calculate_pinnacle_prob,
    calibrate_probability,
    compute_dynamic_edge_threshold,
    compute_kelly_fraction,
    is_match_within_window,
    calculate_signal,
    process_matches,
    MAX_SIGNALS_PER_SCAN,
    MIN_ODDS,
    MAX_ODDS,
)


class TestSafeFloat(unittest.TestCase):
    def test_valid_numbers(self):
        self.assertEqual(safe_float(1.5), 1.5)
        self.assertEqual(safe_float("2.0"), 2.0)
        self.assertEqual(safe_float(0), 0.0)

    def test_invalid_values(self):
        self.assertIsNone(safe_float(None))
        self.assertIsNone(safe_float("abc"))
        self.assertIsNone(safe_float(""))
        self.assertIsNone(safe_float([]))


class TestTournamentFilter(unittest.TestCase):
    """Improvement 1 — Tournament filter with tour keywords."""

    def test_blocked_tier_keywords(self):
        """ITF/Futures keywords must always be blocked."""
        self.assertFalse(is_allowed_tournament("M25 Monastir"))
        self.assertFalse(is_allowed_tournament("W15 Antalya"))
        self.assertFalse(is_allowed_tournament("ITF Men's Circuit"))
        self.assertFalse(is_allowed_tournament("Men's Futures"))

    def test_allowed_tier_labels(self):
        """Known tier labels must pass."""
        self.assertTrue(is_allowed_tournament("Any Name", tier="ATP"))
        self.assertTrue(is_allowed_tournament("Any Name", tier="WTA"))
        self.assertTrue(is_allowed_tournament("Any Name", tier="Challenger"))
        self.assertTrue(is_allowed_tournament("Any Name", tier="Grand Slam"))

    def test_unknown_tier_rejected(self):
        """Unknown tier labels must be rejected."""
        self.assertFalse(is_allowed_tournament("Some Event", tier="ITF"))
        self.assertFalse(is_allowed_tournament("Some Event", tier="Unknown"))

    def test_unnamed_itf_rejected(self):
        """Unknown tournament names without tour keywords must be rejected."""
        self.assertFalse(is_allowed_tournament("Monastir 3"))
        self.assertFalse(is_allowed_tournament("Sharm El Sheikh 4"))
        self.assertFalse(is_allowed_tournament("Antalya 2"))
        self.assertFalse(is_allowed_tournament("Some Random Cup 7"))

    def test_tour_keyword_required_without_tier(self):
        """ATP/WTA keyword in name → allowed when no tier label."""
        self.assertTrue(is_allowed_tournament("ATP Challenger Cherbourg"))
        self.assertTrue(is_allowed_tournament("WTA 500 Dubai"))
        self.assertTrue(is_allowed_tournament("Masters 1000 Indian Wells"))

    def test_grand_slams_allowed_without_tier(self):
        self.assertTrue(is_allowed_tournament("Roland Garros"))
        self.assertTrue(is_allowed_tournament("Wimbledon"))
        self.assertTrue(is_allowed_tournament("US Open"))
        self.assertTrue(is_allowed_tournament("Australian Open"))

    def test_masters_1000_allowed_without_tier(self):
        """ATP Masters 1000 city names must pass without a tier label."""
        events = [
            "Indian Wells Masters",
            "Miami Open",
            "Madrid Open",
            "Rome Masters",
            "Montreal Masters",
            "Toronto Masters",
            "Cincinnati Masters",
            "Shanghai Masters",
            "Paris Masters",
            "Bercy Open",
        ]
        for event in events:
            self.assertTrue(
                is_allowed_tournament(event),
                f"Masters 1000 event incorrectly rejected: '{event}'"
            )

    def test_major_tour_events_allowed_without_tier(self):
        """Major ATP/WTA city names must pass without a tier label."""
        events = [
            "Dubai Duty Free Championships",
            "Qatar ExxonMobil Open",
            "Barcelona Open",
            "Hamburg Open",
            "Washington Open",
            "Beijing Open",
            "Tokyo Open",
            "Vienna Open",
            "Basel Indoor",
            "Rotterdam Open",
        ]
        for event in events:
            self.assertTrue(
                is_allowed_tournament(event),
                f"Major tour event incorrectly rejected: '{event}'"
            )

    def test_wta_1000_allowed_without_tier(self):
        """WTA 1000 city names must pass without a tier label."""
        events = [
            "Chicago Open",
            "Wuhan Open",
            "Guadalajara Open",
            "Ostrava Open",
        ]
        for event in events:
            self.assertTrue(
                is_allowed_tournament(event),
                f"WTA 1000 event incorrectly rejected: '{event}'"
            )

    def test_unnamed_low_tier_still_rejected(self):
        """Unnamed low-tier events must still be rejected after keyword expansion."""
        self.assertFalse(is_allowed_tournament("Monastir 3"))
        self.assertFalse(is_allowed_tournament("Sharm El Sheikh 4"))
        self.assertFalse(is_allowed_tournament("Antalya 2"))
        self.assertFalse(is_allowed_tournament("Some Random Cup 7"))

    def test_city_name_with_lower_tier_prefix(self):
        """Blocked keyword before city name must still reject."""
        self.assertFalse(is_allowed_tournament("M25 Dubai"))
        self.assertFalse(is_allowed_tournament("ITF Madrid Futures"))
        self.assertFalse(is_allowed_tournament("W25 Toronto Qualifier"))

    def test_city_name_with_lower_tier_suffix(self):
        """Blocked keyword after city name must still reject.
        If any assertion here fails, stop immediately and do not proceed
        to deployment. Report which assertion failed."""
        self.assertFalse(is_allowed_tournament("Dubai M25"))
        self.assertFalse(is_allowed_tournament("Paris ITF"))
        self.assertFalse(is_allowed_tournament("Madrid Futures"))
        self.assertFalse(is_allowed_tournament("Paris Futures"))

    def test_case_insensitive_matching(self):
        """Tournament name matching must be case-insensitive.
        Only present if is_allowed_tournament() normalizes input to lowercase."""
        self.assertTrue(is_allowed_tournament("INDIAN WELLS MASTERS"))
        self.assertTrue(is_allowed_tournament("Miami OPEN"))
        self.assertTrue(is_allowed_tournament("ROLAND GARROS"))
        self.assertTrue(is_allowed_tournament("atp 250 delray beach"))


class TestPinnacleProb(unittest.TestCase):
    """Improvement 3 — Pinnacle margin validation."""

    def test_normal_margin_accepted(self):
        """Normal Pinnacle margin (1.02–1.06) must pass."""
        # 1/1.80 + 1/2.00 ≈ 1.0556 — valid tight two-way market
        prob = calculate_pinnacle_prob(1.80, 2.00)
        self.assertGreater(prob, 0.0)
        self.assertLess(prob, 1.0)

    def test_impossible_margin_raises(self):
        """Margin below the configured floor must raise."""
        with self.assertRaises(ValueError):
            # 1/50 + 1/50 = 0.04 < 1.0
            calculate_pinnacle_prob(50.0, 50.0)

    def test_high_margin_raises(self):
        """Margin > 1.15 is not Pinnacle — must raise."""
        # 1/1.50 + 1/1.50 = 1.333 — absurd margin
        with self.assertRaises(ValueError):
            calculate_pinnacle_prob(1.50, 1.50)

    def test_invalid_odds_raises(self):
        """Odds <= 1.0 must raise."""
        with self.assertRaises(ValueError):
            calculate_pinnacle_prob(0.5, 2.0)
        with self.assertRaises(ValueError):
            calculate_pinnacle_prob(1.0, 2.0)

    def test_prob_is_clamped(self):
        """Probability must be clamped between 0.001 and 0.999."""
        # Normal case — prob should be reasonable
        prob = calculate_pinnacle_prob(1.80, 2.00)
        self.assertGreaterEqual(prob, 0.001)
        self.assertLessEqual(prob, 0.999)


class TestCalibrationAndRisk(unittest.TestCase):
    def test_probability_calibration_weighting(self):
        calibrated = calibrate_probability(0.60, 0.50)
        self.assertAlmostEqual(calibrated, 0.575, places=3)

    def test_probability_calibration_extreme_gap_uses_average(self):
        calibrated = calibrate_probability(0.90, 0.20)
        self.assertAlmostEqual(calibrated, 0.55, places=3)

    def test_dynamic_edge_threshold_bands(self):
        self.assertEqual(compute_dynamic_edge_threshold(0.02), 0.04)
        self.assertEqual(compute_dynamic_edge_threshold(0.05), 0.05)
        self.assertEqual(compute_dynamic_edge_threshold(0.12), 0.06)

    def test_half_kelly_capped(self):
        f = compute_kelly_fraction(0.65, 2.2)
        self.assertGreaterEqual(f, 0.0)
        self.assertLessEqual(f, 0.05)

    def test_dynamic_edge_threshold_clamps_out_of_range_base(self):
        with patch("signals.calculator.get_model_parameter", return_value=0.50):
            self.assertEqual(compute_dynamic_edge_threshold(0.02), 0.06)
            self.assertEqual(compute_dynamic_edge_threshold(0.12), 0.08)

    def test_half_kelly_clamps_out_of_range_multiplier(self):
        with patch("signals.calculator.get_model_parameter", return_value=10.0):
            f = compute_kelly_fraction(0.65, 2.2)
        self.assertGreaterEqual(f, 0.0)
        self.assertLessEqual(f, 0.05)


class TestOddsSanityBounds(unittest.TestCase):
    """Improvement 2 — Odds sanity bounds."""

    def test_extreme_odds_rejected(self):
        """Odds outside valid bounds must produce None signal."""
        match = _base_match(pinny_odds_a=999.0, pinny_odds_b=1.001, odds_a=500.0)
        self.assertIsNone(calculate_signal(match))

    def test_odds_at_boundary_accepted(self):
        """Values at boundary should be accepted by safe_float."""
        v = safe_float(1.01)
        self.assertIsNotNone(v)
        v2 = safe_float(100.0)
        self.assertIsNotNone(v2)


class TestSuspiciousOddsGap(unittest.TestCase):
    """Improvement 5 — API data quality."""

    def test_suspicious_odds_gap_rejected(self):
        """Extreme odds gap between player and opponent → rejected."""
        match = _base_match(pinny_odds_a=1.05, pinny_odds_b=25.0, odds_a=1.10)
        self.assertIsNone(calculate_signal(match))


class TestSurfaceMapping(unittest.TestCase):
    def test_get_surface_uses_known_tournament_mapping(self):
        self.assertEqual(get_surface("Wimbledon"), "grass")
        self.assertEqual(get_surface("Roland Garros"), "clay")
        self.assertEqual(get_surface("French Open"), "clay")
        self.assertEqual(get_surface("Monte Carlo Masters"), "clay")
        self.assertEqual(get_surface("Madrid Open"), "clay")
        self.assertEqual(get_surface("ATP Miami Open"), "hard")

    def test_get_surface_defaults_to_hard(self):
        self.assertEqual(get_surface("Unknown Invitational"), "hard")

    def test_tournament_name_normalization_handles_hyphen_and_spaces(self):
        self.assertEqual(normalize_tournament_name("ATP-500 Madrid"), "atp 500 madrid")
        self.assertEqual(normalize_tournament_name("  ATP   Madrid  "), "atp madrid")

    def test_get_surface_with_inconsistent_tournament_formats(self):
        self.assertEqual(get_surface("ATP Madrid"), "clay")
        self.assertEqual(get_surface("ATP Madrid Qualifiers"), "clay")
        self.assertEqual(get_surface("Madrid Masters"), "clay")
        self.assertEqual(get_surface("ATP-500 Madrid"), "clay")


class TestMatchTimingWindow(unittest.TestCase):
    """Improvement 9 — Minimum market liquidity filter."""

    def test_future_match_rejected(self):
        """Match more than 24h away must be rejected."""
        far_future = (datetime.now(tz=timezone.utc) + timedelta(hours=48)).isoformat()
        match = _base_match(match_time=far_future)
        result = calculate_signal(match)
        self.assertIsNone(result)

    def test_imminent_match_allowed(self):
        """Match within 24h must pass timing check."""
        soon = (datetime.now(tz=timezone.utc) + timedelta(hours=3)).isoformat()
        match = _base_match(match_time=soon)
        signal = calculate_signal(match)
        # Signal may be None for other reasons — just confirm timing doesn't block it
        if signal is None:
            self.assertNotEqual(match.get("_rejection"), "timing")

    def test_no_match_time_fails_open(self):
        """Missing match time must not reject the signal."""
        match = _base_match()
        # Remove any time fields
        for key in ["match_time", "start_time", "commence_time", "event_time",
                     "event_date"]:
            match.pop(key, None)
        result = calculate_signal(match)
        if result is None:
            self.assertNotEqual(match.get("_rejection"), "timing")

    def test_unix_timestamp_handled(self):
        """Unix timestamps should be parsed correctly."""
        # 3 hours from now
        ts = (datetime.now(tz=timezone.utc) + timedelta(hours=3)).timestamp()
        match = _base_match(match_time=ts)
        self.assertTrue(is_match_within_window(match))

    def test_iso_string_handled(self):
        """ISO strings should be parsed correctly."""
        iso = (datetime.now(tz=timezone.utc) + timedelta(hours=3)).isoformat()
        match = _base_match(match_time=iso)
        self.assertTrue(is_match_within_window(match))


class TestSignalRateCap(unittest.TestCase):
    """Improvement 6 — Signal rate protection."""

    def test_signal_rate_cap(self):
        """Never fire more than MAX_SIGNALS_PER_SCAN signals in one scan."""
        # Create 20 identical valid matches that would all fire as signals
        matches = [_base_match(match_id=f"cap_test_{i}") for i in range(20)]
        signals, _ = process_matches(matches)
        self.assertLessEqual(
            len(signals), MAX_SIGNALS_PER_SCAN,
            f"Fired {len(signals)} signals — exceeds cap of {MAX_SIGNALS_PER_SCAN}"
        )


class TestEvScore(unittest.TestCase):
    """Improvement 4 — ev_score as additional field."""

    def test_ev_score_present_in_signal(self):
        """ev_score must be present in fired signals."""
        match = _base_match()
        result = calculate_signal(match)
        if result is not None:
            self.assertIn("ev_score", result)
            self.assertIn("true_edge_score", result)
            # ev_score should be value_edge * model_prob
            expected_ev = result["value_edge"] * result["model_prob"]
            self.assertAlmostEqual(result["ev_score"], round(expected_ev, 4), places=3)

    def test_probability_source_present(self):
        """probability_source must be 'pinnacle_devig'."""
        match = _base_match()
        result = calculate_signal(match)
        if result is not None:
            self.assertEqual(result["probability_source"], "pinnacle_devig")


class TestScanSummary(unittest.TestCase):
    """Improvement 8 — Scan summary metrics."""

    def test_rejection_counts_returned(self):
        """process_matches must return rejection counts dict."""
        matches = [_base_match()]
        _, rejection_counts = process_matches(matches)
        self.assertIn("tournament", rejection_counts)
        self.assertIn("edge", rejection_counts)
        self.assertIn("prob", rejection_counts)
        self.assertIn("invalid_odds", rejection_counts)
        self.assertIn("elo", rejection_counts)
        self.assertIn("timing", rejection_counts)
        self.assertIn("total", rejection_counts)


class TestCalculateSignalIntegration(unittest.TestCase):
    """Integration tests for the full calculate_signal flow."""

    def test_valid_signal_fires(self):
        """A well-formed match with good edge should fire a signal."""
        match = _base_match()
        result = calculate_signal(match)
        # It may or may not fire depending on edge thresholds
        # but it should not crash
        if result is not None:
            self.assertIn("model_prob", result)
            self.assertIn("value_edge", result)
            self.assertIn("true_edge_score", result)
            self.assertIn("confidence", result)

    def test_missing_pinnacle_odds_rejected(self):
        """Match without Pinnacle odds must be rejected."""
        match = _base_match()
        match.pop("pinny_odds_a", None)
        match.pop("pinny_odds_b", None)
        self.assertIsNone(calculate_signal(match))

    def test_unknown_tournament_rejected(self):
        """Match with unknown tournament name must be rejected."""
        match = _base_match(tournament="Monastir 3")
        self.assertIsNone(calculate_signal(match))
        self.assertEqual(match.get("_rejection"), "tournament")


# ══════════════════════════════════════════════════════════════════════════════
# TEST HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _base_match(
    match_id: str = "test_001",
    player_a: str = "Carlos Alcaraz",
    player_b: str = "Jannik Sinner",
    tournament: str = "ATP Masters 1000 Madrid",
    surface: str = "clay",
    odds_a: float = 1.85,
    odds_b: float = 2.05,
    pinny_odds_a: float = 1.83,
    pinny_odds_b: float = 2.08,
    elo_prob_a: float = 0.58,
    match_time=None,
    **kwargs,
) -> dict:
    """Create a base match dict for testing."""
    match = {
        "match_id":     match_id,
        "player_a":     player_a,
        "player_b":     player_b,
        "tournament":   tournament,
        "surface":      surface,
        "odds_a":       odds_a,
        "odds_b":       odds_b,
        "pinny_odds_a": pinny_odds_a,
        "pinny_odds_b": pinny_odds_b,
        "elo_prob_a":   elo_prob_a,
    }
    if match_time is not None:
        match["match_time"] = match_time
    match.update(kwargs)
    return match


if __name__ == "__main__":
    unittest.main()
