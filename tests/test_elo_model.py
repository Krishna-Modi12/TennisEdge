import unittest
from unittest.mock import patch

from models import elo_model


class EloModelFormAdjustmentTests(unittest.TestCase):
    def test_compute_form_adjustment_returns_zero_for_no_matches(self):
        self.assertEqual(elo_model.compute_form_adjustment([]), 0.0)

    def test_compute_form_adjustment_caps_positive_adjustment(self):
        recent_matches = [{"expected_prob": 0.0, "actual_result": 1} for _ in range(5)]
        self.assertEqual(elo_model.compute_form_adjustment(recent_matches), 40.0)

    def test_compute_form_adjustment_caps_negative_adjustment(self):
        recent_matches = [{"expected_prob": 1.0, "actual_result": 0} for _ in range(5)]
        self.assertEqual(elo_model.compute_form_adjustment(recent_matches), -40.0)

    def test_compute_fatigue_penalty_window_logic(self):
        self.assertEqual(elo_model.compute_fatigue_penalty(0), 0.0)
        self.assertEqual(elo_model.compute_fatigue_penalty(1), 0.0)
        self.assertEqual(elo_model.compute_fatigue_penalty(2), 8.0)
        self.assertEqual(elo_model.compute_fatigue_penalty(4), 24.0)
        self.assertEqual(elo_model.compute_fatigue_penalty(10), 25.0)

    def test_predict_is_stable_when_recent_history_missing(self):
        ratings = {
            ("Alice", "overall"): 1600.0,
            ("Alice", "hard"): 1600.0,
            ("Bob", "overall"): 1500.0,
            ("Bob", "hard"): 1500.0,
        }

        def fake_get_elo(player, surface):
            return ratings[(player, surface)]

        with patch("models.elo_model.get_elo", side_effect=fake_get_elo), patch(
            "models.elo_model.get_recent_player_matches_for_form", return_value=[]
        ), patch("models.elo_model.get_recent_match_count", return_value=0):
            result = elo_model.predict("Alice", "Bob", "hard")

        expected_prob = elo_model.win_probability(1600.0, 1500.0)
        self.assertEqual(result["elo_a"], 1600.0)
        self.assertEqual(result["elo_b"], 1500.0)
        self.assertEqual(result["prob_a"], round(expected_prob, 4))
        self.assertEqual(result["prob_b"], round(1 - expected_prob, 4))


if __name__ == "__main__":
    unittest.main()
