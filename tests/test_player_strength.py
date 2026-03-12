import unittest
from unittest.mock import patch

from models.player_strength import compute_strength_probability, predict_strength_prob


class PlayerStrengthTests(unittest.TestCase):
    def test_compute_strength_probability_formula(self):
        stats_a = {"serve_strength": 0.68, "return_strength": 0.42}
        stats_b = {"serve_strength": 0.62, "return_strength": 0.36}
        expected = (0.68 + 0.42 - 0.62 - 0.36 + 1.0) / 2.0
        self.assertAlmostEqual(compute_strength_probability(stats_a, stats_b), expected, places=6)

    def test_predict_strength_prob_returns_none_when_data_missing(self):
        with patch("models.player_strength.get_player_surface_stats", return_value=None), patch(
            "models.player_strength.get_player_stats", return_value=None
        ):
            self.assertIsNone(predict_strength_prob("A", "B", "hard"))

    def test_predict_strength_prob_uses_surface_table_stats(self):
        table_rows = [
            {"serve_strength": 0.67, "return_strength": 0.41, "matches_counted": 25},
            {"serve_strength": 0.61, "return_strength": 0.37, "matches_counted": 28},
        ]

        with patch("models.player_strength.get_player_surface_stats", side_effect=table_rows), patch(
            "models.player_strength.get_player_stats", return_value=None
        ):
            result = predict_strength_prob("A", "B", "hard")

        self.assertIsNotNone(result)
        self.assertIn("prob_a", result)
        self.assertGreater(result["prob_a"], 0.0)
        self.assertLess(result["prob_a"], 1.0)


if __name__ == "__main__":
    unittest.main()
