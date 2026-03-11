import unittest
from unittest.mock import patch

import pandas as pd

import backtest.engine as engine


class BacktestEngineRegressionTests(unittest.TestCase):
    def test_backtest_sorts_matches_before_prediction(self):
        unsorted_matches = [
            {
                "date": "2024-01-02",
                "winner": "Alice",
                "loser": "Bob",
                "surface": "hard",
                "odds_winner": 2.10,
                "odds_loser": 1.90,
            },
            {
                "date": "2024-01-01",
                "winner": "Bob",
                "loser": "Alice",
                "surface": "hard",
                "odds_winner": 2.10,
                "odds_loser": 1.90,
            },
        ]

        predict_calls = []

        def fake_predict(_, player_a, player_b, surface):
            predict_calls.append((player_a, player_b, surface))
            return {"prob_a": 0.55, "prob_b": 0.45}

        dummy_df = pd.DataFrame({"winner": ["Seed"], "loser": ["Seed2"]})

        with patch.object(engine, "_download_year", return_value=dummy_df), patch.object(
            engine, "_parse_matches", return_value=unsorted_matches
        ), patch.object(
            engine, "_predict_point_in_time", side_effect=fake_predict
        ), patch.object(
            engine, "_update_point_in_time", return_value=None
        ):
            result = engine.backtest(
                atp_from=2024,
                atp_to=2024,
                wta_from=2025,
                wta_to=2024,
                stake=100.0,
                min_value_edge=-1.0,
                min_model_prob=0.0,
            )

        self.assertEqual(result["total_matches"], 2)
        self.assertEqual(
            predict_calls,
            [("Bob", "Alice", "hard"), ("Alice", "Bob", "hard")],
        )

    def test_backtest_uses_pinnacle_probs_as_primary_model(self):
        matches = [
            {
                "date": "2024-01-01",
                "winner": "Alice",
                "loser": "Bob",
                "surface": "hard",
                "odds_winner": 2.10,
                "odds_loser": 1.80,
                "pinny_odds_winner": 1.91,
                "pinny_odds_loser": 1.91,
            }
        ]

        dummy_df = pd.DataFrame({"winner": ["Seed"], "loser": ["Seed2"]})
        with patch.object(engine, "_download_year", return_value=dummy_df), patch.object(
            engine, "_parse_matches", return_value=matches
        ), patch.object(
            engine, "_predict_point_in_time", return_value={"prob_a": 0.9, "prob_b": 0.1}
        ), patch.object(
            engine, "_update_point_in_time", return_value=None
        ):
            result = engine.backtest(
                atp_from=2024,
                atp_to=2024,
                wta_from=2025,
                wta_to=2024,
                stake=100.0,
                min_value_edge=-1.0,
                min_model_prob=0.0,
                min_elo_prob=0.0,
            )

        self.assertEqual(result["total_matches"], 1)
        # If Pinnacle de-vig is primary, model_prob should be ~0.5, not Elo 0.9.
        self.assertGreater(result["total_bets"], 0)
        self.assertNotEqual(result["avg_true_edge"], "90.0%")


if __name__ == "__main__":
    unittest.main()
