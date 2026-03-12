import unittest

from models.ensemble_model import combine_ensemble_probability


class EnsembleModelTests(unittest.TestCase):
    def test_combine_ensemble_probability_with_all_components(self):
        prob, payload = combine_ensemble_probability(
            elo_prob=0.60,
            strength_prob=0.58,
            mc_prob=0.62,
            market_prob=0.55,
        )
        self.assertGreater(prob, 0.0)
        self.assertLess(prob, 1.0)
        self.assertIn("ensemble_prob", payload)
        self.assertIn("elo_prob", payload)

    def test_combine_ensemble_probability_fallbacks_to_elo(self):
        prob, payload = combine_ensemble_probability(elo_prob=0.57)
        self.assertAlmostEqual(prob, 0.57, places=6)
        self.assertEqual(payload["elo_prob"], 0.57)


if __name__ == "__main__":
    unittest.main()
