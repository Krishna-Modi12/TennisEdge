import unittest

import pandas as pd

from tennis_backtest.step4b_baseline_probs import add_baseline_probabilities, de_vig_probs


class Step4bBaselineTests(unittest.TestCase):
    def test_de_vig_probs(self):
        p1, p2 = de_vig_probs(1.91, 1.91)
        self.assertAlmostEqual(p1, 0.5, places=6)
        self.assertAlmostEqual(p2, 0.5, places=6)

    def test_add_baseline_probabilities(self):
        df = pd.DataFrame(
            [
                {
                    "winner": "Alice",
                    "loser": "Bob",
                    "psw": 1.91,
                    "psl": 1.91,
                    "maxw": 2.10,
                    "maxl": 1.80,
                }
            ]
        )
        out = add_baseline_probabilities(df)
        self.assertIn("pinnacle_prob_w", out.columns)
        self.assertIn("value_edge_w", out.columns)
        self.assertAlmostEqual(float(out.loc[0, "pinnacle_prob_w"]), 0.5, places=6)
        self.assertAlmostEqual(float(out.loc[0, "value_edge_w"]), 0.05, places=6)


if __name__ == "__main__":
    unittest.main()
