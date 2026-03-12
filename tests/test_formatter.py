import unittest

from signals.formatter import format_signal


class FormatterTests(unittest.TestCase):
    def _base_signal(self, model_prob: float) -> dict:
        return {
            "tournament": "ATP Finals",
            "surface": "hard",
            "player_a": "Alice",
            "player_b": "Bob",
            "bet_on": "Alice",
            "odds": 2.1,
            "model_prob": model_prob,
            "market_prob": 0.48,
            "value_edge": 0.05,
            "confidence": 1.2,
            "true_edge_score": 0.06,
        }

    def test_suspicious_probability_flag_added(self):
        msg = format_signal(self._base_signal(model_prob=0.95))
        self.assertIn("\\[Suspicious Model Probability\\]", msg)

    def test_normal_probability_has_no_flag(self):
        msg = format_signal(self._base_signal(model_prob=0.60))
        self.assertNotIn("\\[Suspicious Model Probability\\]", msg)


if __name__ == "__main__":
    unittest.main()
