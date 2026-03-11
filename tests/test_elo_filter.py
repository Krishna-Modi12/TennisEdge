import unittest

from tennis_backtest.elo_filter import elo_agrees


class EloFilterTests(unittest.TestCase):
    def test_agrees_when_direction_matches_and_gap_small(self):
        self.assertTrue(elo_agrees(0.58, 0.60, max_gap=0.15))
        self.assertTrue(elo_agrees(0.42, 0.40, max_gap=0.15))

    def test_rejects_on_direction_conflict(self):
        self.assertFalse(elo_agrees(0.58, 0.45, max_gap=0.15))

    def test_rejects_on_large_gap(self):
        self.assertFalse(elo_agrees(0.70, 0.50, max_gap=0.15))


if __name__ == "__main__":
    unittest.main()
