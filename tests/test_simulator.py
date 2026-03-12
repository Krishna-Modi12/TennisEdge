import unittest

from models.simulator import simulate_match_probability


class SimulatorTests(unittest.TestCase):
    def test_simulator_probability_bounds(self):
        p = simulate_match_probability(
            serve_strength_a=0.65,
            return_strength_a=0.40,
            serve_strength_b=0.60,
            return_strength_b=0.35,
            simulations=1000,
            seed=42,
        )
        self.assertGreaterEqual(p, 0.01)
        self.assertLessEqual(p, 0.99)

    def test_simulator_reflects_strength_advantage(self):
        p_strong = simulate_match_probability(
            serve_strength_a=0.72,
            return_strength_a=0.46,
            serve_strength_b=0.58,
            return_strength_b=0.32,
            simulations=1000,
            seed=1,
        )
        p_weak = simulate_match_probability(
            serve_strength_a=0.58,
            return_strength_a=0.32,
            serve_strength_b=0.72,
            return_strength_b=0.46,
            simulations=1000,
            seed=1,
        )
        self.assertGreater(p_strong, 0.5)
        self.assertLess(p_weak, 0.5)


if __name__ == "__main__":
    unittest.main()
