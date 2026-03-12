import unittest
from unittest.mock import patch

from monitoring.model_monitor import (
    apply_adaptive_tuning,
    calculate_recent_model_performance,
    enforce_parameter_safety_bounds,
    get_parameter_safety_report,
)


class ModelMonitorTests(unittest.TestCase):
    def test_calculate_recent_model_performance(self):
        rows = [
            {"taken_odds": 2.0, "model_prob": 0.55, "is_win": True, "clv_ratio": 1.02, "line_movement": 0.03},
            {"taken_odds": 1.9, "model_prob": 0.52, "is_win": False, "clv_ratio": 0.98, "line_movement": -0.01},
        ]
        with patch("monitoring.model_monitor.get_recent_signal_performance", return_value=rows):
            perf = calculate_recent_model_performance(window=100)
        self.assertEqual(perf["sample_size"], 2)
        self.assertIsNotNone(perf["avg_clv_ratio"])

    def test_apply_adaptive_tuning_updates_parameters(self):
        rows = [
            {"taken_odds": 1.8, "model_prob": 0.55, "is_win": False, "clv_ratio": 0.97, "line_movement": -0.04},
            {"taken_odds": 1.9, "model_prob": 0.56, "is_win": False, "clv_ratio": 0.96, "line_movement": -0.03},
        ]
        with patch("monitoring.model_monitor.get_recent_signal_performance", return_value=rows), patch(
            "monitoring.model_monitor.get_model_parameter",
            side_effect=lambda name, default: default,
        ), patch("monitoring.model_monitor.set_model_parameter") as set_mock:
            result = apply_adaptive_tuning(window=100)
        self.assertTrue(result["changed"])
        self.assertGreaterEqual(set_mock.call_count, 2)

    def test_enforce_parameter_safety_bounds_clamps_out_of_range_values(self):
        values = {
            "dynamic_edge_base": 0.2,
            "kelly_multiplier": 5.0,
        }

        with patch(
            "monitoring.model_monitor.get_model_parameter",
            side_effect=lambda name, default: values.get(name, default),
        ), patch("monitoring.model_monitor.set_model_parameter") as set_mock:
            report = enforce_parameter_safety_bounds()

        self.assertTrue(report["changed"])
        self.assertAlmostEqual(report["edge_threshold_base"], 0.06, places=6)
        self.assertAlmostEqual(report["kelly_multiplier"], 1.25, places=6)
        self.assertGreaterEqual(set_mock.call_count, 2)

    def test_get_parameter_safety_report_flags_out_of_bounds(self):
        values = {
            "dynamic_edge_base": 0.01,
            "kelly_multiplier": 2.0,
        }
        with patch(
            "monitoring.model_monitor.get_model_parameter",
            side_effect=lambda name, default: values.get(name, default),
        ):
            report = get_parameter_safety_report()
        self.assertFalse(report["edge_in_bounds"])
        self.assertFalse(report["kelly_in_bounds"])
        self.assertFalse(report["safe"])


if __name__ == "__main__":
    unittest.main()
