import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from optimization.hyperparameter_search import (
    export_optimized_config,
    iter_parameter_configs,
    run_grid_search,
)


class OptimizationTests(unittest.TestCase):
    def test_iter_parameter_configs_uses_product(self):
        grid = {
            "edge_threshold": [0.04, 0.05],
            "kelly_fraction": [0.5],
            "weights": [{"elo": 0.35, "strength": 0.25, "mc": 0.25, "market": 0.15}],
        }
        combos = list(iter_parameter_configs(grid))
        self.assertEqual(len(combos), 2)
        self.assertEqual(combos[0]["edge_threshold"], 0.04)
        self.assertEqual(combos[1]["edge_threshold"], 0.05)

    def test_run_grid_search_returns_best_trial(self):
        fake_results = [
            {"roi": "2.0%", "win_rate": "50.0%"},
            {"roi": "6.0%", "win_rate": "52.0%"},
        ]

        with patch(
            "optimization.hyperparameter_search.iter_parameter_configs",
            return_value=[
                {"edge_threshold": 0.04, "kelly_fraction": 0.5, "weights": {"elo": 0.35}},
                {"edge_threshold": 0.05, "kelly_fraction": 0.5, "weights": {"elo": 0.35}},
            ],
        ), patch(
            "optimization.hyperparameter_search.run_historical_backtest",
            side_effect=fake_results,
        ), patch(
            "optimization.hyperparameter_search.save_model_optimization_result",
            side_effect=[1, 2],
        ), patch("optimization.hyperparameter_search.set_model_parameter"):
            result = run_grid_search(run_name="t", max_trials=2)

        self.assertEqual(result["trials"], 2)
        self.assertEqual(result["best"]["trial_id"], 2)

    def test_export_optimized_config_writes_json(self):
        payload = {
            "run_name": "t",
            "trials": 1,
            "best": {
                "score": 1.23,
                "roi_pct": 2.0,
                "win_rate_pct": 50.0,
                "params": {"edge_threshold": 0.04},
            },
        }
        with tempfile.TemporaryDirectory() as td:
            out_path = Path(td) / "config_optimized.json"
            exported = export_optimized_config(payload, output_path=str(out_path))
            self.assertEqual(Path(exported), out_path.resolve())
            loaded = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(loaded["best"]["score"], 1.23)


if __name__ == "__main__":
    unittest.main()
