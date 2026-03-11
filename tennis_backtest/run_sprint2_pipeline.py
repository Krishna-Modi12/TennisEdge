"""
tennis_backtest/run_sprint2_pipeline.py
Execute Sprint-2 pipeline in strict order with hard-stop on failures.
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys


def _run_step(name: str, cmd: list[str], cwd: pathlib.Path) -> int:
    print(f"[pipeline] Running {name}: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(cwd))
    if result.returncode != 0:
        print(f"[pipeline] FAILED at {name} (exit {result.returncode})")
    else:
        print(f"[pipeline] OK: {name}")
    return result.returncode


def _parse_args():
    p = argparse.ArgumentParser(description="Run Sprint-2 pipeline (hard-stop mode).")
    p.add_argument(
        "--input-csv",
        help="Input CSV for step4b/threshold scripts. Required unless skipping those steps.",
    )
    p.add_argument("--output-dir", default="tennis_backtest/out")
    p.add_argument("--skip-step4b", action="store_true")
    p.add_argument("--skip-kcal", action="store_true")
    p.add_argument("--skip-threshold", action="store_true")
    p.add_argument("--skip-step6", action="store_true")
    p.add_argument("--skip-step8", action="store_true")
    p.add_argument("--skip-step9", action="store_true")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    out_dir = repo_root / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    step4b_out = out_dir / "step4b_baseline_probs.csv"

    steps: list[tuple[str, list[str], bool]] = []
    if not args.skip_step4b:
        if not args.input_csv:
            print("[pipeline] --input-csv is required when step4b is enabled.")
            return 1
        steps.append(
            (
                "step4b_baseline_probs",
                [
                    sys.executable,
                    "-m",
                    "tennis_backtest.step4b_baseline_probs",
                    "--input",
                    args.input_csv,
                    "--output",
                    str(step4b_out),
                ],
                True,
            )
        )
    if not args.skip_kcal:
        k_input = str(step4b_out) if step4b_out.exists() else (args.input_csv or "")
        cmd = [sys.executable, "-m", "tennis_backtest.elo_k_calibration"]
        if k_input:
            cmd.extend(["--input-csv", k_input])
        steps.append(("elo_k_calibration", cmd, True))
    if not args.skip_threshold:
        steps.append(
            (
                "threshold_sweep",
                [sys.executable, "-m", "tennis_backtest.threshold_sweep_v2", "--atp-from", "2023", "--atp-to", "2024"],
                True,
            )
        )
    if not args.skip_step6:
        steps.append(
            (
                "step6_backtest_v2",
                [sys.executable, "-m", "tennis_backtest.step6_backtest_v2", "--atp-from", "2023", "--atp-to", "2024"],
                True,
            )
        )
    if not args.skip_step8:
        steps.append(("step8_forward_test", [sys.executable, "-m", "paper_trading_status"], True))
    if not args.skip_step9:
        steps.append(("step9_summary", [sys.executable, "-m", "tennis_backtest.paper_trading.weekly_summary"], True))

    for name, cmd, enabled in steps:
        if not enabled:
            continue
        rc = _run_step(name, cmd, cwd=repo_root)
        if rc != 0:
            return rc

    print("[pipeline] Completed all configured steps.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
