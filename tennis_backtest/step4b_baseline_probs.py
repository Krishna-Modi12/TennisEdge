"""
tennis_backtest/step4b_baseline_probs.py
Build Pinnacle-first baseline probabilities and value-edge columns.
"""

from __future__ import annotations

import argparse
import pathlib
from typing import Sequence

import pandas as pd


def de_vig_probs(odds_a: float, odds_b: float) -> tuple[float | None, float | None]:
    """Convert two-way odds to no-vig probabilities."""
    try:
        oa = float(odds_a)
        ob = float(odds_b)
    except (TypeError, ValueError):
        return None, None

    if oa <= 1.0 or ob <= 1.0:
        return None, None
    pa_raw = 1.0 / oa
    pb_raw = 1.0 / ob
    total = pa_raw + pb_raw
    if total <= 0:
        return None, None
    return pa_raw / total, pb_raw / total


def _pick_offer_pair(row: pd.Series, order: Sequence[str]) -> tuple[float | None, float | None, str | None]:
    """Pick first valid offer odds pair from ordered prefixes."""
    for prefix in order:
        wc = f"{prefix}w"
        lc = f"{prefix}l"
        if wc not in row.index or lc not in row.index:
            continue
        w = row.get(wc)
        l = row.get(lc)
        try:
            fw = float(w)
            fl = float(l)
        except (TypeError, ValueError):
            continue
        if fw > 1.0 and fl > 1.0:
            return fw, fl, prefix
    return None, None, None


def add_baseline_probabilities(
    df: pd.DataFrame,
    offer_order: Sequence[str] = ("max", "b365", "avg", "ps"),
) -> pd.DataFrame:
    """
    Add baseline probability columns:
    - pinnacle_prob_w/l (de-vig PSW/PSL)
    - offer_odds_w/l, offer_source
    - market_prob_w/l (from chosen offer odds)
    - value_edge_w/l based on pinnacle probs vs chosen offer odds
    """
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    required = {"winner", "loser"}
    if not required.issubset(df.columns):
        missing = ", ".join(sorted(required - set(df.columns)))
        raise ValueError(f"Input missing required columns: {missing}")
    if "psw" not in df.columns or "psl" not in df.columns:
        raise ValueError("Input must include Pinnacle odds columns: psw, psl")

    pinny_w = []
    pinny_l = []
    offer_w = []
    offer_l = []
    offer_src = []
    market_w = []
    market_l = []
    edge_w = []
    edge_l = []

    for _, row in df.iterrows():
        ppw, ppl = de_vig_probs(row.get("psw"), row.get("psl"))
        pinny_w.append(ppw)
        pinny_l.append(ppl)

        ow, ol, src = _pick_offer_pair(row, offer_order)
        offer_w.append(ow)
        offer_l.append(ol)
        offer_src.append(src)

        if ow and ol:
            mw = 1.0 / ow
            ml = 1.0 / ol
            market_w.append(mw)
            market_l.append(ml)
        else:
            market_w.append(None)
            market_l.append(None)

        if ppw is not None and ow is not None:
            edge_w.append((ppw * ow) - 1.0)
        else:
            edge_w.append(None)

        if ppl is not None and ol is not None:
            edge_l.append((ppl * ol) - 1.0)
        else:
            edge_l.append(None)

    df["pinnacle_prob_w"] = pinny_w
    df["pinnacle_prob_l"] = pinny_l
    df["offer_odds_w"] = offer_w
    df["offer_odds_l"] = offer_l
    df["offer_source"] = offer_src
    df["market_prob_w"] = market_w
    df["market_prob_l"] = market_l
    df["value_edge_w"] = edge_w
    df["value_edge_l"] = edge_l
    return df


def _parse_args():
    p = argparse.ArgumentParser(description="Build Pinnacle-first baseline probability dataset.")
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--output", required=True, help="Output CSV path")
    p.add_argument(
        "--offer-order",
        default="max,b365,avg,ps",
        help="Comma-separated odds preference for offer odds (default: max,b365,avg,ps)",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    in_path = pathlib.Path(args.input)
    out_path = pathlib.Path(args.output)

    if not in_path.exists():
        print(f"[step4b] Input file not found: {in_path}")
        return 1

    df = pd.read_csv(in_path)
    order = [x.strip().lower() for x in args.offer_order.split(",") if x.strip()]
    out = add_baseline_probabilities(df, offer_order=order)
    out.to_csv(out_path, index=False)

    valid = out["value_edge_w"].notna().sum() + out["value_edge_l"].notna().sum()
    print(f"[step4b] wrote {out_path} ({len(out)} rows, {valid} side-edges)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
