# User Communication Templates

These templates use Telegram Markdown v1 (parse_mode="Markdown").
Do NOT send with MarkdownV2 — the escaping rules are different and will
break rendering. Test each message with a bot admin send before broadcast.

Special characters that are safe in Markdown v1: * _ ` [

---

## Template 1 — Beta Transparency Notice

Send once to all current subscribers.

---
📊 *Signal System Update*

Our tennis signal system is now live, sending signals based on a
14.8% ROI backtest across 1,700+ historical signals.

A few things to know as an early subscriber:

✅ The model is statistically validated (p=0.0025) on historical data.
⚠️ We are in live validation phase — real-world results may differ
   from backtest performance while we collect live data.

We will share performance updates every 4 weeks. If signals
underperform, you will know.

Thank you for being an early member
---

## Template 2 — Monthly Performance Update

Send every 30 days. Fill in the X values from monitor_live_quality.py output.

---
📈 *Monthly Signal Update — [Month]*

Signals sent this month : X
Resolved                : X
Win Rate                : X.X%
ROI this month          : X.X%

Backtest baseline: 14.8% ROI | 45.4% win rate

[If tracking well:]
Performance is tracking close to our backtest model.

[If below baseline:]
This month came in below our backtest baseline. We are investigating.

Next update: [date]
---

## Template 3 — Performance Below Baseline (20+pp gap)

Send if monitor_live_quality.py returns REQUIRES INVESTIGATION verdict.

---
⚠️ *Important Update — Signal Performance*

Our live signal performance over the past [X] weeks has come in
significantly below our backtest expectations
(live ROI: X.X% vs backtest: 14.8%).

We are pausing new signal delivery while we investigate.
Current subscribers will not be charged during this period.

We will update you within 7 days with our findings
---
