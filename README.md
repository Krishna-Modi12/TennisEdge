# 🎾 TennisEdge
[![CI](https://github.com/Krishna-Modi12/TennisEdge/actions/workflows/ci.yml/badge.svg)](https://github.com/Krishna-Modi12/TennisEdge/actions/workflows/ci.yml)

A Telegram bot that detects value betting edges in tennis matches using surface-adjusted Elo ratings.

---

## How It Works

```text
Fetch Odds (AllSportsAPI)
        ↓
4-Factor Model (Elo, Form, Surface, H2H)
        ↓
Value Edge = (Model Prob * Market Odds) - 1
True Edge = Value Edge * Confidence
        ↓
If Value Edge ≥ 4% and Model Prob ≥ 35% → Send Signal via Telegram
        ↓
Deduct 1 Credit from User
```

### Backtest Model

`/backtest` uses Pinnacle implied probabilities (de-vigged) as the primary model
and applies point-in-time Elo as a secondary confirmation filter.
Use paper trading + CLV metrics as the primary validator for live behavior.

---

## Setup

### 1. Clone & install dependencies

```bash
cd tennisedge
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Create your .env file

```bash
copy .env.example .env
# then edit .env with your values
```

### 3. Create the PostgreSQL database

```bash
psql -U postgres
CREATE DATABASE tennisedge;
\q
```

### 4. Run the bot

```bash
python bot.py
```

---

## Testing Without API Keys (Mock Mode)

Leave `MOCK_MODE=true` in your `.env` — the bot will use fake match data so you can test all commands.

To trigger a manual scan (as admin):

```text
/scan
```

To add credits to a user (as admin):

```text
/addcredits <telegram_id> <amount>
```

---

## Bot Commands

| Command | Description |
|---|---|
| /start | Register & welcome |
| /balance | Check your credits |
| /buy | Purchase credits info |
| /signals | View recent signals |
| /matches | Upcoming matches |
| /predict | AI match analysis |
| /portfolio | Paper trading stats |
| /beta | Join free beta channel |
| /help | How it works |
| /scan | (Admin) Run pipeline now |
| /addcredits | (Admin) Add credits manually |
| /broadcastbeta | (Admin) Invite all users to beta channel |
| /backtest | (Admin) Pinnacle+Elo backtest |

---

## File Structure

```text
tennisedge/
├── bot.py                      ← main entry point
├── config.py                   ← all settings
├── requirements.txt
├── .env.example
│
├── ingestion/
│   └── fetch_odds.py           ← AllSportsAPI + mock data
│
├── models/
│   └── elo_model.py            ← surface-adjusted Elo
│
├── signals/
│   ├── edge_detector.py        ← edge detection engine
│   └── formatter.py            ← Telegram message formatter
│
├── scheduler/
│   └── job.py                  ← 30-min automation pipeline
│
└── database/
    └── db.py                   ← PostgreSQL all-in-one
```

---

## Going Live

1. Set `MOCK_MODE=false` in `.env`
2. Add your real `ODDS_API_KEY`
3. Add your real `TELEGRAM_BOT_TOKEN`
4. Set `BETA_CHANNEL_LINK` to your free Telegram beta channel invite
5. Deploy to any VPS (DigitalOcean, Hetzner, etc.)
6. Run with: `python bot.py`

### Paper Trading Kickoff

- Scheduler now performs the first pipeline run immediately on startup.
- Use this status command daily until 100 resolved paper bets are reached:
  - `python paper_trading_status.py`
  - JSON mode: `python paper_trading_status.py --json`

### Sprint 2 Tooling

- Elo calibration diagnostics: `python -m tennis_backtest.elo_calibration_check`
- Baseline probability build: `python -m tennis_backtest.step4b_baseline_probs --input <in.csv> --output <out.csv>`
- Elo K sweep: `python -m tennis_backtest.elo_k_calibration --input-csv <out.csv>`
- Backtest wrapper: `python -m tennis_backtest.step6_backtest_v2`
- Hard-stop pipeline runner: `python -m tennis_backtest.run_sprint2_pipeline --input-csv <in.csv>`

### Render Cron (Daily Elo Update)

Add a Render Cron Job to keep Elo ratings updated from finished matches:

- Schedule: `0 6 * * *` (06:00 UTC daily)
- Command: `python -m scheduler.update_elo_job`
- Optional dry-run check: `python -m scheduler.update_elo_job --dry-run`

---

## Credit Packages

| Plan | Credits | Price |
|---|---|---|
| Starter | 10 | ₹199 |
| Pro | 50 | ₹799 |
| VIP | 200 | ₹2499 |

Credits are added manually by admin after UPI payment confirmation.
