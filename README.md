# 🎾 TennisEdge

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

`/backtest` now runs a leakage-safe, point-in-time Elo simulation.
It does **not** use the full live 4-factor model (form/surface/H2H blend).
Use paper trading metrics as the primary validator for live behavior.

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
| /help | How it works |
| /scan | (Admin) Run pipeline now |
| /addcredits | (Admin) Add credits manually |
| /backtest | (Admin) Point-in-time Elo backtest |

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
4. Deploy to any VPS (DigitalOcean, Hetzner, etc.)
5. Run with: `python bot.py`

---

## Credit Packages

| Plan | Credits | Price |
|---|---|---|
| Starter | 10 | ₹199 |
| Pro | 50 | ₹799 |
| VIP | 200 | ₹2499 |

Credits are added manually by admin after UPI payment confirmation.
