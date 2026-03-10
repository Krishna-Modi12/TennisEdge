# TennisEdge – Full Agent Handoff Document

## 🎯 What This Project Is

TennisEdge is a **Telegram bot** that detects value betting opportunities in
tennis matches by comparing a statistical model's win probability against
bookmaker-implied probability. When the model finds a mispriced bet (edge > 7%),
it sends a signal to subscribed users via Telegram and deducts 1 credit from
their balance.

**One-line summary:**
> A fully automated tennis betting signal bot with credit-based monetisation,
> surface-adjusted Elo model, and live odds from TheOddsAPI.

---

## 🧠 Core Concept

```
EDGE = Model Probability – Market Probability

Example:
  Alcaraz vs Sinner on clay
  Model says Alcaraz: 58% chance
  Bookmaker odds 1.85 → implied: 54%
  Edge = 58% - 54% = +4%  (below threshold, no signal)

  If edge > 7% → fire signal → deduct 1 credit from each subscriber
```

---

## 📐 Probability Model

Tennis uses **surface-adjusted Elo**, NOT Poisson (tennis has no draws).

- Separate Elo ratings per surface: clay, hard, grass, carpet, overall
- Blend formula: 60% surface Elo + 40% overall Elo
- Win probability: E(A) = 1 / (1 + 10^((Rb - Ra) / 400))
- Elo is built from real historical data (tennis-data.co.uk, 2010 to 2025)

---

## 🗂 Complete File Structure

```
tennisedge/
├── bot.py                          Main entry point. Starts bot, registers all
│                                   handlers, seeds Elo, starts scheduler.
│
├── config.py                       All settings: API keys, thresholds, credit
│                                   packages, scheduler interval, Elo config, MOCK_MODE
│
├── requirements.txt
├── .env.example
├── CONTEXT.md                      This file
│
├── ingestion/
│   ├── fetch_odds.py               Pulls live tennis odds from TheOddsAPI.
│   │                               Has MOCK_MODE fallback with 5 realistic matches.
│   │                               Returns: match_id, player_a, player_b,
│   │                                        tournament, surface, odds_a, odds_b
│   │
│   └── build_elo_from_history.py   ONE-TIME SCRIPT. Downloads yearly Excel files
│                                   from tennis-data.co.uk (ATP 2010-2025, WTA 2010-2025).
│                                   Processes every match result, updates Elo,
│                                   flushes final ratings to PostgreSQL.
│
├── models/
│   └── elo_model.py                predict(player_a, player_b, surface)
│                                   returns {prob_a, prob_b, elo_a, elo_b}
│                                   update_elo_after_match(winner, loser, surface)
│                                   seed_top_players() — fallback hardcoded ratings
│
├── signals/
│   ├── edge_detector.py            detect_edges(matches) → list of signal dicts.
│   │                               Skips already-sent signals (match_id dedup).
│   │                               Filters odds outside MIN_ODDS/MAX_ODDS range.
│   │                               Saves signal to DB, returns signal dict.
│   │
│   └── formatter.py                format_signal(signal) → Telegram markdown string
│                                   format_signal_list(signals) → summary list
│
├── scheduler/
│   └── job.py                      APScheduler background job.
│                                   Runs run_pipeline() every 30 minutes:
│                                   fetch_odds → detect_edges → push to subscribers
│
└── database/
    └── db.py                       All PostgreSQL logic in one file.
                                    Tables: users, transactions, signals,
                                            signal_deliveries, player_elo
                                    Helpers: get_or_create_user, deduct_credit,
                                             add_credits_manual, save_signal,
                                             get_recent_signals, get/upsert elo
```

---

## 🤖 Bot Commands

| Command | Access | What It Does |
|---|---|---|
| /start | All | Registers user in DB, shows welcome + balance |
| /balance | All | Shows current credit count |
| /buy | All | Shows 3 packages with inline buttons, UPI payment instructions |
| /signals | All | Shows last 5 signals from DB |
| /help | All | Explains edge concept, surfaces, credits |
| /scan | Admin only | Manually triggers the full pipeline right now |
| /addcredits <telegram_id> <n> | Admin only | Manually top up a user's credits |

---

## 🗄 Database Schema

```sql
users               (id, telegram_id, username, credits, created_at)
transactions        (id, user_id, credits_added, payment_amount, payment_id, package, timestamp)
signals             (id, match_id UNIQUE, tournament, surface, player_a, player_b,
                     bet_on, model_prob, market_prob, edge, odds, created_at)
signal_deliveries   (id, signal_id, user_id, delivered_at)
player_elo          (id, player_name, surface, elo_rating, matches_played, updated_at)
                     UNIQUE(player_name, surface)
```

---

## 💳 Credit and Payment System

```
Packages:
  Starter  →  10 credits   Rs.199
  Pro      →  50 credits   Rs.799
  VIP      → 200 credits  Rs.2499

Payment flow (manual):
  User clicks /buy → selects package → gets UPI ID
  User pays → sends screenshot to admin on Telegram
  Admin runs /addcredits <telegram_id> <amount>
  Credits added instantly

1 signal = 1 credit deducted automatically on delivery
```

No Razorpay — Indian payment gateways reject gambling-adjacent services.

---

## Environment Variables (.env)

```
TELEGRAM_BOT_TOKEN=your_token        # from @BotFather
TELEGRAM_ADMIN_ID=your_numeric_id    # from @userinfobot on Telegram
ODDS_API_KEY=your_key                # from the-odds-api.com
DATABASE_URL=postgresql://postgres:password@localhost:5432/tennisedge
MOCK_MODE=true                       # set false for live API data
```

---

## How To Run (First Time)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create database
psql -U postgres -c "CREATE DATABASE tennisedge;"

# 3. Build real Elo ratings from history (run once, takes ~3 mins)
python -m ingestion.build_elo_from_history --atp-from 2015 --atp-to 2025 --wta-from 2015 --wta-to 2025

# 4. Start the bot
python bot.py
```

---

## Pipeline Flow (Every 30 Minutes)

```
APScheduler triggers run_pipeline()
        ↓
fetch_odds()         TheOddsAPI or mock data
        ↓
detect_edges()       Elo model vs market odds
        ↓
For each signal:
    save to DB
    get_all_subscribers() → users with credits > 0
    for each subscriber:
        deduct_credit()
        bot.send_message() → formatted signal
```

---

## What Is Already Built

- Full project folder structure
- PostgreSQL schema (auto-initialised on startup via init_schema())
- Surface-adjusted Elo model (predict, update, seed)
- TheOddsAPI live odds ingestion with mock fallback
- Edge detection engine with deduplication
- Telegram bot with all 7 commands
- APScheduler 30-minute automation pipeline
- Credit deduction on signal delivery
- Admin commands (/scan, /addcredits)
- Historical Elo builder from tennis-data.co.uk (2000-2026 ATP, 2007-2026 WTA)
- Inline keyboard for /buy with package selection
- Signal formatter with surface emoji and clean Telegram markdown

---

## What Is NOT Built Yet (Build These Next)

### Priority 1 — Live Elo Updates After Match Results
Currently Elo is only built from history (one-time script).
Need a daily job that:
- Fetches completed match results from TheOddsAPI scores endpoint or a results API
- Calls update_elo_after_match(winner, loser, surface) for each finished match
- Runs once daily (not every 30 mins)
File to create: scheduler/update_elo_job.py

### Priority 2 — Tournament-to-Surface Mapping
TheOddsAPI does not return surface in its response.
fetch_odds.py currently defaults everything to "hard" which is wrong.
Need to create a dict in config.py: TOURNAMENT_SURFACE_MAP
Example: {"French Open": "clay", "Wimbledon": "grass", "US Open": "hard"}
Then map tournament name from odds response to correct surface.

### Priority 3 — Signal Accuracy Tracker
After a signal fires, record the actual match result.
Mark each signal as WIN or LOSS in the signals table (add a result column).
Track overall accuracy % and ROI for credibility.
File to create: signals/result_tracker.py

### Priority 4 — Back-testing Script
Use historical data from tennis-data.co.uk to simulate:
- How many signals would have fired per year?
- What was the win rate and ROI?
- Helps validate and tune EDGE_THRESHOLD (currently 7%)
File to create: scripts/backtest.py

### Priority 5 — Low Credit Notification
When user drops to 0 credits, automatically send:
"You have 0 credits remaining. Use /buy to top up and keep receiving signals."

### Priority 6 — VIP Telegram Channel
Create a private Telegram channel.
Auto-invite users who purchase credits.
Remove access when credits hit 0.

---

## Known Issues and Warnings

1. Telegram token was accidentally exposed publicly — must revoke via @BotFather
   and generate a new one before running anything.

2. scheduler/job.py mixes sync APScheduler with async Telegram bot.
   The asyncio.ensure_future() call may behave differently depending on Python
   version and whether the event loop is already running. Test this carefully.

3. TheOddsAPI free tier = 500 requests/month.
   At 30-min polling = ~1440 requests/month for production.
   Upgrade to paid tier before going live.

4. build_elo_from_history.py downloads from tennis-data.co.uk which is a small
   site. Be respectful — add time.sleep(1) between downloads if processing many years.

5. openpyxl must be in requirements.txt for reading .xlsx files.
   xlrd must also be included for older .xls files (pre-2013 data).
   Currently requirements.txt does not include these — add them.

---

## Data Sources

| Source | Purpose | URL |
|---|---|---|
| TheOddsAPI | Live odds + upcoming matches | the-odds-api.com |
| tennis-data.co.uk | Historical results for Elo building | tennis-data.co.uk/alldata.php |
| Telegram Bot API | Bot messaging interface | core.telegram.org/bots/api |

---

## Key Design Decisions

- Elo over Poisson: tennis is 2-outcome, Poisson is for goal-scoring sports
- Surface Elo split: clay/hard/grass tracked separately, blended 60/40 with overall
- No Razorpay: Indian payment gateways reject this category, manual UPI used instead
- MOCK_MODE flag: allows full local testing with zero API keys
- Single DB file: all PostgreSQL logic in database/db.py for simplicity
- Raw psycopg2: no ORM, keeps dependencies minimal and queries explicit
