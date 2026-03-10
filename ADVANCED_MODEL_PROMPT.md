# TennisEdge – Advanced Player Stats & Probability Model
## Prompt for Claude Opus 4.6

---

## Context: What This Project Is

TennisEdge is a live Telegram bot that detects value betting edges in tennis
matches. It is already built and running. The core loop is:

1. Fetch live tennis odds from TheOddsAPI (sport keys: tennis_atp, tennis_wta)
2. Run a probability model to estimate each player's win chance
3. Compare model probability vs bookmaker-implied probability
4. If EDGE = Model Prob – Market Prob > 7%, send a signal to Telegram subscribers
5. Deduct 1 credit per signal from the user's balance

The bot is written in Python. Stack: python-telegram-bot 20.7, PostgreSQL
(psycopg2 ThreadedConnectionPool), APScheduler, TheOddsAPI, python-dotenv.

---

## Current Model: What Exists

The current probability model is in `models/elo_model.py`.

It uses surface-adjusted Elo only:
- Tracks separate Elo ratings per surface: clay, hard, grass, carpet, overall
- Blend: 60% surface Elo + 40% overall Elo
- Win probability formula: E(A) = 1 / (1 + 10^((Rb - Ra) / 400))
- Elo ratings were built from tennis-data.co.uk historical match data (2010–2025)

The current model works but is too simplistic. It misses:
- Recent form (a player on a 10-match streak vs one returning from injury)
- Head-to-head record (especially surface-specific H2H)
- Surface win rate (actual % wins on clay/hard/grass in last 2 years)
- Fatigue / match load
- Tournament-specific performance

---

## What You Need to Build

Build an advanced player stats system that replaces the pure Elo model with a
blended probability model. The new model should combine 4 factors:

```
Final Probability =
    40% × Surface-Adjusted Elo          (already exists)
  + 25% × Recent Form Score             (NEW)
  + 20% × Surface Win Rate              (NEW)
  + 15% × Head-to-Head on this surface  (NEW)
```

---

## Data Source

All data comes from tennis-data.co.uk Excel files. These are already downloaded
in `ingestion/build_elo_from_history.py`. You will extend that script OR create
a new `ingestion/build_player_stats.py` that downloads the same files and
extracts additional stats.

URL pattern for files:
- ATP 2013+:  http://www.tennis-data.co.uk/{year}/{year}.xlsx
- ATP pre-2013: http://www.tennis-data.co.uk/{year}/{year}.xls
- WTA 2013+:  http://www.tennis-data.co.uk/{year}w/{year}.xlsx
- WTA pre-2012: http://www.tennis-data.co.uk/{year}w/{year}.xls

Key columns in these files (already normalised to lowercase):
- winner: name of match winner
- loser: name of match loser
- surface: Hard / Clay / Grass / Carpet
- date: match date
- tournament: tournament name
- round: match round (R128, R64, R32, R16, QF, SF, F)

---

## Detailed Specification for Each New Factor

### Factor 1: Recent Form Score (weight: 25%)

Calculate a weighted win rate over the player's last 20 matches overall
(not surface-specific, to ensure enough data even for clay specialists).

Decay weighting — more recent matches count more:
- Match 1 (most recent): weight 1.00
- Match 2: weight 0.95
- Match 3: weight 0.90
- ...each match: weight decreases by 0.05
- Match 20: weight 0.05

Formula:
```
form_score = Σ(result_i × weight_i) / Σ(weight_i)
where result_i = 1 for win, 0 for loss
```

Output: a float between 0.0 and 1.0
Example: player on 8-match win streak → form_score ≈ 0.85+

### Factor 2: Surface Win Rate (weight: 20%)

Calculate win rate specifically on the current match surface
using the last 2 years of data only (to keep it current).

Formula:
```
surface_win_rate = wins_on_surface / (wins_on_surface + losses_on_surface)
```

Minimum matches threshold: if player has fewer than 10 matches on a surface
in last 2 years, fall back to their overall win rate to avoid small sample
size distortion.

Output: float between 0.0 and 1.0

### Factor 3: Head-to-Head on Surface (weight: 15%)

Calculate H2H win rate between the two specific players on the current surface,
using all available historical data (not time-limited, H2H is always relevant).

Formula:
```
h2h_win_rate = h2h_wins_on_surface / h2h_total_on_surface
```

Minimum matches threshold: if fewer than 3 H2H matches exist on this surface,
use overall H2H across all surfaces. If fewer than 3 overall H2H matches exist,
fall back to 0.5 (neutral — no H2H data).

Output: float between 0.0 and 1.0

### Factor 4: Surface-Adjusted Elo (weight: 40%)

Already exists in `models/elo_model.py`. Use the existing `predict()` function
which returns prob_a and prob_b. No changes needed here.

---

## Final Probability Blend

```python
def blend_probabilities(elo_prob, form_score_a, form_score_b,
                        surface_wr_a, surface_wr_b,
                        h2h_wr_a) -> tuple[float, float]:
    """
    Returns (prob_a, prob_b) as final blended win probabilities.
    All inputs are from player A's perspective.
    h2h_wr_a = player A's H2H win rate vs player B on this surface.
    """
    # Normalise form scores to probabilities
    form_total = form_score_a + form_score_b
    form_prob_a = form_score_a / form_total if form_total > 0 else 0.5

    # Normalise surface win rates to probabilities
    surf_total = surface_wr_a + surface_wr_b
    surf_prob_a = surface_wr_a / surf_total if surf_total > 0 else 0.5

    # H2H is already a probability for player A
    h2h_prob_a = h2h_wr_a  # float 0.0–1.0

    # Weighted blend
    prob_a = (
        0.40 * elo_prob +
        0.25 * form_prob_a +
        0.20 * surf_prob_a +
        0.15 * h2h_prob_a
    )
    prob_b = 1.0 - prob_a
    return round(prob_a, 4), round(prob_b, 4)
```

---

## Database Changes Required

Add these new tables to the schema in `database/db.py`:

```sql
-- Stores each player's recent form score and surface win rates
CREATE TABLE IF NOT EXISTS player_stats (
    id               SERIAL PRIMARY KEY,
    player_name      TEXT NOT NULL,
    surface          TEXT NOT NULL,   -- 'overall' | 'clay' | 'hard' | 'grass'
    form_score       NUMERIC(5,4),    -- recent form: 0.0 to 1.0
    surface_win_rate NUMERIC(5,4),    -- win rate on surface: 0.0 to 1.0
    matches_counted  INTEGER,         -- how many matches used for this stat
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(player_name, surface)
);

-- Stores head-to-head records between player pairs on each surface
CREATE TABLE IF NOT EXISTS h2h_records (
    id            SERIAL PRIMARY KEY,
    player_a      TEXT NOT NULL,
    player_b      TEXT NOT NULL,
    surface       TEXT NOT NULL,   -- 'overall' | 'clay' | 'hard' | 'grass'
    wins_a        INTEGER DEFAULT 0,
    wins_b        INTEGER DEFAULT 0,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(player_a, player_b, surface)
);
```

Add DB helper functions in `database/db.py`:
- `get_player_stats(player, surface) -> dict`
- `upsert_player_stats(player, surface, form_score, surface_win_rate, matches_counted)`
- `get_h2h(player_a, player_b, surface) -> dict`  — should check BOTH orderings (a vs b AND b vs a)
- `upsert_h2h(player_a, player_b, surface, wins_a, wins_b)`

---

## Files to Create

### 1. `ingestion/build_player_stats.py`

One-time script (like build_elo_from_history.py) that:
- Downloads ATP and WTA Excel files from tennis-data.co.uk (use same download logic)
- Processes every match to build:
  - Recent form score (last 20 matches per player, date-sorted)
  - Surface win rate (last 2 years per player per surface)
  - H2H records (all time, per surface and overall)
- Flushes everything to the new DB tables
- Has CLI args: --atp-from, --atp-to, --wta-from, --wta-to, --no-db

Run command:
```bash
python -m ingestion.build_player_stats --atp-from 2015 --atp-to 2025 --wta-from 2015 --wta-to 2025
```

### 2. `models/advanced_model.py`

New model file that:
- Imports `predict()` from `models/elo_model.py` for the Elo component
- Imports player stats and H2H from DB
- Implements `advanced_predict(player_a, player_b, surface) -> dict`
- Returns same shape as old `predict()` but with additional fields:

```python
{
    "prob_a":         float,   # final blended probability for player_a
    "prob_b":         float,
    "elo_prob_a":     float,   # Elo component only (for transparency)
    "form_prob_a":    float,   # form component
    "surface_prob_a": float,   # surface win rate component
    "h2h_prob_a":     float,   # H2H component
    "form_a":         float,   # raw form score player_a
    "form_b":         float,
    "h2h_wins_a":     int,     # raw H2H wins on surface
    "h2h_wins_b":     int,
    "data_quality":   str,     # "full" | "partial" | "elo_only"
                               # "partial" = some stats missing, fell back to defaults
                               # "elo_only" = no stats in DB yet, pure Elo used
}
```

### 3. Update `signals/edge_detector.py`

Change the import from:
```python
from models.elo_model import predict
```
to:
```python
from models.advanced_model import advanced_predict as predict
```

The `advanced_predict` function must return at minimum `prob_a` and `prob_b`
so the rest of edge_detector.py works without any other changes.

### 4. Update `signals/formatter.py`

Update `format_signal()` to show the model breakdown when available:

```
📊 Model Breakdown
Elo:         54%
Form:        61%
Surface W%:  58%
H2H:         45%
━━━━━━━━━━━━━━
Final:       56%
```

Only show this breakdown if the signal dict contains the extra fields.
Keep backward compatibility — if fields are missing, show the old format.

---

## Important Implementation Rules

1. **Graceful fallback**: If a player has no stats in the DB (new player, or
   stats not built yet), fall back to pure Elo. Never crash — always return
   a valid probability. Set data_quality = "elo_only" in this case.

2. **Minimum sample sizes**: Enforce minimums to avoid small-sample distortion:
   - Form score: needs at least 5 matches, else use 0.5 (neutral)
   - Surface win rate: needs at least 10 surface matches in 2 years, else use overall win rate
   - H2H on surface: needs at least 3 matches, else use overall H2H
   - Overall H2H: needs at least 3 matches, else use 0.5 (neutral)

3. **H2H symmetry**: The H2H table stores player_a and player_b alphabetically
   (sorted) to avoid duplicate entries. Always sort player names before querying.
   `player_a, player_b = sorted([player_a, player_b])`

4. **Date handling**: tennis-data.co.uk date column format is inconsistent
   across years. Handle both DD/MM/YYYY and YYYY-MM-DD formats. Use
   `pd.to_datetime(df['date'], dayfirst=True, errors='coerce')` and drop NaT rows.

5. **Player name consistency**: Names in TheOddsAPI may differ slightly from
   tennis-data.co.uk (e.g. "Carlos Alcaraz" vs "Alcaraz C."). Do NOT try to
   fuzzy match — just build stats for whatever names appear in the historical data.
   The Elo fallback handles unknown players gracefully.

6. **Thread safety**: All DB operations must use the existing `get_conn()` /
   `release_conn()` pattern from `database/db.py`. Never hold a connection
   across multiple function calls.

7. **Do not modify**: `bot.py`, `scheduler/job.py`, `config.py`, `database/db.py`
   schema tables (only ADD new tables, never modify existing ones),
   `ingestion/fetch_odds.py`, `ingestion/build_elo_from_history.py`.

---

## Existing File Structure (Do Not Break These)

```
tennisedge/
├── bot.py                          ← DO NOT MODIFY
├── config.py                       ← DO NOT MODIFY
├── requirements.txt                ← ADD new deps if needed
├── database/
│   └── db.py                       ← ADD new tables + helpers only
├── ingestion/
│   ├── fetch_odds.py               ← DO NOT MODIFY
│   └── build_elo_from_history.py   ← DO NOT MODIFY
├── models/
│   ├── elo_model.py                ← DO NOT MODIFY
│   └── advanced_model.py           ← CREATE THIS
├── signals/
│   ├── edge_detector.py            ← SMALL CHANGE: swap model import only
│   └── formatter.py                ← UPDATE: add breakdown display
└── scheduler/
    └── job.py                      ← DO NOT MODIFY
```

---

## New Files to Create (Summary)

| File | Purpose |
|---|---|
| `ingestion/build_player_stats.py` | One-time script to build form/H2H/win-rate from history |
| `models/advanced_model.py` | Blended probability model (Elo + form + surface + H2H) |

## Files to Modify (Summary)

| File | Change |
|---|---|
| `database/db.py` | Add 2 new tables + 4 new helper functions |
| `signals/edge_detector.py` | Swap model import (1 line change) |
| `signals/formatter.py` | Add model breakdown to signal message |
| `requirements.txt` | Add any new deps (pandas already there) |

---

## Deliverable

Working Python code for all files listed above. Each file should:
- Have clear docstrings
- Handle all edge cases gracefully (missing data, DB errors, division by zero)
- Print progress during the one-time build scripts
- Be consistent with the existing code style in the project

After building, the run order is:
```bash
# Step 1 - build Elo (already done, skip if done)
python -m ingestion.build_elo_from_history --atp-from 2015 --atp-to 2025 --wta-from 2015 --wta-to 2025

# Step 2 - build player stats (NEW)
python -m ingestion.build_player_stats --atp-from 2015 --atp-to 2025 --wta-from 2015 --wta-to 2025

# Step 3 - start bot as normal (advanced model is auto-used)
python bot.py
```
