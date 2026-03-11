# Pre-Deployment Checklist

Run before every push to main / Render redeploy.

## 1 — Code Syntax Checks

```bash
python -m py_compile fetch_odds.py
python -m py_compile scheduler/job.py
python -m py_compile database/db.py
python -m py_compile bot.py
python -m py_compile resolve_matches.py
python -m py_compile signals/formatter.py
python -m py_compile health.py
python -m py_compile audit_deliveries.py
python -m py_compile monitor_live_quality.py
```

## 2 — Environment Variable Checks (Render dashboard)

- [ ] TELEGRAM_BOT_TOKEN — set and correct
- [ ] ALLSPORTS_API_KEY — set and active (check API quota remaining)
- [ ] UPI_ID — set and matches payment details
- [ ] DB_PATH — set correctly for Render persistent disk
- [ ] MOCK_MODE — explicitly set to "false" (never blank, never "true")
- [ ] ALLOW_MOCK_ON_RENDER — must NOT be set in production

## 3 — Threshold Consistency Check

- [ ] Open config.py — confirm thresholds are edge ≥ 4%, prob ≥ 35%
- [ ] Open README.md — confirm threshold description matches config.py exactly
- [ ] Do NOT change thresholds until paper trading validation is complete

## 4 — Database Checks

```bash
python audit_deliveries.py
```

- [ ] No duplicate deliveries
- [ ] No stale signals older than 48h without resolution

## 5 — System Health Check

```bash
python health.py
```

- [ ] All checks green before deploying

## 6 — After Deployment (on Render)

- [ ] Run health.py again remotely (or check Render logs for startup errors)
- [ ] Send /status command via bot — confirm response
- [ ] Run resolve_matches.py dry-run:

  ```bash
  python resolve_matches.py --dry-run
  ```

- [ ] Run audit_deliveries.py once more

## Never Deploy With

- [ ] MOCK_MODE=true without ALLOW_MOCK_ON_RENDER=true
- [ ] Hardcoded UPI_ID, API keys, or tokens anywhere in code
- [ ] Thresholds changed from edge ≥ 4%, prob ≥ 35%
- [ ] README threshold description not matching config.py
