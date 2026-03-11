# health.py
"""
Lightweight health check for Render / uptime monitoring.
Checks: database connectivity, last signal time, MOCK_MODE safety,
        required environment variables.

Note: Does NOT check AllSportsAPI connectivity (that requires a live
API call and is better handled by a dedicated ping in fetch_odds.py).

Run from: project root
"""
import json
import os
import sys
import sqlite3
from datetime import datetime

# DB_PATH has a safe default — no env var required for basic operation
DB_PATH = os.getenv("DB_PATH", "database/signals.db")


def check_database() -> dict:
    """Verify database is accessible and has recent activity."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check signal_deliveries table exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='signal_deliveries'
        """)
        table_exists = cursor.fetchone() is not None

        # Last signal sent
        last_signal = None
        hours_since_last = None
        if table_exists:
            cursor.execute("SELECT MAX(created_at) FROM signal_deliveries")
            row = cursor.fetchone()
            if row and row[0]:
                last_signal = row[0]
                try:
                    last_dt = datetime.fromisoformat(last_signal)
                    delta_secs = (datetime.utcnow() - last_dt).total_seconds()
                    hours_since_last = round(delta_secs / 3600, 1)
                except Exception:
                    pass

        return {
            "status":           "ok",
            "table_exists":     table_exists,
            "last_signal":      last_signal,
            "hours_since_last": hours_since_last
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}

    finally:
        if conn:
            conn.close()


def check_mock_mode() -> dict:
    """Verify MOCK_MODE is not accidentally enabled in production."""
    mock_mode    = os.getenv("MOCK_MODE", "false").lower()
    is_production = os.getenv("RENDER", "") != ""   # Render sets this env var

    warning = None
    if mock_mode == "true" and is_production:
        warning = "CRITICAL: MOCK_MODE=true on production Render instance"

    return {
        "mock_mode":     mock_mode,
        "is_production": is_production,
        "warning":       warning
    }


def check_env_vars() -> dict:
    """Verify all required environment variables are set."""
    # DB_PATH is excluded — it has a safe default in this file
    required = [
        "TELEGRAM_BOT_TOKEN",
        "ALLSPORTS_API_KEY",
        "UPI_ID",
    ]
    missing = [v for v in required if not os.getenv(v)]
    return {
        "status":  "ok" if not missing else "missing_vars",
        "missing": missing
    }


if __name__ == "__main__":
    report = {
        "checked_at": datetime.utcnow().isoformat(),
        "database":   check_database(),
        "mock_mode":  check_mock_mode(),
        "env_vars":   check_env_vars()
    }

    print("\n" + "=" * 50)
    print("  SYSTEM HEALTH CHECK")
    print(f"  {report['checked_at']} UTC")
    print("=" * 50)

    db = report['database']
    print(f"\nDatabase       : {db['status']}")
    if db.get('last_signal'):
        print(f"Last signal    : {db['last_signal']} ({db['hours_since_last']}h ago)")
    else:
        print("Last signal    : none recorded yet")

    mock = report['mock_mode']
    if mock.get('warning'):
        print(f"\n⚠️  {mock['warning']}")
    else:
        print(f"\nMOCK_MODE      : {mock['mock_mode']}")
        print(f"Production     : {mock['is_production']}")

    env = report['env_vars']
    if env['missing']:
        print(f"\n❌ Missing env vars: {', '.join(env['missing'])}")
    else:
        print(f"\n✅ All required env vars present")

    # Overall verdict
    issues = []
    if db['status'] != 'ok':
        issues.append("database unreachable")
    if mock.get('warning'):
        issues.append("MOCK_MODE=true on production")
    if env['missing']:
        issues.append(f"missing env vars: {env['missing']}")

    print("\n" + "-" * 50)
    if issues:
        print(f"❌ UNHEALTHY: {'; '.join(issues)}")
        sys.exit(1)
    else:
        print("✅ SYSTEM HEALTHY")
    print("-" * 50 + "\n")

    with open("health_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print("Saved to health_report.json")
