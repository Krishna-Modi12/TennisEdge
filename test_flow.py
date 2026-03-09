import logging
logging.basicConfig(level=logging.DEBUG)

from database.db import init_schema, get_conn
init_schema()

# Insert dummy match data for the functions to use
with get_conn() as conn:
    conn.execute("INSERT OR REPLACE INTO matches (id, date, player1, player2, surface, winner) VALUES (1, '2024-01-01', 'Carlos Alcaraz', 'Jannik Sinner', 'clay', 'Carlos Alcaraz')")
    conn.execute("INSERT OR REPLACE INTO h2h (id, player1, player2, surface, winner, date) VALUES (1, 'Carlos Alcaraz', 'Jannik Sinner', 'clay', 'Carlos Alcaraz', '2024-01-01')")
    conn.commit()

from ingestion.fetch_odds import fetch_odds
from signals.edge_detector import detect_edges

print("Fetching odds...")
matches = fetch_odds()
print(f"Got {len(matches)} matches.")

print("Detecting edges...")
signals = detect_edges(matches)
print(f"Got {len(signals)} signals.")
if signals:
    from signals.formatter import format_signal
    print("Formatting first signal:")
    print(format_signal(signals[0]))
