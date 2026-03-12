"""
streaming/odds_stream.py
Async ingestion engine for real-time odds updates.
"""

import asyncio
import logging
import json
from datetime import datetime
from database.db import save_odds_snapshot

logger = logging.getLogger(__name__)

class OddsStreamer:
    """
    Handles connection to odds providers and puts events into an internal queue.
    """
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.running = False

    async def start(self):
        self.running = True
        logger.info("Starting Odds Ingestion Stream...")
        
        # In a real scenario, this would connect to a WebSocket (e.g. Betfair, Pinnacle API)
        # For now, we simulate an incoming stream.
        while self.running:
            await self._simulate_event()
            await asyncio.sleep(2) # New odds every 2s

    async def _simulate_event(self):
        """Simulate an incoming odds event for a featured match."""
        # For demo: hardcoded match ID or random from DB
        event = {
            "match_id": "SIM_MATCH_123",
            "odds_a": 1.85,
            "odds_b": 2.10,
            "timestamp": datetime.now().isoformat()
        }
        
        # Persist to history for time-series features
        save_odds_snapshot(event["match_id"], event["odds_a"], event["odds_b"])
        
        # Push to workers
        await self.queue.put(event)

    def stop(self):
        self.running = False
        logger.info("Stopping Odds Stream.")
