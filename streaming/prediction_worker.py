"""
streaming/prediction_worker.py
Concurrent workers for matching streaming odds against models.
"""

import asyncio
import logging
from signals.calculator import calculate_signal

logger = logging.getLogger(__name__)

async def prediction_worker(worker_id: int, queue: asyncio.Queue):
    """
    Consumes odds events and runs the full signal calculation pipeline.
    """
    from scheduler.job import _send_signal_to_subscribers, _bot_loop
    
    logger.info(f"Worker-{worker_id} started.")
    while True:
        # Get an event from the stream
        event = await queue.get()
        
        try:
            logger.debug(f"Worker-{worker_id} processing {event['match_id']}")
            
            # Convert simple event back to match dict format expected by calculator
            # In a real system, we'd fetch full match metadata from cache
            match_data = {
                "match_id": event["match_id"],
                "pinnacle_odds_a": event["odds_a"],
                "pinnacle_odds_b": event["odds_b"],
                "odds_a": event["odds_a"],
                "odds_b": event["odds_b"],
                "player_a": event.get("player_a", "Jannik Sinner"), 
                "player_b": event.get("player_b", "Carlos Alcaraz"),
                "tournament": event.get("tournament", "ATP Masters Simulation"),
                "surface": event.get("surface", "hard")
            }
            
            # Run the heavy lifting
            signal = calculate_signal(match_data)
            
            if signal:
                logger.info(f"🔥 STREAM SIGNAL: {signal['bet_on']} @ {signal['odds']}")
                
                # Trigger live delivery to subscribers via the bot loop
                if _bot_loop and _bot_loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        _send_signal_to_subscribers(signal),
                        _bot_loop
                    )
                else:
                    # If called standalone, just use current loop
                    await _send_signal_to_subscribers(signal)
                
        except Exception as e:
            logger.error(f"Worker-{worker_id} error: {e}")
        finally:
            queue.task_done()

async def run_engine(num_workers: int = 3):
    """
    Entry point for the streaming prediction engine.
    """
    from streaming.odds_stream import OddsStreamer
    
    queue = asyncio.Queue()
    streamer = OddsStreamer(queue)
    
    # Start workers
    workers = []
    for i in range(num_workers):
        t = asyncio.create_task(prediction_worker(i, queue))
        workers.append(t)
    
    # Start streamer
    try:
        await streamer.start()
    except asyncio.CancelledError:
        streamer.stop()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_engine())
