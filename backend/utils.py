# backend/utils.py

import asyncio
import logging
from backend.adx_client import ADXSystemError

logger = logging.getLogger(__name__)

async def execute_with_backoff(func, *args, max_retries=3, **kwargs):
    """
    Executes a function with Exponential Backoff retry logic.
    
    Rules:
    - If it's a System Error (Network/Auth) -> WAIT and RETRY.
    - If it's a Logic Error (Semantic) -> FAIL IMMEDIATELY (so LLM can fix it).
    - If it succeeds -> Return data.
    """
    delay = 1  # Start with 1 second wait
    
    for attempt in range(max_retries + 1):
        try:
            # ⚡ Await the async function passed in (run_kql)
            return await func(*args, **kwargs)
            
        except ADXSystemError as e:
            # This is a network glitch. We should retry.
            if attempt == max_retries:
                logger.critical(f"❌ System unreachable after {max_retries} retries. Giving up.")
                raise e
            
            logger.warning(f"⚠️ Network/System glitch. Retrying in {delay}s... (Error: {e})")
            # ⚡ NON-BLOCKING SLEEP
            # This releases the CPU to handle other users!
            await asyncio.sleep(delay)
            delay *= 2  # Double the wait time (1s -> 2s -> 4s)
            
        except Exception as e:
            # This is a Logic/Semantic error (or unknown). 
            # Do NOT retry. Fail fast so the LLM can see the error message.
            raise e