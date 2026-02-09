# backend/chat_llm.py
# FIXED VERSION:
# - Added safety handling if Ollama is down
# - Chat LLM is ONLY used for greetings / casual talk
# - No DB, no JSON, no formatter logic here

import requests
import logging  # 1. Import logging
from backend.config import OLLAMA_GENERATE_URL, MODEL

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a friendly conversational assistant.

Rules:
- Respond ONLY to greetings and casual conversation
- Be short, polite, and natural
- Do NOT talk about databases, data, queries, or systems
- Do NOT ask follow-up questions unless necessary
"""


def chat_llm(user_message: str) -> str:
    """
    FIXED & SAFE CHAT LLM

    This function:
    - Handles ONLY greeting / small-talk queries
    - Uses LLM directly (no formatter, no JSON)
    - Gracefully fails if Ollama is unavailable
    """

    try:
        prompt = f"""
{SYSTEM_PROMPT}

User:
{user_message}
"""

        response = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False
            },
            timeout=30
        )

        return response.json()["response"].strip()

    except Exception as e:
        # 3. Log the error (Vital for debugging timeouts/crashes)
        logger.error(f"⚠️ Chat LLM Failed (Ollama down?): {str(e)}")
        
        # FIX: Prevent backend crash if Ollama is down
        return "Hi! I'm here to help 😊"
