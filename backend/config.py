# backend/config.py
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Load the .env file
if not load_dotenv():
    logger.warning("⚠️ No .env file found! Using system environment variables or defaults.")


# --- MISSING VARIABLE ADDED HERE ---
MODEL = os.getenv("OLLAMA_MODEL", "qwen3:4b")  #
# -----------------------------------

# ADX Settings
ADX_CLUSTER_URL = os.getenv("ADX_CLUSTER_URL")
ADX_DATABASE = os.getenv("ADX_DATABASE")

if not ADX_CLUSTER_URL:
    logger.warning("⚠️ ADX_CLUSTER_URL is not set. Database queries will fail.")

# Ollama Settings
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_CHAT_URL = f"{OLLAMA_BASE_URL}/api/chat"
OLLAMA_GENERATE_URL = f"{OLLAMA_BASE_URL}/api/generate"

# Authentication Settings
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")


