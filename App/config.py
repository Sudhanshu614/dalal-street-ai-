"""
Simple Configuration - Keep It Simple!

Philosophy: Dead simple for hobby projects
Just put your database and CSV files in the right place and it works

For Production:
    1. Copy Database/ folder to server
    2. Run: python your_app.py
    That's it!
"""

from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from App/.env
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR.parent / ".env")


class Config:
    """
    Configuration that prefers environment variables for sensitive values.

    Notes:
    - Reads API keys from environment variables first (recommended).
    - Falls back to empty string if not present (prevents accidental leakage of secrets).
    """

    def __init__(self):
        # Project root (auto-detected)
        self.PROJECT_ROOT = Path(__file__).parent

        # Default paths (override if needed)
        self.DB_PATH = self.PROJECT_ROOT / "Database" / "stock_market_new.db"
        self.CSV_DIRECTORY = self.PROJECT_ROOT / "Database"

        # LLM Configuration (read from environment variables)
        # IMPORTANT: Do NOT commit real API keys into source control. Use environment variables or secret managers.
        def _read_env(name: str) -> str:
            v = os.getenv(name, "")
            if v:
                return v.strip()
            for k, val in os.environ.items():
                if k.strip() == name:
                    return (val or "").strip()
            return ""
        self.GEMINI_API_KEY = _read_env("GEMINI_API_KEY")
        self.GROQ_API_KEY = _read_env("GROQ_API_KEY")

        # LLM Provider Selection
        # Options: 'gemini', 'groq', 'hybrid'
        self.LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")
        # To enable Groq later: set LLM_PROVIDER='groq'
        # For mixed mode: set LLM_PROVIDER='hybrid'

        # Price cache TTL in seconds
        self.PRICE_CACHE_TTL_SEC = int(os.getenv("PRICE_CACHE_TTL_SEC", "60"))

        # Frontend streaming toggle
        self.FRONTEND_STREAMING = os.getenv("FRONTEND_STREAMING", "0") == "1"
        self.FRONTEND_STREAM_CHUNK = int(os.getenv("FRONTEND_STREAM_CHUNK", "10"))

    def __repr__(self):
        return f"Config(DB_PATH={self.DB_PATH}, CSV_DIRECTORY={self.CSV_DIRECTORY})"


# Simple singleton
config = Config()


# NOTE:
# - Set GEMINI_API_KEY and GROQ_API_KEY in environment or a .env file before starting the server.
# - Example (PowerShell):
#   $env:GEMINI_API_KEY = 'your_key_here'; $env:GROQ_API_KEY = 'your_key_here'
