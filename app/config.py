import os
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///racing_assistant.db")

# Prefer a Betfair-specific name, but keep APP_KEY as a fallback so existing
# local setups keep working.
BETFAIR_APP_KEY = os.getenv("BETFAIR_APP_KEY") or os.getenv("APP_KEY")
BETFAIR_SSID = os.getenv("BETFAIR_SSID")
BETFAIR_USERNAME = os.getenv("BETFAIR_USERNAME")
BETFAIR_PASSWORD = os.getenv("BETFAIR_PASSWORD")
