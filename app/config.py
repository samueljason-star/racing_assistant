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
BETFAIR_CERT_FILE = os.getenv("BETFAIR_CERT_FILE")
BETFAIR_KEY_FILE = os.getenv("BETFAIR_KEY_FILE")
BETFAIR_REGION = os.getenv("BETFAIR_REGION", "AU").upper()
PUNTING_FORM_API_KEY = os.getenv("PUNTING_FORM_API_KEY")
BETFAIR_IDENTITY_BASE_URL = os.getenv(
    "BETFAIR_IDENTITY_BASE_URL",
    "https://identitysso-cert.betfair.com.au"
    if BETFAIR_REGION in {"AU", "NZ"}
    else "https://identitysso-cert.betfair.com",
)
BETFAIR_KEEPALIVE_URL = os.getenv(
    "BETFAIR_KEEPALIVE_URL",
    "https://identitysso.betfair.com.au/api/keepAlive"
    if BETFAIR_REGION in {"AU", "NZ"}
    else "https://identitysso.betfair.com/api/keepAlive",
)


def _get_float_env(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return default


BETFAIR_COMMISSION_RATE = _get_float_env("BETFAIR_COMMISSION_RATE", 0.08)
PAPER_STAKE_PCT = _get_float_env("PAPER_STAKE_PCT", 0.01)
PAPER_STARTING_BANK = _get_float_env(
    "PAPER_STARTING_BANK",
    _get_float_env("PAPER_BANK_BASELINE", 10000.0),
)
PAPER_BANK_BASELINE = PAPER_STARTING_BANK
PAPER_MIN_ODDS = _get_float_env("PAPER_MIN_ODDS", 3.0)
PAPER_MAX_ODDS = _get_float_env("PAPER_MAX_ODDS", 15.0)
PAPER_MIN_EDGE = _get_float_env("PAPER_MIN_EDGE", 0.035)
PAPER_MAX_MODEL_PROBABILITY = _get_float_env("PAPER_MAX_MODEL_PROBABILITY", 0.30)
ACTIVE_DECISION_VERSION = os.getenv("ACTIVE_DECISION_VERSION", "model_edge_v2")
