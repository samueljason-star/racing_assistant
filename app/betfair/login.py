import sys
from pathlib import Path

import requests
from betfairlightweight import APIClient

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config import BETFAIR_APP_KEY, BETFAIR_SSID

def login_betfair():
    if not BETFAIR_SSID:
        raise ValueError("BETFAIR_SSID required")
    if not BETFAIR_APP_KEY:
        raise ValueError("BETFAIR_APP_KEY required (APP_KEY also works as fallback)")
    session = requests.Session()
    session.cookies.set("ssid", BETFAIR_SSID)
    client = APIClient(
        username="authenticated",
        password="authenticated",
        app_key=BETFAIR_APP_KEY,
        lightweight=True,
        session=session
    )
    print("Betfair authenticated with SSID")
    return client

if __name__ == "__main__":
    login_betfair()
