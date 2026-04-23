import sys
from pathlib import Path

import requests
from betfairlightweight import APIClient

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betfair.session import get_session_token
from app.config import BETFAIR_APP_KEY

def login_betfair():
    if not BETFAIR_APP_KEY:
        raise ValueError("BETFAIR_APP_KEY required (APP_KEY also works as fallback)")
    session_token = get_session_token()
    session = requests.Session()
    session.cookies.set("ssid", session_token)
    client = APIClient(
        username="authenticated",
        password="authenticated",
        app_key=BETFAIR_APP_KEY,
        lightweight=True,
        session=session
    )
    print("Betfair authenticated")
    return client

if __name__ == "__main__":
    login_betfair()
