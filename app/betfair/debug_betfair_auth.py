import json
import sys
from pathlib import Path

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config import BETFAIR_APP_KEY, BETFAIR_PASSWORD, BETFAIR_SSID, BETFAIR_USERNAME

BETFAIR_BETTING_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
BETFAIR_KEEP_ALIVE_URL = "https://identitysso.betfair.com/api/keepAlive"


def _mask(value, visible=4):
    if not value:
        return "<missing>"
    if len(value) <= visible * 2:
        return "*" * len(value)
    return f"{value[:visible]}...{value[-visible:]}"


def _print_env_summary():
    print("Loaded Betfair config:")
    print(
        json.dumps(
            {
                "BETFAIR_APP_KEY": {
                    "present": bool(BETFAIR_APP_KEY),
                    "length": len(BETFAIR_APP_KEY or ""),
                    "masked": _mask(BETFAIR_APP_KEY),
                },
                "BETFAIR_SSID": {
                    "present": bool(BETFAIR_SSID),
                    "length": len(BETFAIR_SSID or ""),
                    "masked": _mask(BETFAIR_SSID),
                },
                "BETFAIR_USERNAME": {
                    "present": bool(BETFAIR_USERNAME),
                    "length": len(BETFAIR_USERNAME or ""),
                    "masked": _mask(BETFAIR_USERNAME, visible=2),
                },
                "BETFAIR_PASSWORD": {
                    "present": bool(BETFAIR_PASSWORD),
                    "length": len(BETFAIR_PASSWORD or ""),
                    "masked": _mask(BETFAIR_PASSWORD, visible=1),
                },
            },
            indent=2,
        )
    )


def _betting_headers():
    return {
        "X-Application": BETFAIR_APP_KEY or "",
        "X-Authentication": BETFAIR_SSID or "",
        "Content-Type": "application/json",
    }


def _keep_alive_headers():
    return {
        "X-Application": BETFAIR_APP_KEY or "",
        "X-Authentication": BETFAIR_SSID or "",
        "Accept": "application/json",
    }


def check_keep_alive():
    print("\nChecking session with keepAlive...")
    response = requests.post(
        BETFAIR_KEEP_ALIVE_URL,
        headers=_keep_alive_headers(),
        timeout=30,
    )
    print(f"HTTP {response.status_code}")
    try:
        data = response.json()
    except ValueError:
        data = {"raw": response.text}
    print(json.dumps(data, indent=2))
    return data


def check_market_catalogue():
    print("\nChecking betting API with listMarketCatalogue...")
    payload = [
        {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {
                    "eventTypeIds": ["7"],
                    "marketTypeCodes": ["WIN"],
                },
                "maxResults": "5",
                "marketProjection": ["EVENT", "MARKET_START_TIME"],
            },
            "id": 1,
        }
    ]

    response = requests.post(
        BETFAIR_BETTING_URL,
        data=json.dumps(payload),
        headers=_betting_headers(),
        timeout=30,
    )
    print(f"HTTP {response.status_code}")
    data = response.json()
    print(json.dumps(data, indent=2))
    return data


def main():
    _print_env_summary()

    if not BETFAIR_APP_KEY or not BETFAIR_SSID:
        raise SystemExit(
            "Missing BETFAIR_APP_KEY or BETFAIR_SSID. Update .env and try again."
        )

    keep_alive_data = check_keep_alive()
    market_data = check_market_catalogue()

    keep_alive_status = keep_alive_data.get("status") if isinstance(keep_alive_data, dict) else None
    market_error = None
    if isinstance(market_data, list) and market_data and "error" in market_data[0]:
        market_error = (
            market_data[0]
            .get("error", {})
            .get("data", {})
            .get("APINGException", {})
            .get("errorCode")
        )

    print("\nSummary:")
    if keep_alive_status:
        print(f"- keepAlive status: {keep_alive_status}")
    else:
        print("- keepAlive status: unavailable")
    if market_error:
        print(f"- betting API errorCode: {market_error}")
    else:
        print("- betting API call did not report an API error")


if __name__ == "__main__":
    main()
