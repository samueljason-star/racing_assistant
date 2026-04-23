import json
import sys
from pathlib import Path

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betfair.session import (
    BETFAIR_API_URL,
    CERT_LOGIN_URL,
    build_api_headers,
    clear_session_token,
    get_session_token,
    keep_alive,
)
from app.config import (
    BETFAIR_APP_KEY,
    BETFAIR_CERT_FILE,
    BETFAIR_KEEPALIVE_URL,
    BETFAIR_KEY_FILE,
    BETFAIR_PASSWORD,
    BETFAIR_SSID,
    BETFAIR_USERNAME,
)

BETFAIR_BETTING_URL = BETFAIR_API_URL


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
                "BETFAIR_CERT_FILE": {
                    "present": bool(BETFAIR_CERT_FILE),
                    "value": BETFAIR_CERT_FILE,
                },
                "BETFAIR_KEY_FILE": {
                    "present": bool(BETFAIR_KEY_FILE),
                    "value": BETFAIR_KEY_FILE,
                },
                "CERT_LOGIN_URL": CERT_LOGIN_URL,
                "BETFAIR_KEEPALIVE_URL": BETFAIR_KEEPALIVE_URL,
            },
            indent=2,
        )
    )


def _betting_headers():
    return build_api_headers()


def _keep_alive_headers():
    return {
        "X-Application": BETFAIR_APP_KEY or "",
        "X-Authentication": get_session_token(),
        "Accept": "application/json",
    }


def check_cert_login():
    print("\nChecking automatic session acquisition...")
    clear_session_token()
    token = get_session_token(force_refresh=True)
    print(
        json.dumps(
            {
                "session_present": bool(token),
                "session_masked": _mask(token),
            },
            indent=2,
        )
    )
    return token


def check_keep_alive():
    print("\nChecking session with keepAlive...")
    try:
        success = keep_alive()
        data = {"status": "SUCCESS" if success else "FAIL"}
    except Exception as exc:
        data = {"status": "FAIL", "error": str(exc)}
    print("HTTP 200" if data.get("status") == "SUCCESS" else "HTTP FAIL")
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

    if not BETFAIR_APP_KEY:
        raise SystemExit("Missing BETFAIR_APP_KEY. Update .env and try again.")

    check_cert_login()
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
