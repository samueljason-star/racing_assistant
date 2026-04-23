import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betfair.session import post_json_rpc


def _build_headers():
    return None


def _extract_result(data):
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"Unexpected Betfair response shape: {data!r}")

    first_item = data[0]
    if "error" in first_item:
        error = first_item["error"]
        details = error.get("data", {}).get("APINGException", {})
        error_code = details.get("errorCode") or error.get("message", "UNKNOWN")
        request_uuid = details.get("requestUUID", "unknown")
        raise RuntimeError(
            f"Betfair API error: {error_code}. requestUUID={request_uuid}"
        )

    if "result" not in first_item:
        raise RuntimeError(f"Betfair response missing result: {first_item!r}")

    return first_item["result"]


def is_au_thoroughbred_market(market):
    event = market.get("event", {})
    market_name = (market.get("marketName") or "").lower()
    event_name = (event.get("name") or "").lower()
    venue = (event.get("venue") or "").lower()
    country = event.get("countryCode")

    if country != "AU":
        return False

    exclude_terms = ["pace", "trot", "harness", "trotting", "pacing"]
    combined = f"{market_name} {event_name} {venue}"
    if any(term in combined for term in exclude_terms):
        return False

    return market_name.startswith("r")


def fetch_au_thoroughbred_win_markets():
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=1)

    payload = [
        {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {
                    "eventTypeIds": ["7"],
                    "marketTypeCodes": ["WIN"],
                    "marketCountries": ["AU"],
                    "marketStartTime": {
                        "from": now.isoformat().replace("+00:00", "Z"),
                        "to": end.isoformat().replace("+00:00", "Z"),
                    },
                },
                "maxResults": "300",
                "marketProjection": [
                    "RUNNER_DESCRIPTION",
                    "EVENT",
                    "MARKET_START_TIME",
                ],
                "sort": "FIRST_TO_START",
            },
            "id": 1,
        }
    ]

    raw_markets = _extract_result(post_json_rpc(payload, timeout=30))
    filtered = [market for market in raw_markets if is_au_thoroughbred_market(market)]
    return raw_markets, filtered


def list_au_thoroughbred_win_markets():
    raw_markets, filtered = fetch_au_thoroughbred_win_markets()

    print(f"RAW MARKETS: {len(raw_markets)}")
    print(f"FILTERED MARKETS: {len(filtered)}")

    for market in filtered[:20]:
        print(
            market["marketId"],
            "|",
            market["marketName"],
            "|",
            market["event"]["name"],
            "|",
            market["event"]["countryCode"],
        )


if __name__ == "__main__":
    list_au_thoroughbred_win_markets()
