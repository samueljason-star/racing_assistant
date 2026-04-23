import json
import re
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betfair.get_markets import fetch_au_thoroughbred_win_markets
from app.betfair.session import post_json_rpc
from app.db import SessionLocal, init_db
from app.models import Meeting, OddsSnapshot, Race, Runner

RACE_NUMBER_RE = re.compile(r"^R(\d+)\b", re.IGNORECASE)


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


def _parse_race_number(market_name):
    match = RACE_NUMBER_RE.search(market_name or "")
    return int(match.group(1)) if match else None


def _best_back_price(runner_book):
    ex = runner_book.get("ex", {})
    available = ex.get("availableToBack") or []
    if not available:
        return None
    return available[0].get("price")


def _fetch_market_books(market_ids):
    if not market_ids:
        return []

    payload = [
        {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketBook",
            "params": {
                "marketIds": market_ids,
                "priceProjection": {
                    "priceData": ["EX_BEST_OFFERS"],
                    "virtualise": True,
                    "exBestOffersOverrides": {"bestPricesDepth": 1},
                },
            },
            "id": 1,
        }
    ]

    return _extract_result(post_json_rpc(payload, timeout=30))


def save_odds():
    init_db()
    _, markets = fetch_au_thoroughbred_win_markets()
    market_books = _fetch_market_books([market["marketId"] for market in markets])
    market_books_by_id = {market_book["marketId"]: market_book for market_book in market_books}

    db = SessionLocal()
    try:
        saved_snapshots = 0
        skipped_markets = 0
        skipped_runners = 0

        for market in markets:
            event = market.get("event", {})
            track = event.get("venue") or event.get("name")
            date = (
                event.get("openDate", "").replace("T", " ").split(" ")[0]
                or market.get("marketStartTime", "").replace("T", " ").split(" ")[0]
            )
            race_number = _parse_race_number(market.get("marketName"))

            if not track or race_number is None:
                skipped_markets += 1
                continue

            meeting = db.query(Meeting).filter(
                Meeting.track == track,
                Meeting.date == date,
            ).first()
            if not meeting:
                skipped_markets += 1
                continue

            race = db.query(Race).filter(
                Race.meeting_id == meeting.id,
                Race.race_number == race_number,
            ).first()
            if not race:
                skipped_markets += 1
                continue

            runner_map = {
                runner.horse_name: runner
                for runner in db.query(Runner).filter(Runner.race_id == race.id).all()
            }
            market_book = market_books_by_id.get(market["marketId"], {})
            runner_books = market_book.get("runners") or []

            for market_runner in market.get("runners") or []:
                runner_name = market_runner.get("runnerName")
                runner = runner_map.get(runner_name)
                if not runner:
                    skipped_runners += 1
                    continue

                runner_book = next(
                    (
                        item
                        for item in runner_books
                        if item.get("selectionId") == market_runner.get("selectionId")
                    ),
                    None,
                )
                if not runner_book:
                    skipped_runners += 1
                    continue

                odds = _best_back_price(runner_book)
                if odds is None:
                    skipped_runners += 1
                    continue

                db.add(
                    OddsSnapshot(
                        race_id=race.id,
                        runner_id=runner.id,
                        odds=odds,
                        source="betfair",
                    )
                )
                saved_snapshots += 1

        db.commit()
        print(f"MARKETS PROCESSED: {len(markets)}")
        print(f"ODDS SNAPSHOTS SAVED: {saved_snapshots}")
        print(f"MARKETS SKIPPED: {skipped_markets}")
        print(f"RUNNERS SKIPPED: {skipped_runners}")
    finally:
        db.close()


if __name__ == "__main__":
    save_odds()
