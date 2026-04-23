import os
import json
import requests
from dotenv import load_dotenv

from app.db import SessionLocal
from app.models import Race, Runner, OddsSnapshot

load_dotenv()

APP_KEY = os.getenv("APP_KEY")
SESSION_TOKEN = os.getenv("BETFAIR_SESSION_TOKEN")
BETFAIR_API_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"


def fetch_market_books(market_ids):
    headers = {
        "X-Application": APP_KEY,
        "X-Authentication": SESSION_TOKEN,
        "Content-Type": "application/json",
    }

    payload = [
        {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketBook",
            "params": {
                "marketIds": market_ids,
                "priceProjection": {
                    "priceData": ["EX_BEST_OFFERS"]
                }
            },
            "id": 1,
        }
    ]

    response = requests.post(
        BETFAIR_API_URL,
        data=json.dumps(payload),
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()[0]["result"]


def save_odds():
    print("SAVE_ODDS FUNCTION RUNNING")
    db = SessionLocal()

    races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
    print(f"RACES WITH BETFAIR IDS: {len(races)}")

    market_ids = [r.betfair_market_id for r in races if r.betfair_market_id]

    if not market_ids:
        print("No markets found.")
        db.close()
        return

    books = fetch_market_books(market_ids)
    print(f"BOOKS RETURNED: {len(books)}")

    count = 0

    for book in books:
        market_id = book.get("marketId")

        race = db.query(Race).filter(Race.betfair_market_id == market_id).first()
        if not race:
            continue

        for runner_book in book.get("runners", []):
            selection_id = str(runner_book.get("selectionId"))

            runner = db.query(Runner).filter(
                Runner.race_id == race.id,
                Runner.betfair_selection_id == selection_id
            ).first()

            if not runner:
                continue

            prices = runner_book.get("ex", {}).get("availableToBack", [])
            if not prices:
                continue

            price = prices[0].get("price")
            if not price:
                continue

            db.add(OddsSnapshot(
                race_id=race.id,
                runner_id=runner.id,
                odds=float(price),
                source="betfair"
            ))
            count += 1

    db.commit()
    db.close()

    print(f"MARKETS: {len(books)}")
    print(f"SNAPSHOTS: {count}")


if __name__ == "__main__":
    print("SAVE_ODDS FILE STARTED")
    save_odds()