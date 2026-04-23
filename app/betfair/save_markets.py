import re
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betfair.get_markets import fetch_au_thoroughbred_win_markets
from app.db import SessionLocal, init_db
from app.models import Meeting, Race, Runner

RACE_NUMBER_RE = re.compile(r"^R(\d+)\b", re.IGNORECASE)
DISTANCE_RE = re.compile(r"\b(\d{3,4})m\b", re.IGNORECASE)


def _parse_market_date(open_date):
    if not open_date:
        return None
    parsed = datetime.fromisoformat(open_date.replace("Z", "+00:00"))
    return parsed.date().isoformat()


def _parse_race_number(market_name):
    match = RACE_NUMBER_RE.search(market_name or "")
    return int(match.group(1)) if match else None


def _parse_distance(market_name):
    match = DISTANCE_RE.search(market_name or "")
    return int(match.group(1)) if match else None


def _parse_class_name(market_name):
    parts = (market_name or "").split()
    if len(parts) <= 2:
        return None
    return " ".join(parts[2:])


def _upsert_meeting(db, market):
    event = market.get("event", {})
    track = event.get("venue") or event.get("name") or "UNKNOWN"
    date = _parse_market_date(event.get("openDate")) or _parse_market_date(
        market.get("marketStartTime")
    )

    meeting = db.query(Meeting).filter(
        Meeting.date == date,
        Meeting.track == track,
    ).first()
    if meeting:
        return meeting, False

    meeting = Meeting(
        date=date,
        track=track,
        state=event.get("countryCode"),
        meeting_type="THOROUGHBRED",
    )
    db.add(meeting)
    db.flush()
    return meeting, True


def _upsert_race(db, meeting, market):
    event = market.get("event", {})
    market_name = market.get("marketName") or ""
    race_number = _parse_race_number(market_name)
    jump_time = market.get("marketStartTime")
    distance = _parse_distance(market_name)
    class_name = _parse_class_name(market_name)
    field_size = len(market.get("runners") or [])

    race = None
    if race_number is not None:
        race = db.query(Race).filter(
            Race.meeting_id == meeting.id,
            Race.race_number == race_number,
        ).first()

    if race:
        race.betfair_market_id = market.get("marketId")
        race.jump_time = jump_time
        race.distance = distance
        race.class_name = class_name
        race.field_size = field_size
        race.track_condition = race.track_condition or event.get("timezone")
        return race, False

    race = Race(
        meeting_id=meeting.id,
        betfair_market_id=market.get("marketId"),
        race_number=race_number or 0,
        jump_time=jump_time,
        distance=distance,
        class_name=class_name,
        track_condition=event.get("timezone"),
        field_size=field_size,
    )
    db.add(race)
    db.flush()
    return race, True


def _upsert_runners(db, race, market):
    created = 0
    for runner in market.get("runners") or []:
        horse_name = runner.get("runnerName")
        if not horse_name:
            continue

        exists = db.query(Runner).filter(
            Runner.race_id == race.id,
            Runner.horse_name == horse_name,
        ).first()
        if exists:
            continue

        db.add(
            Runner(
                race_id=race.id,
                horse_name=horse_name,
                scratching_flag=False,
            )
        )
        created += 1
    return created


def save_markets():
    init_db()
    raw_markets, filtered_markets = fetch_au_thoroughbred_win_markets()

    db = SessionLocal()
    try:
        meeting_creates = 0
        race_creates = 0
        runner_creates = 0

        for market in filtered_markets:
            meeting, meeting_created = _upsert_meeting(db, market)
            race, race_created = _upsert_race(db, meeting, market)
            runner_creates += _upsert_runners(db, race, market)

            if meeting_created:
                meeting_creates += 1
            if race_created:
                race_creates += 1

        db.commit()

        print(f"RAW MARKETS: {len(raw_markets)}")
        print(f"FILTERED MARKETS: {len(filtered_markets)}")
        print(f"MEETINGS CREATED: {meeting_creates}")
        print(f"RACES CREATED: {race_creates}")
        print(f"RUNNERS CREATED: {runner_creates}")
    finally:
        db.close()


if __name__ == "__main__":
    save_markets()
