from app.db import SessionLocal
from app.models import Meeting, Race, Runner


def update_runners() -> None:
    db = SessionLocal()
    try:
        meeting = db.query(Meeting).filter(Meeting.track == "Eagle Farm").first()
        if not meeting:
            print("Meeting not found.")
            return

        race = db.query(Race).filter(
            Race.meeting_id == meeting.id,
            Race.race_number == 1,
        ).first()

        if not race:
            race = Race(
                meeting_id=meeting.id,
                race_number=1,
                jump_time="13:00",
                distance=1200,
                class_name="BM70",
                track_condition="Good",
                field_size=3,
            )
            db.add(race)
            db.commit()
            db.refresh(race)

        sample_runners = [
            {"horse_name": "Fast One", "barrier": 1, "weight": 56.5, "jockey": "J Smith", "trainer": "T Brown"},
            {"horse_name": "River Boy", "barrier": 4, "weight": 57.0, "jockey": "A Jones", "trainer": "L White"},
            {"horse_name": "Silver Star", "barrier": 7, "weight": 55.5, "jockey": "M Lee", "trainer": "D Green"},
        ]

        for item in sample_runners:
            exists = db.query(Runner).filter(
                Runner.race_id == race.id,
                Runner.horse_name == item["horse_name"],
            ).first()
            if not exists:
                db.add(Runner(race_id=race.id, **item))

        db.commit()
        print("Runners updated.")
    finally:
        db.close()
