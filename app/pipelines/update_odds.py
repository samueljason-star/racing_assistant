from app.db import SessionLocal
from app.models import Runner, OddsSnapshot


def update_odds() -> None:
    db = SessionLocal()
    try:
        odds_map = {
            "Fast One": 2.80,
            "River Boy": 4.20,
            "Silver Star": 3.50,
        }

        runners = db.query(Runner).all()
        for runner in runners:
            odds = odds_map.get(runner.horse_name)
            if odds:
                snapshot = OddsSnapshot(
                    race_id=runner.race_id,
                    runner_id=runner.id,
                    odds=odds,
                    source="sample",
                )
                db.add(snapshot)

        db.commit()
        print("Odds updated.")
    finally:
        db.close()
