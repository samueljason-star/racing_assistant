from __future__ import annotations

from datetime import datetime, timezone

from app.db import SessionLocal
from app.models import HorseHistory, Race, Result, Runner
from app.utils.name_matching import horse_names_match, normalize_horse_name


def _parse_jump_time(value: str | None) -> datetime | None:
    """Parse a stored jump time into a datetime when possible."""
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def get_completed_races(db):
    """Return races with a non-null jump time that is already in the past."""
    now = datetime.now(timezone.utc)
    completed = []

    races = db.query(Race).filter(Race.jump_time.isnot(None)).all()
    for race in races:
        jump_time = _parse_jump_time(race.jump_time)
        if jump_time and jump_time < now:
            completed.append(race)

    return completed


def fetch_result_data_for_race(race):
    """Return mocked structured result data for a race."""
    # TODO: Replace this mock with a real external results source once the
    # production provider and parsing logic are finalized.
    ordered_runners = sorted(getattr(race, "runners", []), key=lambda runner: runner.id)

    return [
        {
            "horse_name": runner.horse_name,
            "finish_position": index,
            "margin": 0.0 if index == 1 else round((index - 1) * 0.5, 2),
            "starting_price": None,
        }
        for index, runner in enumerate(ordered_runners, start=1)
    ]


def find_matching_runner(db, race_id, horse_name):
    """Find the runner in a race using exact then normalized name matching."""
    exact = db.query(Runner).filter(
        Runner.race_id == race_id,
        Runner.horse_name == horse_name,
    ).first()
    if exact:
        return exact

    normalized_target = normalize_horse_name(horse_name)
    runners = db.query(Runner).filter(Runner.race_id == race_id).all()
    for runner in runners:
        if horse_names_match(runner.horse_name, normalized_target):
            return runner

    return None


def upsert_result(db, race_id, runner_id, result_row):
    """Insert or update a result row for the matched runner."""
    existing = db.query(Result).filter(
        Result.race_id == race_id,
        Result.runner_id == runner_id,
    ).first()

    if existing:
        existing.finish_position = result_row.get("finish_position")
        existing.margin = result_row.get("margin")
        existing.starting_price = result_row.get("starting_price")
        return existing, False

    created = Result(
        race_id=race_id,
        runner_id=runner_id,
        finish_position=result_row.get("finish_position"),
        margin=result_row.get("margin"),
        starting_price=result_row.get("starting_price"),
    )
    db.add(created)
    return created, True


def append_horse_history(db, race, runner, result_row):
    """Append a completed run into horse history when it is not already present."""
    meeting = getattr(race, "meeting", None)
    run_date = None
    if race.jump_time:
        parsed = _parse_jump_time(race.jump_time)
        if parsed:
            run_date = parsed.date().isoformat()
    if not run_date and meeting:
        run_date = meeting.date

    track = meeting.track if meeting else None
    distance = race.distance

    exists = db.query(HorseHistory).filter(
        HorseHistory.horse_name == runner.horse_name,
        HorseHistory.run_date == run_date,
        HorseHistory.track == track,
        HorseHistory.distance == distance,
    ).first()
    if exists:
        return False

    db.add(
        HorseHistory(
            horse_name=runner.horse_name,
            run_date=run_date,
            track=track,
            distance=distance,
            class_name=race.class_name,
            barrier=runner.barrier,
            weight=runner.weight,
            jockey=runner.jockey,
            trainer=runner.trainer,
            track_condition=race.track_condition,
            finish_position=result_row.get("finish_position"),
            margin=result_row.get("margin"),
            starting_price=result_row.get("starting_price"),
            source="results_pipeline",
        )
    )
    return True


def update_results():
    """Update results and append completed runs into horse history."""
    db = SessionLocal()

    try:
        completed_races = get_completed_races(db)
        results_upserted = 0
        history_rows_added = 0
        runners_unmatched = 0

        for race in completed_races:
            result_rows = fetch_result_data_for_race(race)

            for result_row in result_rows:
                runner = find_matching_runner(db, race.id, result_row["horse_name"])
                if not runner:
                    runners_unmatched += 1
                    continue

                _, created = upsert_result(db, race.id, runner.id, result_row)
                if created:
                    results_upserted += 1
                else:
                    results_upserted += 1

                if append_horse_history(db, race, runner, result_row):
                    history_rows_added += 1

        db.commit()

        print(f"COMPLETED RACES CHECKED: {len(completed_races)}")
        print(f"RESULTS UPSERTED: {results_upserted}")
        print(f"HORSE HISTORY ROWS ADDED: {history_rows_added}")
        print(f"RUNNERS UNMATCHED: {runners_unmatched}")
    finally:
        db.close()


if __name__ == "__main__":
    update_results()
