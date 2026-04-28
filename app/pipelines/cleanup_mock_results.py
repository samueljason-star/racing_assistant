from __future__ import annotations

import argparse
from collections import defaultdict

from app.db import SessionLocal, init_db
from app.models import HorseHistory, Race, Result, Runner


def _is_placeholder_result_set(runner_count: int, results: list[Result]) -> bool:
    if runner_count <= 0 or len(results) != runner_count:
        return False

    ordered = sorted(results, key=lambda item: item.finish_position or 0)
    expected_positions = list(range(1, runner_count + 1))
    actual_positions = [result.finish_position for result in ordered]
    if actual_positions != expected_positions:
        return False

    for index, result in enumerate(ordered, start=1):
        expected_margin = 0.0 if index == 1 else round((index - 1) * 0.5, 2)
        if result.starting_price is not None:
            return False
        if result.margin != expected_margin:
            return False

    return True


def cleanup_mock_results(apply: bool = False) -> None:
    init_db()
    db = SessionLocal()

    try:
        result_rows = db.query(Result).filter(Result.finish_position.isnot(None)).all()
        results_by_race = defaultdict(list)
        for row in result_rows:
            results_by_race[row.race_id].append(row)

        actual_runner_counts = defaultdict(int)
        for runner in db.query(Runner).all():
            actual_runner_counts[runner.race_id] += 1

        unsafe_race_ids = [
            race_id
            for race_id, rows in results_by_race.items()
            if _is_placeholder_result_set(actual_runner_counts.get(race_id, 0), rows)
        ]

        unsafe_result_count = sum(len(results_by_race[race_id]) for race_id in unsafe_race_ids)
        impacted_history_count = 0
        if unsafe_race_ids:
            races = db.query(Race).filter(Race.id.in_(unsafe_race_ids)).all()
            for race in races:
                meeting = getattr(race, "meeting", None)
                run_date = meeting.date if meeting else None
                runners = db.query(Runner).filter(Runner.race_id == race.id).all()
                horse_names = [runner.horse_name for runner in runners]
                impacted_history_count += db.query(HorseHistory).filter(
                    HorseHistory.source == "results_pipeline",
                    HorseHistory.horse_name.in_(horse_names),
                    HorseHistory.run_date == run_date,
                ).count()

        if apply and unsafe_race_ids:
            db.query(Result).filter(Result.race_id.in_(unsafe_race_ids)).delete(synchronize_session=False)
            db.commit()

        print(f"UNSAFE MOCK RACES IDENTIFIED: {len(unsafe_race_ids)}")
        print(f"UNSAFE MOCK RESULT ROWS IDENTIFIED: {unsafe_result_count}")
        print(f"RELATED GENERATED HORSE HISTORY ROWS DETECTED (NOT DELETED): {impacted_history_count}")
        print(f"APPLY MODE: {apply}")
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect and optionally remove unsafe placeholder results.")
    parser.add_argument("--apply", action="store_true", help="Delete identified unsafe mock Result rows.")
    args = parser.parse_args()
    cleanup_mock_results(apply=args.apply)


if __name__ == "__main__":
    main()
