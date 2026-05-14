from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.db import SessionLocal, init_db
from app.models import OddsSnapshot, Race, Runner
from app.research.utils import RESEARCH_DATA_DIR, RESEARCH_REPORTS_DIR, save_dataframe
from app.strategy.value_bets import _parse_jump_time

OUTPUT_PATH = RESEARCH_DATA_DIR / "odds_time_series.csv"
QUALITY_PATH = RESEARCH_REPORTS_DIR / "odds_time_series_quality.csv"
CHECKPOINTS = (60, 30, 10, 5, 3, 1)
OUTPUT_COLUMNS = [
    "race_id",
    "runner_id",
    "race_date",
    "track",
    "race_number",
    "horse_name",
    "jump_time",
    "odds_60m",
    "odds_30m",
    "odds_10m",
    "odds_5m",
    "odds_3m",
    "odds_1m",
    "latest_odds",
    "missing_60m_flag",
    "missing_30m_flag",
    "missing_10m_flag",
    "missing_5m_flag",
    "missing_3m_flag",
    "missing_1m_flag",
    "complete_time_series_flag",
]


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _snapshot_at_or_before(
    snapshots: list[tuple[datetime, float]],
    cutoff: datetime,
) -> float | None:
    chosen_odds = None
    for timestamp, odds in snapshots:
        if timestamp <= cutoff:
            chosen_odds = odds
        else:
            break
    return chosen_odds


def _latest_pre_jump_odds(
    snapshots: list[tuple[datetime, float]],
    jump_time: datetime,
) -> float | None:
    latest_odds = None
    for timestamp, odds in snapshots:
        if timestamp <= jump_time:
            latest_odds = odds
        else:
            break
    return latest_odds


def build_odds_time_series(
    output_path: Path = OUTPUT_PATH,
    quality_path: Path = QUALITY_PATH,
) -> dict[str, pd.DataFrame]:
    init_db()
    db = SessionLocal()
    try:
        races = db.query(Race).all()
        runners = db.query(Runner).all()
        snapshots = (
            db.query(OddsSnapshot)
            .order_by(OddsSnapshot.runner_id.asc(), OddsSnapshot.timestamp.asc(), OddsSnapshot.id.asc())
            .all()
        )

        race_by_id = {race.id: race for race in races}
        runners_by_race: dict[int, list[Runner]] = defaultdict(list)
        for runner in runners:
            runners_by_race[runner.race_id].append(runner)

        snapshots_by_runner: dict[int, list[tuple[datetime, float]]] = defaultdict(list)
        for snapshot in snapshots:
            timestamp = _ensure_utc(snapshot.timestamp)
            odds = float(snapshot.odds) if snapshot.odds is not None else None
            if timestamp is None or odds is None or odds <= 0:
                continue
            snapshots_by_runner[snapshot.runner_id].append((timestamp, odds))

        rows: list[dict[str, object]] = []
        processed_race_ids: set[int] = set()

        for race_id, race_runners in runners_by_race.items():
            race = race_by_id.get(race_id)
            if race is None:
                continue
            jump_time = _parse_jump_time(race.jump_time)
            if jump_time is None:
                continue

            processed_race_ids.add(race_id)
            jump_time = _ensure_utc(jump_time)
            if jump_time is None:
                continue

            for runner in race_runners:
                runner_snapshots = snapshots_by_runner.get(runner.id, [])
                checkpoint_values: dict[int, float | None] = {}
                missing_flags: dict[int, bool] = {}

                for checkpoint in CHECKPOINTS:
                    cutoff = jump_time - pd.Timedelta(minutes=checkpoint)
                    odds_value = _snapshot_at_or_before(runner_snapshots, cutoff)
                    checkpoint_values[checkpoint] = odds_value
                    missing_flags[checkpoint] = odds_value is None

                latest_odds = _latest_pre_jump_odds(runner_snapshots, jump_time)
                row = {
                    "race_id": race.id,
                    "runner_id": runner.id,
                    "race_date": jump_time.date().isoformat(),
                    "track": getattr(getattr(race, "meeting", None), "track", None),
                    "race_number": race.race_number,
                    "horse_name": runner.horse_name,
                    "jump_time": jump_time.isoformat(),
                    "odds_60m": checkpoint_values[60],
                    "odds_30m": checkpoint_values[30],
                    "odds_10m": checkpoint_values[10],
                    "odds_5m": checkpoint_values[5],
                    "odds_3m": checkpoint_values[3],
                    "odds_1m": checkpoint_values[1],
                    "latest_odds": latest_odds,
                    "missing_60m_flag": missing_flags[60],
                    "missing_30m_flag": missing_flags[30],
                    "missing_10m_flag": missing_flags[10],
                    "missing_5m_flag": missing_flags[5],
                    "missing_3m_flag": missing_flags[3],
                    "missing_1m_flag": missing_flags[1],
                    "complete_time_series_flag": not any(missing_flags.values()) and latest_odds is not None,
                }
                rows.append(row)

        odds_frame = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
        if not odds_frame.empty:
            odds_frame = odds_frame.sort_values(
                ["jump_time", "race_id", "runner_id"],
                ascending=[True, True, True],
            )

        total_runners = len(odds_frame)
        quality_rows = []
        for checkpoint in CHECKPOINTS:
            missing_col = f"missing_{checkpoint}m_flag"
            valid_count = int((~odds_frame[missing_col]).sum()) if total_runners else 0
            missing_count = int(odds_frame[missing_col].sum()) if total_runners else 0
            quality_rows.append(
                {
                    "checkpoint": f"{checkpoint}m",
                    "valid_runners": valid_count,
                    "missing_runners": missing_count,
                    "valid_pct": round(valid_count / total_runners, 4) if total_runners else 0.0,
                    "missing_pct": round(missing_count / total_runners, 4) if total_runners else 0.0,
                }
            )

        quality_frame = pd.DataFrame(quality_rows)

        save_dataframe(odds_frame, output_path)
        save_dataframe(quality_frame, quality_path)

        complete_count = int(odds_frame["complete_time_series_flag"].sum()) if total_runners else 0
        complete_pct = round(complete_count / total_runners, 4) if total_runners else 0.0

        print("Odds Time Series Build Summary")
        print(f"TOTAL RACES PROCESSED: {len(processed_race_ids)}")
        print(f"TOTAL RUNNERS: {total_runners}")
        print(f"COMPLETE TIME-SERIES COVERAGE: {complete_pct:.2%}")
        print("TOP MISSING CHECKPOINT STATS")
        for row in quality_frame.sort_values("missing_pct", ascending=False).to_dict("records"):
            print(
                f"checkpoint={row['checkpoint']} | "
                f"valid_pct={row['valid_pct']:.2%} | "
                f"missing_pct={row['missing_pct']:.2%} | "
                f"valid_runners={row['valid_runners']} | "
                f"missing_runners={row['missing_runners']}"
            )

        return {"odds_time_series": odds_frame, "quality": quality_frame}
    finally:
        db.close()


def main() -> None:
    build_odds_time_series()


if __name__ == "__main__":
    main()
