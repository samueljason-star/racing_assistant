from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd

from app.research.import_betfair_history import import_betfair_history
from app.research.import_punting_form import DEFAULT_MONTHLY_EXPORTS_DIR, import_punting_form
from app.research.match_races import match_races
from app.research.utils import BETFAIR_HISTORY_INPUT_DIR, RESEARCH_DATA_DIR, save_dataframe

MONTH_ABBREV_TO_NUM = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

TEMP_DIR = RESEARCH_DATA_DIR / "betfair_monthly"
COMBINED_BETFAIR_PATH = RESEARCH_DATA_DIR / "betfair_odds_clean.csv"


def _latest_monthly_export_zips(downloads_dir: Path) -> dict[int, Path]:
    latest_by_month: dict[int, Path] = {}
    for zip_path in sorted(downloads_dir.glob("*.zip")):
        month_name = zip_path.name.split("-", 1)[0].lower()
        month_num = MONTH_ABBREV_TO_NUM.get(month_name[:3])
        if month_num is None:
            continue
        latest_by_month[month_num] = zip_path
    return latest_by_month


def _zip_contains_2025_data(zip_path: Path) -> bool:
    try:
        with zipfile.ZipFile(zip_path) as archive:
            meeting_list = next((name for name in archive.namelist() if name.endswith("meetingList.json")), None)
            if meeting_list is None:
                return False
            payload = json.loads(archive.read(meeting_list))
            if not isinstance(payload, list) or not payload:
                return False
            dates = [
                row.get("MeetingDate")
                for row in payload
                if isinstance(row, dict) and row.get("MeetingDate")
            ]
            return bool(dates) and min(dates).startswith("2025-") and max(dates).startswith("2025-")
    except (OSError, zipfile.BadZipFile, json.JSONDecodeError):
        return False


def _available_form_months() -> set[int]:
    month_zips = _latest_monthly_export_zips(DEFAULT_MONTHLY_EXPORTS_DIR)
    months: set[int] = set()
    for month_num, zip_path in month_zips.items():
        if _zip_contains_2025_data(zip_path):
            months.add(month_num)
    return months


def _available_betfair_months() -> set[int]:
    root = BETFAIR_HISTORY_INPUT_DIR / "BASIC" / "2025"
    months: set[int] = set()
    if not root.exists():
        return months
    for path in root.iterdir():
        if not path.is_dir():
            continue
        month_num = MONTH_ABBREV_TO_NUM.get(path.name.lower()[:3])
        if month_num is not None:
            months.add(month_num)
    return months


def _month_dir(month_num: int) -> Path:
    month_abbrev = next(
        name.title()
        for name, number in MONTH_ABBREV_TO_NUM.items()
        if number == month_num
    )
    return BETFAIR_HISTORY_INPUT_DIR / "BASIC" / "2025" / month_abbrev


def build_2025_trial_dataset() -> pd.DataFrame:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    form_months = _available_form_months()
    betfair_months = _available_betfair_months()
    overlap = sorted(form_months & betfair_months)

    if not overlap:
        raise RuntimeError("No overlapping 2025 months were found between Punting Form exports and Betfair history.")

    print(f"2025 form months available: {sorted(form_months)}")
    print(f"2025 betfair months available: {sorted(betfair_months)}")
    print(f"2025 overlap months for trial: {overlap}")

    empty_raw_dir = Path("/tmp/ra_empty_punting_form")
    empty_raw_dir.mkdir(parents=True, exist_ok=True)
    import_punting_form(raw_api_input_dir=empty_raw_dir)

    month_frames: list[pd.DataFrame] = []
    for month_num in overlap:
        input_dir = _month_dir(month_num)
        output_path = TEMP_DIR / f"betfair_odds_clean_2025_{month_num:02d}.csv"
        print(f"Building Betfair clean history for 2025-{month_num:02d} from {input_dir}")
        frame = import_betfair_history(
            input_dir=input_dir,
            output_path=output_path,
            progress_every=500,
        )
        if not frame.empty:
            month_frames.append(frame)

    combined = pd.concat(month_frames, ignore_index=True) if month_frames else pd.DataFrame()
    save_dataframe(combined, COMBINED_BETFAIR_PATH)
    print(f"Combined Betfair rows written: {len(combined)}")

    matched = match_races()
    if not matched.empty:
        matched["race_date"] = pd.to_datetime(matched["race_date"], errors="coerce")
        matched = matched[matched["race_date"].dt.year == 2025].copy()
        matched["race_date"] = matched["race_date"].dt.date.astype(str)

    output_path = RESEARCH_DATA_DIR / "matched_runner_data_2025_trial.csv"
    save_dataframe(matched, output_path)
    print(f"2025 matched trial rows written: {len(matched)} -> {output_path}")
    return matched


def main() -> None:
    build_2025_trial_dataset()


if __name__ == "__main__":
    main()
