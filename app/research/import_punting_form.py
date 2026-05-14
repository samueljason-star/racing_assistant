from __future__ import annotations

import json
import re
import zipfile
from pathlib import Path

import pandas as pd

from app.research.utils import (
    PUNTING_FORM_INPUT_DIR,
    RAW_PUNTING_FORM_INPUT_DIR,
    RESEARCH_DATA_DIR,
    ROOT_DIR,
    attach_common_labels,
    average,
    clean_horse_name,
    clean_text,
    first_present,
    normalize_column_name,
    parse_date,
    parse_distance,
    parse_finish_position,
    parse_list_numbers,
    parse_margin,
    parse_money,
    parse_price,
    parse_float,
    parse_int,
    read_csv_flex,
    save_dataframe,
)

OUTPUT_PATH = RESEARCH_DATA_DIR / "punting_form_clean.csv"
DEFAULT_MONTHLY_EXPORTS_DIR = ROOT_DIR / "data" / "punting_form_monthly_exports"
FALLBACK_MONTHLY_EXPORTS_DIR = Path("/Users/sam/Downloads")

ALIASES = {
    "race_date": ["race_date", "meeting_date", "date", "meetingdate", "startdate"],
    "track": ["track", "track_name", "meeting", "trackname", "venue"],
    "race_number": ["race_number", "racenumber", "race_no", "race"],
    "horse_name": ["horse_name", "horse", "runner_name", "runner", "entityname"],
    "barrier": ["barrier", "gate", "draw"],
    "jockey": ["jockey", "jockey_name", "jockeyfullname"],
    "trainer": ["trainer", "trainer_name", "trainerfullname"],
    "weight": ["weight", "riding_weight", "allocated_weight"],
    "distance": ["distance", "race_distance"],
    "class_name": ["class_name", "class", "grade", "racetype"],
    "track_condition": ["track_condition", "trackcondition", "condition"],
    "finish_position": ["finish_position", "finish", "position", "finishpos"],
    "margin": ["margin", "beaten_margin", "margin_beaten"],
    "starting_price": ["starting_price", "sp", "price", "startingprice"],
    "prize_money": ["prize_money", "prizemoney", "prize"],
    "last_start_finish": ["last_start_finish", "laststartfinish"],
    "last_3_finishes": ["last_3_finishes", "last3finishes", "last_three_finishes"],
    "last_3_margins": ["last_3_margins", "last3margins", "last_three_margins"],
}


def _fallback_last_three(row: pd.Series, prefix: str) -> list[float]:
    values = []
    for index in range(1, 4):
        candidate = first_present(
            row,
            [
                f"{prefix}_{index}",
                f"{prefix}{index}",
                f"last_{index}_{prefix}",
                f"last{index}{prefix}",
            ],
        )
        parsed = parse_float(candidate)
        if parsed is not None:
            values.append(parsed)
    return values


def _parse_last10_finishes(value: object) -> list[float]:
    text = clean_text(value)
    if text is None:
        return []
    digits = [float(char) for char in text if char.isdigit()]
    if not digits:
        return []
    return list(reversed(digits[-3:]))


def _row_to_clean_record(row: pd.Series, source_file: Path) -> dict[str, object]:
    last_3_finishes = parse_list_numbers(first_present(row, ALIASES["last_3_finishes"]))
    if not last_3_finishes:
        last_3_finishes = _fallback_last_three(row, "finish")
    if not last_3_finishes:
        last_3_finishes = _parse_last10_finishes(first_present(row, ["last10"]))

    last_3_margins = parse_list_numbers(first_present(row, ALIASES["last_3_margins"]))
    if not last_3_margins:
        last_3_margins = _fallback_last_three(row, "margin")

    last_start_finish = parse_finish_position(first_present(row, ALIASES["last_start_finish"]))
    if last_start_finish is None and last_3_finishes:
        last_start_finish = int(last_3_finishes[0])

    record = {
        "race_date": parse_date(first_present(row, ALIASES["race_date"])),
        "track": clean_text(first_present(row, ALIASES["track"])),
        "race_number": parse_finish_position(first_present(row, ALIASES["race_number"])),
        "horse_name": clean_horse_name(first_present(row, ALIASES["horse_name"])),
        "barrier": parse_finish_position(first_present(row, ALIASES["barrier"])),
        "jockey": clean_text(first_present(row, ALIASES["jockey"])),
        "trainer": clean_text(first_present(row, ALIASES["trainer"])),
        "weight": parse_float(first_present(row, ALIASES["weight"])),
        "distance": parse_distance(first_present(row, ALIASES["distance"])),
        "class_name": clean_text(first_present(row, ALIASES["class_name"])),
        "track_condition": clean_text(first_present(row, ALIASES["track_condition"])),
        "finish_position": parse_finish_position(first_present(row, ALIASES["finish_position"])),
        "margin": parse_margin(first_present(row, ALIASES["margin"])),
        "starting_price": parse_price(first_present(row, ALIASES["starting_price"])),
        "prize_money": parse_money(first_present(row, ALIASES["prize_money"])),
        "last_start_finish": last_start_finish,
        "last_3_finishes": "|".join(str(int(value)) for value in last_3_finishes[:3]) if last_3_finishes else None,
        "last_3_margins": "|".join(f"{value:.2f}" for value in last_3_margins[:3]) if last_3_margins else None,
        "average_last_3_finish": average(last_3_finishes[:3]),
        "average_last_3_margin": average(last_3_margins[:3]),
        "source_file": str(source_file),
    }
    return record


def _parse_json_payload(path: Path) -> dict | list | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _json_record_from_runner(
    *,
    meeting_date: object,
    track_name: object,
    race_number: object,
    distance: object,
    class_name: object,
    track_condition: object,
    runner: dict,
    source_file: Path,
    prize_money: object = None,
    finish_position: object = None,
    margin: object = None,
    starting_price: object = None,
) -> dict[str, object]:
    last_3_finishes = _parse_last10_finishes(runner.get("last10"))
    return {
        "race_date": parse_date(meeting_date),
        "track": clean_text(track_name),
        "race_number": parse_int(race_number),
        "horse_name": clean_horse_name(runner.get("name") or runner.get("runner")),
        "barrier": parse_int(runner.get("barrier") or runner.get("originalBarrier")),
        "jockey": clean_text((runner.get("jockey") or {}).get("fullName") if isinstance(runner.get("jockey"), dict) else runner.get("jockey")),
        "trainer": clean_text((runner.get("trainer") or {}).get("fullName") if isinstance(runner.get("trainer"), dict) else runner.get("trainer")),
        "weight": parse_float(runner.get("weight") or runner.get("weightTotal") or runner.get("weightAllocated")),
        "distance": parse_distance(distance),
        "class_name": clean_text(class_name),
        "track_condition": clean_text(track_condition),
        "finish_position": parse_finish_position(finish_position if finish_position is not None else runner.get("position")),
        "margin": parse_margin(margin if margin is not None else runner.get("margin")),
        "starting_price": parse_price(starting_price if starting_price is not None else runner.get("priceSP") or runner.get("price")),
        "prize_money": parse_money(prize_money if prize_money is not None else runner.get("prizeMoney")),
        "last_start_finish": int(last_3_finishes[0]) if last_3_finishes else None,
        "last_3_finishes": "|".join(str(int(value)) for value in last_3_finishes[:3]) if last_3_finishes else None,
        "last_3_margins": None,
        "average_last_3_finish": average(last_3_finishes[:3]),
        "average_last_3_margin": None,
        "source_file": str(source_file),
        "source_kind": "punting_form_api",
    }


def _parse_export_forms(runner: dict) -> tuple[list[float], list[float]]:
    forms = runner.get("Forms") or runner.get("forms") or []
    finishes: list[float] = []
    margins: list[float] = []
    for form in forms:
        if not isinstance(form, dict):
            continue
        if bool(form.get("IsBarrierTrial") or form.get("isBarrierTrial")):
            continue
        finish = parse_finish_position(form.get("Position") or form.get("position"))
        margin = parse_margin(form.get("Margin") or form.get("margin"))
        if finish is not None:
            finishes.append(float(finish))
        if margin is not None:
            margins.append(float(margin))
        if len(finishes) >= 3 and len(margins) >= 3:
            break
    return finishes[:3], margins[:3]


def _monthly_export_runner_to_record(
    *,
    meeting_date: object,
    track_name: object,
    race_number: object,
    runner: dict,
    source_file: Path,
) -> dict[str, object]:
    forms_finishes, forms_margins = _parse_export_forms(runner)
    if not forms_finishes:
        forms_finishes = _parse_last10_finishes(runner.get("Last10"))

    trainer = runner.get("Trainer") if isinstance(runner.get("Trainer"), dict) else {}
    jockey = runner.get("Jockey") if isinstance(runner.get("Jockey"), dict) else {}

    return {
        "race_date": parse_date(meeting_date),
        "track": clean_text(track_name),
        "race_number": parse_int(race_number),
        "horse_name": clean_horse_name(runner.get("Name") or runner.get("Runner")),
        "barrier": parse_int(runner.get("Barrier") or runner.get("OriginalBarrier")),
        "jockey": clean_text(jockey.get("FullName") if isinstance(jockey, dict) else runner.get("Jockey")),
        "trainer": clean_text(trainer.get("FullName") if isinstance(trainer, dict) else runner.get("Trainer")),
        "weight": parse_float(runner.get("Weight") or runner.get("WeightTotal") or runner.get("WeightAllocated")),
        "distance": None,
        "class_name": None,
        "track_condition": None,
        "finish_position": parse_finish_position(runner.get("Position")),
        "margin": parse_margin(runner.get("Margin")),
        "starting_price": parse_price(runner.get("PriceSP")),
        "prize_money": parse_money(runner.get("PrizeMoney")),
        "last_start_finish": int(forms_finishes[0]) if forms_finishes else None,
        "last_3_finishes": "|".join(str(int(value)) for value in forms_finishes[:3]) if forms_finishes else None,
        "last_3_margins": "|".join(f"{value:.2f}" for value in forms_margins[:3]) if forms_margins else None,
        "average_last_3_finish": average(forms_finishes[:3]),
        "average_last_3_margin": average(forms_margins[:3]),
        "source_file": str(source_file),
        "source_kind": "punting_form_monthly_export",
    }


def _monthly_export_stem_for_meeting(track_name: object, meeting_date: object) -> str | None:
    track = clean_text(track_name)
    race_date = parse_date(meeting_date)
    if track is None or race_date is None:
        return None
    date_prefix = pd.Timestamp(race_date).strftime("%y%m%d")
    slug = re.sub(r"[^A-Za-z0-9]+", "_", track).strip("_")
    return f"{date_prefix}_{slug}"


def _load_monthly_export_records(monthly_exports_dir: Path) -> tuple[list[dict[str, object]], int]:
    if not monthly_exports_dir.exists() and FALLBACK_MONTHLY_EXPORTS_DIR.exists():
        monthly_exports_dir = FALLBACK_MONTHLY_EXPORTS_DIR
    if not monthly_exports_dir.exists():
        return [], 0

    zip_candidates = sorted(monthly_exports_dir.glob("*.zip"))
    latest_by_month: dict[str, Path] = {}
    for zip_path in zip_candidates:
        month_key = zip_path.name.split("-", 1)[0].lower()
        latest_by_month[month_key] = zip_path
    zip_files = sorted(latest_by_month.values())
    records: list[dict[str, object]] = []
    files_loaded = 0

    for index, zip_path in enumerate(zip_files, start=1):
        print(f"Processing monthly export {index}/{len(zip_files)}: {zip_path.name}")
        try:
            archive = zipfile.ZipFile(zip_path)
        except (OSError, zipfile.BadZipFile):
            continue

        with archive:
            meeting_list_name = next((name for name in archive.namelist() if name.endswith("meetingList.json")), None)
            if meeting_list_name is None:
                continue

            try:
                meeting_rows = json.loads(archive.read(meeting_list_name))
            except json.JSONDecodeError:
                continue
            if not isinstance(meeting_rows, list):
                continue

            meeting_lookup: dict[str, dict] = {}
            for meeting in meeting_rows:
                if not isinstance(meeting, dict):
                    continue
                track = meeting.get("Track") or {}
                stem = _monthly_export_stem_for_meeting(
                    track.get("Name") if isinstance(track, dict) else track,
                    meeting.get("MeetingDate"),
                )
                if stem:
                    meeting_lookup[stem] = meeting

            form_names = sorted(
                name for name in archive.namelist() if "/Form/" in name and name.endswith(".json")
            )
            for form_name in form_names:
                files_loaded += 1
                try:
                    runner_rows = json.loads(archive.read(form_name))
                except json.JSONDecodeError:
                    continue
                if not isinstance(runner_rows, list) or not runner_rows:
                    continue

                stem = Path(form_name).stem
                meeting = meeting_lookup.get(stem)
                if meeting is None:
                    continue

                track = meeting.get("Track") or {}
                track_name = track.get("Name") if isinstance(track, dict) else track
                meeting_date = meeting.get("MeetingDate")
                ordered_race_ids = sorted(
                    {
                        parse_int(row.get("RaceId"))
                        for row in runner_rows
                        if isinstance(row, dict) and parse_int(row.get("RaceId")) is not None
                    }
                )
                race_number_lookup = {
                    race_id: index
                    for index, race_id in enumerate(ordered_race_ids, start=1)
                }

                for runner in runner_rows:
                    if not isinstance(runner, dict):
                        continue
                    race_id = parse_int(runner.get("RaceId"))
                    race_number = race_number_lookup.get(race_id)
                    if race_number is None:
                        continue
                    records.append(
                        _monthly_export_runner_to_record(
                            meeting_date=meeting_date,
                            track_name=track_name,
                            race_number=race_number,
                            runner=runner,
                            source_file=Path(f"{zip_path.name}:{form_name}"),
                        )
                    )
    return records, files_loaded


def _records_from_results_json(path: Path, payload: dict) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    races = payload.get("payLoad") or []
    for meeting in races:
        meeting_date = meeting.get("meetingDate")
        track_name = meeting.get("track")
        for race_result in meeting.get("raceResults") or []:
            race_number = race_result.get("raceNumber")
            distance = race_result.get("distance")
            track_condition = race_result.get("trackConditionLabel") or race_result.get("trackCondition")
            class_name = race_result.get("raceType") or race_result.get("className")
            for runner in race_result.get("runners") or []:
                records.append(
                    _json_record_from_runner(
                        meeting_date=meeting_date,
                        track_name=track_name,
                        race_number=race_number,
                        distance=distance,
                        class_name=class_name,
                        track_condition=track_condition,
                        runner=runner,
                        source_file=path,
                    )
                )
    return records


def _records_from_meeting_like_json(path: Path, payload: dict) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    meeting = payload.get("payLoad") or {}
    track = meeting.get("track") or {}
    track_name = track.get("name")
    meeting_date = meeting.get("meetingDate")
    for race in meeting.get("races") or []:
        race_number = race.get("raceNumber")
        distance = race.get("distance")
        class_name = race.get("raceType") or race.get("className") or race.get("name")
        track_condition = race.get("trackConditionLabel") or race.get("trackCondition")
        for runner in race.get("runners") or []:
            records.append(
                _json_record_from_runner(
                    meeting_date=meeting_date,
                    track_name=track_name,
                    race_number=race_number,
                    distance=distance,
                    class_name=class_name,
                    track_condition=track_condition,
                    runner=runner,
                    source_file=path,
                )
            )
    return records


def _records_from_form_json(path: Path, payload: dict) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    runners = payload.get("payLoad") or []
    for runner in runners:
        records.append(
            _json_record_from_runner(
                meeting_date=runner.get("meetingDate"),
                track_name=runner.get("trackName") or runner.get("track"),
                race_number=runner.get("raceNumber"),
                distance=runner.get("distance"),
                class_name=runner.get("raceType") or runner.get("className"),
                track_condition=runner.get("trackConditionLabel") or runner.get("trackCondition"),
                runner=runner,
                source_file=path,
            )
        )
    return records


def _load_raw_api_records(input_dir: Path) -> tuple[list[dict[str, object]], int]:
    files = sorted(
        path
        for path in input_dir.rglob("*.json")
        if path.is_file() and not path.name.endswith(".error.json")
    )
    records: list[dict[str, object]] = []
    for file_path in files:
        payload = _parse_json_payload(file_path)
        if not isinstance(payload, dict):
            continue
        if file_path.name == "results.json":
            records.extend(_records_from_results_json(file_path, payload))
        elif file_path.name in {"meeting.json", "fields.json"}:
            records.extend(_records_from_meeting_like_json(file_path, payload))
        elif file_path.name == "form.json":
            records.extend(_records_from_form_json(file_path, payload))
    return records, len(files)


def import_punting_form(
    input_dir: Path = PUNTING_FORM_INPUT_DIR,
    output_path: Path = OUTPUT_PATH,
    raw_api_input_dir: Path = RAW_PUNTING_FORM_INPUT_DIR,
    monthly_exports_dir: Path = DEFAULT_MONTHLY_EXPORTS_DIR,
) -> pd.DataFrame:
    files = sorted(input_dir.rglob("*.csv"))
    clean_rows: list[dict[str, object]] = []
    rows_loaded = 0

    for file_path in files:
        frame = read_csv_flex(file_path)
        rows_loaded += len(frame)
        frame.columns = [normalize_column_name(column) for column in frame.columns]
        for _, row in frame.iterrows():
            clean_rows.append(_row_to_clean_record(row, file_path))

    raw_api_rows, raw_api_file_count = _load_raw_api_records(raw_api_input_dir)
    rows_loaded += len(raw_api_rows)
    clean_rows.extend(raw_api_rows)

    monthly_export_rows, monthly_export_file_count = _load_monthly_export_records(monthly_exports_dir)
    rows_loaded += len(monthly_export_rows)
    clean_rows.extend(monthly_export_rows)

    clean_frame = pd.DataFrame(clean_rows)
    if clean_frame.empty:
        clean_frame = pd.DataFrame(columns=list(_row_to_clean_record(pd.Series(dtype=object), Path("")).keys()))
    else:
        dedupe_columns = ["race_date", "track", "race_number", "horse_name"]
        clean_frame["_source_priority"] = clean_frame.get("source_file", pd.Series(dtype=object)).map(
            lambda value: 0
            if str(value).endswith("results.json")
            else 1
            if str(value).endswith("meeting.json")
            else 2
            if str(value).endswith("fields.json")
            else 3
            if str(value).endswith("form.json")
            else 4
        )
        clean_frame = clean_frame.sort_values(["_source_priority", "source_file"])
        clean_frame = clean_frame.drop_duplicates(subset=dedupe_columns, keep="first")
        clean_frame = clean_frame.drop(columns=["_source_priority"])

    clean_frame = attach_common_labels(clean_frame)
    critical_missing = (
        clean_frame[["race_date", "track", "horse_name"]].isna().any(axis=1).sum()
        if not clean_frame.empty
        else 0
    )

    save_dataframe(clean_frame, output_path)

    print("Punting Form Import Summary")
    print(f"FILES LOADED: {len(files) + raw_api_file_count + monthly_export_file_count}")
    print(f"ROWS LOADED: {rows_loaded}")
    print(f"ROWS CLEANED: {len(clean_frame)}")
    print(f"MISSING CRITICAL FIELDS: {critical_missing}")
    return clean_frame


def main() -> None:
    import_punting_form()


if __name__ == "__main__":
    main()
