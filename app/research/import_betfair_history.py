from __future__ import annotations

import argparse
import bz2
import json
import re
from pathlib import Path

import pandas as pd

from app.research.utils import (
    BETFAIR_HISTORY_INPUT_DIR,
    RESEARCH_DATA_DIR,
    attach_common_labels,
    clean_horse_name,
    clean_text,
    first_present,
    flatten_payload_to_records,
    normalize_column_name,
    normalize_track_name,
    parse_date,
    parse_datetime,
    parse_float,
    parse_int,
    parse_price,
    read_csv_flex,
    read_json_records,
    save_dataframe,
)

OUTPUT_PATH = RESEARCH_DATA_DIR / "betfair_odds_clean.csv"
OUTPUT_COLUMNS = list(_row_to_clean_record(pd.Series(dtype=object), Path("")).keys()) if False else [
    "race_date",
    "track",
    "race_number",
    "market_id",
    "selection_id",
    "horse_name",
    "timestamp",
    "market_start_time",
    "traded_price",
    "best_back_price",
    "best_lay_price",
    "total_matched",
    "minutes_to_jump",
    "source_file",
    "odds_for_matching",
]

ALIASES = {
    "race_date": ["race_date", "date", "event_date", "market_date", "meeting_date"],
    "track": ["track", "venue", "event_name", "track_name"],
    "race_number": ["race_number", "race", "race_no", "market_number"],
    "market_id": ["market_id", "marketid"],
    "selection_id": ["selection_id", "selectionid"],
    "horse_name": ["horse_name", "runner_name", "runner", "selection_name", "name"],
    "timestamp": ["timestamp", "publish_time", "time", "update_time"],
    "market_start_time": ["market_start_time", "start_time", "market_time", "scheduled_off"],
    "traded_price": ["traded_price", "last_price_traded", "ltp", "price"],
    "best_back_price": ["best_back_price", "back_price", "bestbackprice"],
    "best_lay_price": ["best_lay_price", "lay_price", "bestlayprice"],
    "total_matched": ["total_matched", "matched", "volume", "tv"],
}


def _records_from_csv(path: Path) -> list[dict[str, object]]:
    frame = read_csv_flex(path)
    frame.columns = [normalize_column_name(column) for column in frame.columns]
    return frame.to_dict(orient="records")


def _records_from_json(path: Path) -> list[dict[str, object]]:
    records = read_json_records(path)
    normalized = []
    for record in records:
        normalized.append({normalize_column_name(str(key)): value for key, value in record.items()})
    return normalized


def _extract_race_number_from_name(value: object) -> int | None:
    text = clean_text(value)
    if text is None:
        return None
    match = re.search(r"\bR(\d+)\b", text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _records_from_bz2_market_stream(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    market_metadata: dict[str, object] = {}
    runner_names: dict[str, str] = {}
    should_include_market = False

    with bz2.open(path, "rt", encoding="utf-8", errors="replace") as stream:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue

            publish_time = payload.get("pt")
            publish_time_iso = (
                pd.to_datetime(publish_time, unit="ms", utc=True).isoformat()
                if publish_time is not None
                else None
            )

            for market_change in payload.get("mc", []) or []:
                market_definition = market_change.get("marketDefinition")
                if market_definition:
                    should_include_market = market_definition.get("marketType") == "WIN"
                    market_metadata = {
                        "market_id": market_change.get("id"),
                        "market_start_time": market_definition.get("marketTime"),
                        "event_name": market_definition.get("eventName"),
                        "market_name": market_definition.get("name"),
                    }
                    runner_names = {
                        str(runner.get("id")): clean_horse_name(runner.get("name"))
                        for runner in market_definition.get("runners", []) or []
                        if runner.get("id") is not None
                    }

                if not should_include_market:
                    continue

                for runner_change in market_change.get("rc", []) or []:
                    selection_id = runner_change.get("id")
                    if selection_id is None:
                        continue

                    traded_price = parse_price(runner_change.get("ltp"))
                    best_back_price = None
                    best_lay_price = None
                    total_matched = None

                    if "batb" in runner_change and runner_change["batb"]:
                        first_back = runner_change["batb"][0]
                        if isinstance(first_back, (list, tuple)) and len(first_back) >= 2:
                            best_back_price = parse_price(first_back[1])
                    if "batl" in runner_change and runner_change["batl"]:
                        first_lay = runner_change["batl"][0]
                        if isinstance(first_lay, (list, tuple)) and len(first_lay) >= 2:
                            best_lay_price = parse_price(first_lay[1])
                    if "tv" in runner_change and runner_change["tv"]:
                        first_tv = runner_change["tv"][0]
                        if isinstance(first_tv, (list, tuple)) and len(first_tv) >= 2:
                            total_matched = parse_float(first_tv[1])

                    records.append(
                        {
                            "race_date": parse_date(market_metadata.get("market_start_time")),
                            "track": _infer_track_from_text(market_metadata.get("event_name")),
                            "race_number": _extract_race_number_from_name(market_metadata.get("market_name")),
                            "market_id": clean_text(market_change.get("id") or market_metadata.get("market_id")),
                            "selection_id": clean_text(selection_id),
                            "horse_name": clean_horse_name(runner_names.get(str(selection_id))),
                            "timestamp": publish_time_iso,
                            "market_start_time": clean_text(market_metadata.get("market_start_time")),
                            "traded_price": traded_price,
                            "best_back_price": best_back_price,
                            "best_lay_price": best_lay_price,
                            "total_matched": total_matched,
                            "source_file": str(path),
                            "odds_for_matching": traded_price or best_back_price,
                        }
                    )

    return records


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Betfair historical market data into a clean research CSV."
    )
    parser.add_argument(
        "--input-dir",
        default=str(BETFAIR_HISTORY_INPUT_DIR),
        help="Folder containing Betfair historical files.",
    )
    parser.add_argument(
        "--output-path",
        default=str(OUTPUT_PATH),
        help="Destination CSV for cleaned Betfair odds history.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional cap on number of source files to process.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Print progress every N files. Default: 25.",
    )
    return parser.parse_args()


def _infer_track_from_text(value: object) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    if "(" in text:
        text = text.split("(", 1)[0]
    text = re.sub(r"\s+\d{1,2}(st|nd|rd|th)\s+[A-Za-z]{3,9}\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+\d{4}-\d{2}-\d{2}\s*$", "", text)
    return clean_text(text)


def _row_to_clean_record(row: pd.Series, source_file: Path) -> dict[str, object]:
    timestamp = parse_datetime(first_present(row, ALIASES["timestamp"]))
    market_start_time = parse_datetime(first_present(row, ALIASES["market_start_time"]))
    minutes_to_jump = None
    if timestamp and market_start_time:
        minutes_to_jump = round((market_start_time - timestamp).total_seconds() / 60.0, 2)

    best_back_price = parse_price(first_present(row, ALIASES["best_back_price"]))
    traded_price = parse_price(first_present(row, ALIASES["traded_price"]))
    odds_used = traded_price or best_back_price

    track_value = first_present(row, ALIASES["track"])
    track = _infer_track_from_text(track_value)

    return {
        "race_date": parse_date(first_present(row, ALIASES["race_date"]) or market_start_time),
        "track": track,
        "race_number": parse_int(first_present(row, ALIASES["race_number"])),
        "market_id": clean_text(first_present(row, ALIASES["market_id"])),
        "selection_id": clean_text(first_present(row, ALIASES["selection_id"])),
        "horse_name": clean_horse_name(first_present(row, ALIASES["horse_name"])),
        "timestamp": timestamp.isoformat() if timestamp else None,
        "market_start_time": market_start_time.isoformat() if market_start_time else None,
        "traded_price": traded_price,
        "best_back_price": best_back_price,
        "best_lay_price": parse_price(first_present(row, ALIASES["best_lay_price"])),
        "total_matched": parse_float(first_present(row, ALIASES["total_matched"])),
        "minutes_to_jump": minutes_to_jump,
        "source_file": str(source_file),
        "odds_for_matching": odds_used,
    }


def import_betfair_history(
    input_dir: Path = BETFAIR_HISTORY_INPUT_DIR,
    output_path: Path = OUTPUT_PATH,
    *,
    max_files: int | None = None,
    progress_every: int = 25,
) -> pd.DataFrame:
    files = sorted(
        path
        for path in input_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".csv", ".json", ".jsonl", ".bz2"}
    )
    if max_files is not None:
        files = files[:max_files]
    clean_rows: list[dict[str, object]] = []
    markets_loaded: set[str] = set()
    rows_loaded = 0

    total_files = len(files)
    for index, file_path in enumerate(files, start=1):
        if file_path.suffix.lower() == ".csv":
            records = _records_from_csv(file_path)
        elif file_path.suffix.lower() == ".bz2":
            if not file_path.name.startswith("1."):
                continue
            records = _records_from_bz2_market_stream(file_path)
        else:
            records = _records_from_json(file_path)

        rows_loaded += len(records)
        for record in records:
            row = pd.Series(record)
            clean_record = _row_to_clean_record(row, file_path)
            clean_rows.append(clean_record)
            if clean_record.get("market_id"):
                markets_loaded.add(str(clean_record["market_id"]))

        if progress_every and index % progress_every == 0:
            print(
                f"PROGRESS: files={index}/{total_files} | "
                f"rows_loaded={rows_loaded} | rows_cleaned={len(clean_rows)} | "
                f"markets_loaded={len(markets_loaded)}"
            )

    clean_frame = pd.DataFrame(clean_rows)
    if clean_frame.empty:
        clean_frame = pd.DataFrame(columns=OUTPUT_COLUMNS)

    clean_frame = attach_common_labels(clean_frame)
    critical_missing = (
        clean_frame[["race_date", "track", "horse_name", "timestamp"]].isna().any(axis=1).sum()
        if not clean_frame.empty
        else 0
    )

    save_dataframe(clean_frame, output_path)

    print("Betfair History Import Summary")
    print(f"FILES LOADED: {len(files)}")
    print(f"MARKETS LOADED: {len(markets_loaded)}")
    print(f"ROWS CLEANED: {len(clean_frame)}")
    print(f"MISSING CRITICAL FIELDS: {critical_missing}")
    if len(files) == 0:
        print(f"WARNING: No Betfair history files found under {input_dir}")
    return clean_frame


def main() -> None:
    args = parse_args()
    import_betfair_history(
        input_dir=Path(args.input_dir),
        output_path=Path(args.output_path),
        max_files=args.max_files,
        progress_every=args.progress_every,
    )


if __name__ == "__main__":
    main()
