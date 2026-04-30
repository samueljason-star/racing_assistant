from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from app.punting_form.client import (
    DATE_PARAM_CANDIDATES,
    VISIBLE_FORM_ENDPOINTS,
    build_meeting_params,
    build_meetings_list_params,
    request_endpoint,
    require_api_key,
)

DEFAULT_OUTPUT_ROOT = Path("data/raw/punting_form")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download the visible Punting Form Forms endpoints for the last N days."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of calendar days to fetch, including today. Default: 30.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Directory where raw API responses are saved.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=10,
        help="Runs parameter for the form endpoints. Default: 10.",
    )
    return parser.parse_args()


def date_range(days: int) -> list[date]:
    today = datetime.now().date()
    return [today - timedelta(days=offset) for offset in range(days)]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_response_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")


def save_response_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def try_parse_json(text: str) -> Any | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def extract_meeting_ids(payload: Any) -> set[int]:
    meeting_ids: set[int] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if key.lower() == "meetingid" and isinstance(child, (int, float, str)):
                    try:
                        meeting_ids.add(int(child))
                    except (TypeError, ValueError):
                        pass
                walk(child)
            return
        if isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return meeting_ids


def fetch_meetings_list_for_date(api_key: str, target_date: date, output_root: Path) -> set[int]:
    date_label = target_date.isoformat()
    day_dir = output_root / date_label
    ensure_dir(day_dir)

    json_payload: Any | None = None
    last_error: Exception | None = None

    for endpoint_name in ("meetings_list", "meetings_list_csv"):
        endpoint = next(spec for spec in VISIBLE_FORM_ENDPOINTS if spec.name == endpoint_name)
        success = False
        for param_name in DATE_PARAM_CANDIDATES:
            try:
                params = build_meetings_list_params(api_key, date_label, param_name)
                response = request_endpoint(endpoint, params)
                extension = "csv" if endpoint.format == "csv" else "json"
                save_response_text(
                    day_dir / f"{endpoint.name}.{extension}",
                    response.text,
                )
                if endpoint.name == "meetings_list":
                    json_payload = try_parse_json(response.text)
                    metadata = {
                        "endpoint": endpoint.name,
                        "date": date_label,
                        "param_name": param_name,
                        "meeting_ids": sorted(extract_meeting_ids(json_payload)),
                    }
                    save_response_json(day_dir / "meetings_list.meta.json", metadata)
                success = True
                break
            except Exception as exc:  # pragma: no cover - network/runtime path
                last_error = exc
        if not success:
            raise RuntimeError(
                f"Failed to fetch {endpoint.name} for {date_label}: {last_error}"
            ) from last_error

    return extract_meeting_ids(json_payload)


def fetch_meeting_endpoints(
    api_key: str,
    target_date: date,
    meeting_id: int,
    output_root: Path,
    *,
    runs: int,
) -> None:
    date_label = target_date.isoformat()
    meeting_dir = output_root / date_label / f"meeting_{meeting_id}"
    ensure_dir(meeting_dir)

    for endpoint in VISIBLE_FORM_ENDPOINTS:
        if endpoint.name.startswith("meetings_list"):
            continue
        params = build_meeting_params(
            api_key,
            meeting_id,
            runs=runs,
            entity_type=endpoint.entity_type,
        )
        try:
            response = request_endpoint(endpoint, params)
            extension = "csv" if endpoint.format == "csv" else "json"
            save_response_text(meeting_dir / f"{endpoint.name}.{extension}", response.text)
        except Exception as exc:  # pragma: no cover - network/runtime path
            save_response_json(
                meeting_dir / f"{endpoint.name}.error.json",
                {
                    "endpoint": endpoint.name,
                    "path": endpoint.path,
                    "meeting_id": meeting_id,
                    "entity_type": endpoint.entity_type,
                    "error": str(exc),
                },
            )
            print(f"    warning: {endpoint.name} failed for meeting {meeting_id}: {exc}")


def main() -> None:
    args = parse_args()
    api_key = require_api_key()
    output_root = Path(args.output_root)
    ensure_dir(output_root)

    total_meetings = 0
    for target_date in date_range(args.days):
        print(f"Fetching Punting Form data for {target_date.isoformat()}...")
        meeting_ids = fetch_meetings_list_for_date(api_key, target_date, output_root)
        total_meetings += len(meeting_ids)
        print(f"  meetings found: {len(meeting_ids)}")
        for meeting_id in sorted(meeting_ids):
            print(f"  fetching meeting {meeting_id}")
            fetch_meeting_endpoints(
                api_key,
                target_date,
                meeting_id,
                output_root,
                runs=args.runs,
            )

    print(
        "Finished Punting Form fetch | "
        f"days={args.days} | meetings={total_meetings} | output={output_root}"
    )


if __name__ == "__main__":
    main()
