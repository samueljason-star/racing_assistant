from __future__ import annotations

from datetime import datetime, timezone
import re

from bs4 import BeautifulSoup

from app.db import SessionLocal
from app.models import HorseHistory, Race, Result, Runner
from app.racing_australia.client import get_html
from app.racing_australia.load_today_races import STATE_CODES
from app.utils.name_matching import horse_names_match, normalize_horse_name

CALENDAR_RESULTS_PATH = "/FreeFields/Calendar.aspx?State={state}"
RACE_HEADING_RE = re.compile(r"^Race\s+(\d+)\b", re.IGNORECASE)


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


def _parse_meeting_date(value: str | None):
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%a %d-%b", "%d%b%y"):
        try:
            parsed = datetime.strptime(value.strip(), fmt)
            return parsed.date()
        except ValueError:
            continue

    return None


def _parse_calendar_row_date(text: str):
    for fmt in ("%a %d-%b", "%A, %d %B %Y", "%d/%m/%Y"):
        try:
            parsed = datetime.strptime(text.strip(), fmt)
            today = datetime.now(timezone.utc)
            if fmt == "%a %d-%b":
                parsed = parsed.replace(year=today.year)
            return parsed.date()
        except ValueError:
            continue
    return None


def _normalize_track(value: str | None) -> str:
    normalized = normalize_horse_name(value or "")
    normalized = normalized.replace(" racecourse", "")
    normalized = normalized.replace(" racetrack", "")
    normalized = normalized.replace(" poly track", "")
    normalized = normalized.replace(" synthetic", "")
    normalized = normalized.replace(" turf", "")
    return normalized.strip()


def _tracks_match(a: str | None, b: str | None) -> bool:
    normalized_a = _normalize_track(a)
    normalized_b = _normalize_track(b)
    if not normalized_a or not normalized_b:
        return False
    return (
        normalized_a == normalized_b
        or normalized_a in normalized_b
        or normalized_b in normalized_a
    )


def _parse_finish_position(text: str | None):
    if not text:
        return None
    cleaned = text.strip()
    return int(cleaned) if cleaned.isdigit() else None


def _parse_margin(text: str | None):
    if not text:
        return None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    return float(match.group(1)) if match else None


def _parse_starting_price(text: str | None):
    if not text:
        return None
    match = re.search(r"\$([0-9]+(?:\.[0-9]+)?)", text)
    return float(match.group(1)) if match else None


def _parse_result_table(table):
    rows = table.find_all("tr")
    if not rows:
        return None

    header_cells = rows[0].find_all(["th", "td"])
    headers = [cell.get_text(" ", strip=True) for cell in header_cells]
    if "Finish" not in headers or "Horse" not in headers:
        return None

    header_index = {header: idx for idx, header in enumerate(headers)}
    parsed_rows = []
    for row in rows[1:]:
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
        if len(cells) < len(headers):
            continue

        horse_name = cells[header_index["Horse"]]
        if not horse_name:
            continue
        finish_position = _parse_finish_position(cells[header_index["Finish"]])
        if finish_position is None:
            continue

        parsed_rows.append(
            {
                "horse_name": horse_name,
                "finish_position": finish_position,
                "margin": _parse_margin(cells[header_index["Margin"]]) if "Margin" in header_index else None,
                "starting_price": _parse_starting_price(cells[header_index["Starting Price"]]) if "Starting Price" in header_index else None,
            }
        )

    return parsed_rows or None


def _parse_results_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(" ", strip=True)
    if "Results for this meeting are not currently available." in page_text:
        return {}

    results_by_race_number = {}
    current_race_number = None

    for tag in soup.find_all(True):
        if tag.name == "table":
            table_results = _parse_result_table(tag)
            if current_race_number and table_results:
                results_by_race_number[current_race_number] = table_results
            continue

        text = tag.get_text(" ", strip=True)
        match = RACE_HEADING_RE.search(text)
        if match:
            current_race_number = int(match.group(1))

    return results_by_race_number


def _results_links_for_date(meeting_date, calendar_cache):
    if meeting_date in calendar_cache:
        return calendar_cache[meeting_date]

    links = []
    for state in STATE_CODES:
        try:
            html = get_html(CALENDAR_RESULTS_PATH.format(state=state))
        except Exception:
            continue

        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue

            row_date = _parse_calendar_row_date(cells[0].get_text(" ", strip=True))
            if row_date != meeting_date:
                continue

            venue_text = cells[1].get_text(" ", strip=True)
            results_link = None
            for a in tr.find_all("a", href=True):
                href = a["href"]
                if "/FreeFields/Results.aspx?Key=" in href:
                    results_link = a["href"]
                    break

            if not results_link:
                continue

            links.append(
                {
                    "track": venue_text,
                    "url": results_link,
                    "state": state,
                }
            )

    calendar_cache[meeting_date] = links
    return links


def _results_url_for_race(race, calendar_cache):
    meeting = getattr(race, "meeting", None)
    if not meeting:
        return None

    meeting_date = _parse_meeting_date(meeting.date)
    if not meeting_date:
        jump_time = _parse_jump_time(race.jump_time)
        if jump_time:
            meeting_date = jump_time.date()
    if not meeting_date:
        return None

    candidates = _results_links_for_date(meeting_date, calendar_cache)
    for candidate in candidates:
        if _tracks_match(meeting.track, candidate["track"]):
            return candidate["url"]
    return None


def fetch_result_data_for_race(race, calendar_cache, page_cache):
    """Return real structured result data for a race when available."""
    results_url = _results_url_for_race(race, calendar_cache)
    if not results_url:
        return None

    if results_url not in page_cache:
        try:
            page_cache[results_url] = _parse_results_page(get_html(results_url))
        except Exception:
            page_cache[results_url] = {}

    return page_cache[results_url].get(race.race_number)


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
        races_skipped_no_real_result = 0
        results_upserted = 0
        history_rows_added = 0
        runners_unmatched = 0
        races_with_real_results = 0
        unmatched_samples = []
        calendar_cache = {}
        page_cache = {}

        for race in completed_races:
            existing_real_result = db.query(Result).filter(
                Result.race_id == race.id,
                Result.finish_position.isnot(None),
            ).first()
            if existing_real_result:
                continue

            result_rows = fetch_result_data_for_race(race, calendar_cache, page_cache)
            if not result_rows:
                races_skipped_no_real_result += 1
                continue
            races_with_real_results += 1

            for result_row in result_rows:
                if result_row.get("finish_position") is None:
                    continue
                runner = find_matching_runner(db, race.id, result_row["horse_name"])
                if not runner:
                    runners_unmatched += 1
                    if len(unmatched_samples) < 10:
                        meeting = getattr(race, "meeting", None)
                        unmatched_samples.append(
                            {
                                "race_id": race.id,
                                "track": meeting.track if meeting else None,
                                "race_number": race.race_number,
                                "horse_name": result_row["horse_name"],
                            }
                        )
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
        print(f"RACES WITH REAL RESULT PAGE: {races_with_real_results}")
        print(f"RACES SKIPPED DUE TO NO REAL RESULT: {races_skipped_no_real_result}")
        print(f"RESULTS UPSERTED: {results_upserted}")
        print(f"HORSE HISTORY ROWS ADDED: {history_rows_added}")
        print(f"RUNNERS UNMATCHED: {runners_unmatched}")
        if unmatched_samples:
            print("UNMATCHED RESULT ROW SAMPLES:")
            for sample in unmatched_samples:
                print(
                    f"race_id={sample['race_id']} | "
                    f"track={sample['track'] or 'Unknown'} | "
                    f"race_number={sample['race_number']} | "
                    f"horse={sample['horse_name']}"
                )
    finally:
        db.close()


if __name__ == "__main__":
    update_results()
