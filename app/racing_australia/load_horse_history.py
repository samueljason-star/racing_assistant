from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from app.db import SessionLocal, init_db
from app.models import HorseHistory
from app.racing_australia.client import build_url, get_html
from app.racing_australia.load_today_races import extract_today_race_links

DISTANCE_RE = re.compile(r"\b(\d{3,4})m\b", re.IGNORECASE)
DATE_RE = re.compile(r"\b(\d{2}[A-Za-z]{3}\d{2})\b")
TRACK_RE = re.compile(r"^([A-Z]{3,5})\s+\d{2}[A-Za-z]{3}\d{2}\b")
TRACK_CONDITION_RE = re.compile(r"\b(Firm|Good|Soft|Heavy|Synthetic)\d+\b", re.IGNORECASE)
CLASS_RE = re.compile(
    r"^[A-Z]{3,5}\s+\d{2}[A-Za-z]{3}\d{2}\s+\d{3,4}m\s+\S+\s+(.*?)\s+\$\d",
    re.IGNORECASE,
)
WEIGHT_BARRIER_RE = re.compile(
    r"\$\d[\d,]*(?:\s+\(\$\d[\d,]*\))?\s+(.+?)\s+(\d+(?:\.\d+)?)kg\s+Barrier\s+(\d+)",
    re.IGNORECASE,
)
FINISH_RE = re.compile(r"(\d+)(?:st|nd|rd|th)")
MARGIN_RE = re.compile(r",\s*([0-9.]+)L\b")
STARTING_PRICE_RE = re.compile(r",\s*\$(\d+(?:\.\d+)?)(?:/\$\d+(?:\.\d+)?)*\s*$")


def parse_distance(text: str):
    if not text:
        return None
    match = DISTANCE_RE.search(text)
    return int(match.group(1)) if match else None


def find_runner_profile_links(race_html: str):
    soup = BeautifulSoup(race_html, "lxml")
    links_by_horsecode = {}
    ignored_text = {"", "View Pedigree Report", "Next Horse", "Previous Horse"}

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(" ", strip=True)

        if "HorseFullForm.aspx" not in href or text in ignored_text:
            continue

        url = build_url(href.replace("../", "/"))
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        horsecode = (params.get("horsecode") or [None])[0]
        stage = (params.get("stage") or [None])[0]
        raceentry = (params.get("raceentry") or params.get("raceEntry") or [None])[0]

        if not horsecode or stage != "FinalFields" or not raceentry:
            continue

        existing_name = links_by_horsecode.get(horsecode, {}).get("name")
        if existing_name and len(existing_name) >= len(text):
            continue

        links_by_horsecode[horsecode] = {"name": text, "url": url}

    return [
        (item["name"], item["url"])
        for item in links_by_horsecode.values()
    ]


def _parse_history_row(horse_name: str, text: str, finish_text: str):
    track_match = TRACK_RE.search(text)
    date_match = DATE_RE.search(text)
    class_match = CLASS_RE.search(text)
    track_condition_match = TRACK_CONDITION_RE.search(text)
    weight_barrier_match = WEIGHT_BARRIER_RE.search(text)
    finish_match = FINISH_RE.search(finish_text)
    margin_match = MARGIN_RE.search(text)
    sp_match = STARTING_PRICE_RE.search(text)

    jockey = None
    weight = None
    barrier = None
    if weight_barrier_match:
        jockey = weight_barrier_match.group(1).strip()
        weight = float(weight_barrier_match.group(2))
        barrier = int(weight_barrier_match.group(3))

    finish_position = None
    if finish_match:
        finish_position = int(finish_match.group(1))

    return {
        "horse_name": horse_name,
        "run_date": date_match.group(1) if date_match else None,
        "track": track_match.group(1) if track_match else None,
        "distance": parse_distance(text),
        "class_name": class_match.group(1).strip() if class_match else None,
        "barrier": barrier,
        "weight": weight,
        "jockey": jockey,
        "trainer": None,
        "track_condition": track_condition_match.group(0) if track_condition_match else None,
        "finish_position": finish_position,
        "margin": float(margin_match.group(1)) if margin_match else None,
        "starting_price": float(sp_match.group(1)) if sp_match else None,
        "source": "racing_australia",
    }


def parse_recent_runs(horse_name: str, profile_html: str, limit: int = 10):
    soup = BeautifulSoup(profile_html, "lxml")
    history = []

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) != 2:
                continue
            if not DATE_RE.search(cells[1]):
                continue

            history.append(_parse_history_row(horse_name, cells[1], cells[0]))
            if len(history) >= limit:
                return history

    return history


def save_history_rows(db, rows):
    created = 0

    for row in rows:
        exists = db.query(HorseHistory).filter(
            HorseHistory.horse_name == row["horse_name"],
            HorseHistory.run_date == row["run_date"],
            HorseHistory.track == row["track"],
            HorseHistory.distance == row["distance"],
        ).first()

        if exists:
            continue

        db.add(HorseHistory(**row))
        created += 1

    return created


def load_today_horse_history():
    init_db()
    db = SessionLocal()

    try:
        race_links = extract_today_race_links()
        total_profiles = 0
        total_rows = 0

        print(f"RACE LINKS FOUND: {len(race_links)}")

        for race_url in race_links:
            race_html = get_html(race_url)
            profile_links = find_runner_profile_links(race_html)

            print(f"RACE PAGE: {race_url} | HORSE LINKS: {len(profile_links)}")

            for horse_name, profile_url in profile_links:
                try:
                    profile_html = get_html(profile_url)
                    rows = parse_recent_runs(horse_name, profile_html)
                    total_rows += save_history_rows(db, rows)
                    total_profiles += 1
                except Exception as exc:
                    print(f"FAILED HORSE PROFILE: {horse_name} | {profile_url} | {exc}")

            db.commit()

        print(f"HORSE PROFILES CHECKED: {total_profiles}")
        print(f"HISTORY ROWS ADDED: {total_rows}")

    finally:
        db.close()


if __name__ == "__main__":
    load_today_horse_history()
