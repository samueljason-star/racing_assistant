from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from app.racing_australia.client import build_url, get_html

STATE_CODES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]
CALENDAR_FORM_PATH = "/FreeFields/Calendar_Form.aspx?State={state}"


def _parse_row_date(text: str):
    try:
        parsed = datetime.strptime(text.strip(), "%a %d-%b")
    except ValueError:
        return None
    today = datetime.now(ZoneInfo("Australia/Brisbane"))
    return parsed.replace(year=today.year).date()


def extract_today_race_links(states=None):
    states = states or STATE_CODES
    today = datetime.now(ZoneInfo("Australia/Brisbane")).date()
    links = []
    seen = set()

    for state in states:
        html = get_html(CALENDAR_FORM_PATH.format(state=state))
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            continue

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) < 3:
                continue

            row_date = _parse_row_date(cells[0].get_text(" ", strip=True))
            if row_date != today:
                continue

            form_link = None
            for a in tr.find_all("a", href=True):
                href = a["href"]
                if "/FreeFields/Form.aspx?Key=" in href:
                    form_link = build_url(href)
                    break

            if not form_link or form_link in seen:
                continue

            seen.add(form_link)
            links.append(form_link)

    return links


if __name__ == "__main__":
    race_links = extract_today_race_links()
    print(f"RACE LINKS FOUND: {len(race_links)}")
    for link in race_links[:30]:
        print(link)
