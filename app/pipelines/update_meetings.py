import requests
from bs4 import BeautifulSoup
from app.db import SessionLocal, init_db
from app.models import Meeting

URL = "https://www.racingaustralia.horse/FreeFields/Calendar.aspx"

def update_meetings():
    init_db()
    db = SessionLocal()

    response = requests.get(URL)
    soup = BeautifulSoup(response.text, "html.parser")

    rows = soup.select("table tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        date = cols[0].text.strip()
        track = cols[1].text.strip()

        if not track:
            continue

        exists = db.query(Meeting).filter(
            Meeting.date == date,
            Meeting.track == track
        ).first()

        if not exists:
            db.add(Meeting(
                date=date,
                track=track,
                state="UNKNOWN",
                meeting_type="UNKNOWN"
            ))

    db.commit()
    db.close()
    print("Meetings updated from real source.")