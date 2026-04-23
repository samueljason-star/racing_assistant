import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.racing_australia.load_today_races import extract_today_race_links


if __name__ == "__main__":
    race_links = extract_today_race_links()
    print(f"RACE LINKS FOUND: {len(race_links)}")
    for link in race_links[:30]:
        print(link)
