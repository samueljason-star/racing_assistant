import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.racing_australia.load_horse_history import load_today_horse_history


if __name__ == "__main__":
    load_today_horse_history()
