import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import SessionLocal
from app.models import Feature, Result


def main():
    db = SessionLocal()

    try:
        joined_rows = db.query(Feature, Result).filter(
            Feature.race_id == Result.race_id,
            Feature.runner_id == Result.runner_id,
        ).count()

        winner_rows = db.query(Result).filter(Result.finish_position == 1).count()

        print(f"JOINED FEATURE/RESULT ROWS: {joined_rows}")
        print(f"WINNER ROWS: {winner_rows}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
