import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.pipelines.update_results import update_results


def main():
    print("Starting history backfill...")
    update_results()
    print("History backfill done.")


if __name__ == "__main__":
    main()
