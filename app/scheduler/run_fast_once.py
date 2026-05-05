import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.scheduler.run_once import run_fast_pipeline_once


def main() -> None:
    run_fast_pipeline_once()


if __name__ == "__main__":
    main()
