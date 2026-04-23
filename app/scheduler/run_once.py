import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.run_strategy import main as run_strategy_main


def run_pipeline_once():
    """Run the core racing pipeline once."""
    run_strategy_main()


if __name__ == "__main__":
    run_pipeline_once()
