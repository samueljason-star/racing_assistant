from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from app.research.utils import RESEARCH_DATA_DIR
from app.research.validation import run_validation_suite

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"


def optimize_strategy(matched_path: Path = MATCHED_PATH) -> pd.DataFrame:
    outputs = run_validation_suite(matched_path)
    return outputs["recommendations"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the validation-led strategy optimizer.")
    parser.add_argument(
        "--matched-path",
        type=Path,
        default=MATCHED_PATH,
        help="Path to matched_runner_data.csv for validation reruns.",
    )
    args = parser.parse_args()
    optimize_strategy(args.matched_path)


if __name__ == "__main__":
    main()
