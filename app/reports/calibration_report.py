from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import SessionLocal, init_db
from app.reports.calibration_utils import collect_calibration_rows, summarize_calibration


def main() -> None:
    init_db()
    db = SessionLocal()
    try:
        rows = collect_calibration_rows(db)
        summary = summarize_calibration(rows)

        print("CALIBRATION REPORT")
        print(f"Rows scored: {len(rows)}")
        if summary["brier_score"] is not None:
            print(f"Brier score: {summary['brier_score']:.4f}")
        else:
            print("Brier score: N/A")
        print("")
        print(
            "Bucket   Count  Avg Pred  Actual Win  Avg Market  Avg Edge"
        )
        print(
            "-------  -----  --------  ----------  ----------  --------"
        )
        for bucket in summary["bucket_summaries"]:
            avg_market = (
                f"{bucket['avg_market_probability']:.4f}"
                if bucket["avg_market_probability"] is not None
                else "N/A"
            )
            avg_edge = (
                f"{bucket['avg_edge']:.4f}"
                if bucket["avg_edge"] is not None
                else "N/A"
            )
            print(
                f"{bucket['bucket']:<7}  "
                f"{bucket['count']:>5}  "
                f"{bucket['avg_predicted_probability']:.4f}    "
                f"{bucket['actual_win_rate']:.4f}      "
                f"{avg_market:>10}  "
                f"{avg_edge:>8}"
            )
    finally:
        db.close()


if __name__ == "__main__":
    main()
