from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.betting.market_helpers import edge_bucket_label, odds_bucket_label
from app.research.form_score_optimizer import OUTPUT_PATH as FORM_CONFIG_PATH
from app.research.form_score_optimizer import apply_form_formula, prepare_form_features
from app.research.utils import (
    RESEARCH_DATA_DIR,
    RESEARCH_REPORTS_DIR,
    assign_market_rank,
    build_group_summary,
    compute_edge_and_clv_columns,
    estimate_runner_probabilities,
    save_dataframe,
)

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
MARKET_PATTERNS_PATH = RESEARCH_REPORTS_DIR / "market_patterns.csv"
ODDS_BUCKET_PATH = RESEARCH_REPORTS_DIR / "odds_bucket_report.csv"
MOVEMENT_REPORT_PATH = RESEARCH_REPORTS_DIR / "movement_report.csv"


def _load_best_form_config() -> dict[str, float]:
    if not FORM_CONFIG_PATH.exists():
        return {
            "finish_weight": 2.0,
            "margin_weight": 1.0,
            "distance_weight": 0.5,
            "class_weight": 0.25,
            "barrier_weight": 0.5,
            "trainer_weight": 0.0,
            "jockey_weight": 0.0,
        }
    import json

    return json.loads(FORM_CONFIG_PATH.read_text(encoding="utf-8"))


def build_analysis_frame(matched_path: Path = MATCHED_PATH) -> pd.DataFrame:
    frame = pd.read_csv(matched_path)
    config = _load_best_form_config()
    scored = apply_form_formula(prepare_form_features(frame), config)
    scored = estimate_runner_probabilities(scored, "form_score")
    scored = compute_edge_and_clv_columns(scored)
    scored = assign_market_rank(scored)
    scored["won_flag"] = pd.to_numeric(scored.get("won_flag"), errors="coerce").fillna(0)
    odds_used = pd.to_numeric(scored.get("price_10m"), errors="coerce").combine_first(
        pd.to_numeric(scored.get("closing_price"), errors="coerce")
    )
    scored["odds_used"] = odds_used
    scored["stake"] = 1.0
    scored["profit_loss"] = scored.apply(
        lambda row: ((row["odds_used"] - 1.0) * 0.92) if row["won_flag"] == 1 else -1.0,
        axis=1,
    )
    scored["odds_bucket"] = scored["odds_used"].map(odds_bucket_label)
    scored["edge_bucket"] = scored["edge"].map(edge_bucket_label)
    scored["movement_bucket"] = scored["open_to_close_change"].map(
        lambda value: "unknown"
        if pd.isna(value)
        else "strong_shorten"
        if value >= 1.0
        else "shorten"
        if value > 0
        else "strong_drift"
        if value <= -1.0
        else "drift"
        if value < 0
        else "flat"
    )
    scored["form_movement_bucket"] = scored.apply(
        lambda row: f"{'high' if row['form_score'] >= 0.65 else 'low'}_form__{row['movement_bucket']}",
        axis=1,
    )
    return scored


def analyze_market_patterns(matched_path: Path = MATCHED_PATH) -> dict[str, pd.DataFrame]:
    frame = build_analysis_frame(matched_path)

    odds_bucket_report = build_group_summary(frame, "odds_bucket")
    movement_report = build_group_summary(frame, "movement_bucket")
    market_rank_report = build_group_summary(frame.assign(market_rank=frame["market_rank"].fillna(0).astype(int)), "market_rank")
    form_movement_report = build_group_summary(frame, "form_movement_bucket")

    market_patterns = pd.concat(
        [
            odds_bucket_report.assign(report_type="odds_bucket"),
            movement_report.assign(report_type="movement"),
            market_rank_report.assign(report_type="market_rank"),
            form_movement_report.assign(report_type="form_movement"),
        ],
        ignore_index=True,
    )

    save_dataframe(odds_bucket_report, ODDS_BUCKET_PATH)
    save_dataframe(movement_report, MOVEMENT_REPORT_PATH)
    save_dataframe(market_patterns, MARKET_PATTERNS_PATH)

    print("Market Pattern Report")
    print("ROI by odds bucket")
    print(odds_bucket_report.to_string(index=False))
    print("ROI by movement")
    print(movement_report.to_string(index=False))
    print("ROI by market rank")
    print(market_rank_report.to_string(index=False))
    print("ROI by form score and movement")
    print(form_movement_report.head(20).to_string(index=False))

    return {
        "market_patterns": market_patterns,
        "odds_bucket_report": odds_bucket_report,
        "movement_report": movement_report,
    }


def main() -> None:
    analyze_market_patterns()


if __name__ == "__main__":
    main()
