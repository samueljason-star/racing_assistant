from __future__ import annotations

import pandas as pd

from app.research.labs.common import LabConfig, ReportSpec, build_parser, run_lab, summarize_groups


def _favourite_longshot_report(frame, target_spec):
    frame["implied_prob_bucket"] = pd.cut(
        frame["anchor_market_prob_norm"],
        bins=[0, 0.03, 0.06, 0.10, 0.18, 1.0],
        labels=["0-3%", "3-6%", "6-10%", "10-18%", "18%+"],
        include_lowest=True,
    ).astype(str)
    return summarize_groups(frame, ["implied_prob_bucket", "odds_bucket"], target_spec)


def _odds_bucket_calibration(frame, target_spec):
    report = summarize_groups(frame, ["odds_bucket", "market_rank_bucket"], target_spec)
    if report.empty:
        return report
    report["actual_win_rate"] = report["strike_rate"]
    report["implied_probability_proxy"] = 1.0 / report["average_odds"].replace(0, pd.NA)
    report["favourite_longshot_gap"] = report["actual_win_rate"] - report["implied_probability_proxy"].fillna(0.0)
    return report


def _residual_distortion(frame, target_spec):
    frame["residual_bucket"] = pd.cut(
        frame["market_disagreement_proxy"],
        bins=[-float("inf"), 0.02, 0.05, 0.10, float("inf")],
        labels=["tiny", "small", "medium", "large"],
        include_lowest=True,
    ).astype(str)
    return summarize_groups(frame, ["residual_bucket", "odds_bucket"], target_spec)


CONFIG = LabConfig(
    slug="favourite_longshot",
    title="Favourite Longshot Lab",
    purpose="Investigate favourite-longshot distortion, calibration, and where market disagreements cluster.",
    questions=[
        "Are longshots systematically overbet?",
        "Are favourites underbet?",
        "Where do residual disagreements cluster?",
    ],
    reports=[
        ReportSpec("favourite_longshot_report.csv", "Favourite Longshot Report", _favourite_longshot_report),
        ReportSpec("odds_bucket_calibration.csv", "Odds Bucket Calibration", _odds_bucket_calibration),
        ReportSpec("residual_distortion_analysis.csv", "Residual Distortion Analysis", _residual_distortion),
    ],
    assumptions=[
        "Calibration is assessed relative to available market-implied probability proxies rather than exchange SP alone.",
        "Longshot rows are explicitly flagged for noise risk.",
        "If useful, the next step is targeted calibration and disagreement studies by odds regime and market rank.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

