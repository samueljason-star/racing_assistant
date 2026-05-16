from __future__ import annotations

from app.research.labs.common import LabConfig, ReportSpec, build_parser, rolling_month_report, run_lab, summarize_groups


def _regime_detection_report(frame, target_spec):
    monthly = rolling_month_report(frame, target_spec)
    if monthly.empty:
        return monthly
    monthly["low_sample_size"] = monthly["selections"] < 100
    monthly["high_concentration"] = False
    monthly["likely_false_positive"] = monthly["low_sample_size"]
    monthly["commercially_plausible"] = (monthly["average_clv"] > 0) & (~monthly["low_sample_size"])
    return monthly.sort_values(["average_clv", "actual_shorten_rate"], ascending=[False, False]).reset_index(drop=True)


def _regime_clusters(frame, target_spec):
    return summarize_groups(frame, ["race_month", "track_norm", "race_class_group"], target_spec, min_sample=30)


def _regime_clv_analysis(frame, target_spec):
    return summarize_groups(frame, ["race_month", "odds_bucket"], target_spec, min_sample=40)


CONFIG = LabConfig(
    slug="regime_detection",
    title="Regime Detection Lab",
    purpose="Detect changing market regimes over time, by track, and by race type.",
    questions=[
        "Does market efficiency vary over time?",
        "Are some months, tracks, or race types structurally weaker?",
        "Are there volatility regimes?",
    ],
    reports=[
        ReportSpec("regime_detection_report.csv", "Regime Detection Report", _regime_detection_report),
        ReportSpec("regime_clusters.csv", "Regime Clusters", _regime_clusters),
        ReportSpec("regime_clv_analysis.csv", "Regime CLV Analysis", _regime_clv_analysis),
    ],
    assumptions=[
        "Regime work is descriptive first and should not be confused with forward-tested timing.",
        "Monthly and track rows are flagged when concentration or sample issues dominate.",
        "If useful, the next step is rolling model-score overlays rather than static monthly grouping.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

