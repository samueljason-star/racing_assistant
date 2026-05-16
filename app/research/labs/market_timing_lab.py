from __future__ import annotations

import pandas as pd

from app.research.labs.common import LabConfig, ReportSpec, build_parser, rolling_month_report, run_lab, summarize_groups


def _market_timing_report(frame, target_spec):
    return summarize_groups(frame, ["steam_flag", "drift_flag", "odds_bucket"], target_spec)


def _movement_clusters(frame, target_spec):
    frame["timing_window"] = pd.cut(
        frame["late_price_volatility"],
        bins=[-float("inf"), 0.25, 0.75, float("inf")],
        labels=["quiet", "moderate", "volatile"],
        include_lowest=True,
    ).astype(str)
    return summarize_groups(frame, ["timing_window", "fake_steam_flag", "fake_drift_flag"], target_spec)


def _timing_clv_analysis(frame, target_spec):
    monthly = rolling_month_report(frame, target_spec)
    if monthly.empty:
        return monthly
    monthly["low_sample_size"] = monthly["selections"] < 80
    monthly["high_concentration"] = False
    monthly["likely_false_positive"] = monthly["low_sample_size"]
    return monthly


CONFIG = LabConfig(
    slug="market_timing",
    title="Market Timing Lab",
    purpose="Investigate when the market is most wrong and whether movement timing contains exploitable structure.",
    questions=[
        "Are early markets weaker?",
        "Are late drifts meaningful?",
        "Are steamers overbet?",
        "Are there volatility windows where inefficiency increases?",
    ],
    reports=[
        ReportSpec("market_timing_report.csv", "Market Timing Report", _market_timing_report),
        ReportSpec("market_movement_clusters.csv", "Market Movement Clusters", _movement_clusters),
        ReportSpec("timing_clv_analysis.csv", "Timing CLV Analysis", _timing_clv_analysis),
    ],
    assumptions=[
        "Timing analysis uses open, 60-minute, and close proxies already available in the matched research frame.",
        "Fake steam/drift is treated as movement without confirmation in result quality.",
        "If useful, the next step is finer-grained intraday liquidity and matched-volume features.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

