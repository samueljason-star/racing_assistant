from __future__ import annotations

from app.research.labs.common import LabConfig, ReportSpec, build_parser, run_lab, summarize_groups


def _track_bias_report(frame, target_spec):
    return summarize_groups(frame, ["track_norm", "barrier_bucket", "track_condition"], target_spec, min_sample=30)


def _lane_bias_analysis(frame, target_spec):
    frame["bias_bucket"] = frame["track_bias_proxy"].round(1).astype(str)
    return summarize_groups(frame, ["bias_bucket", "barrier_bucket"], target_spec)


def _track_bias_market_response(frame, target_spec):
    return summarize_groups(frame, ["track_condition", "barrier_bucket", "steam_flag"], target_spec)


CONFIG = LabConfig(
    slug="track_bias",
    title="Track Bias Lab",
    purpose="Investigate dynamic track bias proxies and whether the market adapts slowly.",
    questions=[
        "Does the market adapt slowly to track bias?",
        "Are rail/lane biases underpriced?",
        "Are certain run styles advantaged or disadvantaged?",
    ],
    reports=[
        ReportSpec("track_bias_report.csv", "Track Bias Report", _track_bias_report),
        ReportSpec("lane_bias_analysis.csv", "Lane Bias Analysis", _lane_bias_analysis),
        ReportSpec("track_bias_market_response.csv", "Track Bias Market Response", _track_bias_market_response),
    ],
    assumptions=[
        "No direct rail-position feed was available, so bias uses barrier and track-condition proxies.",
        "Track-level rows are exploratory and may be concentration-sensitive.",
        "If useful, the next step is adding meeting-level lane/rail metadata.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

