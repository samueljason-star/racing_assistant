from __future__ import annotations

from app.research.labs.common import LabConfig, ReportSpec, build_parser, run_lab, summarize_groups


def _pace_shape_report(frame, target_spec):
    return summarize_groups(frame, ["pace_regime", "market_rank_bucket"], target_spec)


def _pace_shape_market_response(frame, target_spec):
    return summarize_groups(frame, ["pace_regime", "steam_flag", "barrier_bucket"], target_spec)


def _pace_shape_clv_analysis(frame, target_spec):
    return summarize_groups(frame, ["pace_regime", "field_size_bucket", "odds_bucket"], target_spec)


CONFIG = LabConfig(
    slug="pace_shape",
    title="Pace Shape Lab",
    purpose="Investigate whether race pace shape proxies create systematic market mispricing.",
    questions=[
        "Do high-pressure races create late-closing underpricing?",
        "Are leaders overbet in certain pace profiles?",
        "Are backmarkers underbet in pace-collapse races?",
        "Does the market misprice pace pressure proxies?",
    ],
    reports=[
        ReportSpec("pace_shape_report.csv", "Pace Shape Report", _pace_shape_report),
        ReportSpec("pace_shape_market_response.csv", "Pace Shape Market Response", _pace_shape_market_response),
        ReportSpec("pace_shape_clv_analysis.csv", "Pace Shape CLV Analysis", _pace_shape_clv_analysis),
    ],
    assumptions=[
        "No explicit speed-map dataset was available, so pace uses a proxy from barrier, field size, and favourite density.",
        "Signals are treated as structural diagnostics, not betting rules.",
        "If this lab is useful, the next step is collecting true run-style / pace-map features.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

