from __future__ import annotations

from app.research.labs.common import LabConfig, ReportSpec, build_parser, run_lab, summarize_groups


def _sectional_efficiency_report(frame, target_spec):
    frame["sectional_bucket"] = frame["sectional_efficiency_proxy"].round(1).astype(str)
    return summarize_groups(frame, ["sectional_bucket", "odds_bucket"], target_spec)


def _sectional_market_response(frame, target_spec):
    frame["efficiency_regime"] = frame["sectional_efficiency_proxy"].round(1).astype(str)
    return summarize_groups(frame, ["efficiency_regime", "steam_flag"], target_spec)


def _sectional_clv_analysis(frame, target_spec):
    frame["runner_efficiency_bucket"] = frame["runner_efficiency_proxy"].round(1).astype(str)
    return summarize_groups(frame, ["runner_efficiency_bucket", "market_rank_bucket"], target_spec)


CONFIG = LabConfig(
    slug="sectional_efficiency",
    title="Sectional Efficiency Lab",
    purpose="Investigate whether sectional-efficiency proxies and energy-distribution patterns are misread by the market.",
    questions=[
        "Are strong late sectionals underpriced?",
        "Are inefficient energy distributions overbet?",
        "Does the market misread sectional context?",
    ],
    reports=[
        ReportSpec("sectional_efficiency_report.csv", "Sectional Efficiency Report", _sectional_efficiency_report),
        ReportSpec("sectional_market_response.csv", "Sectional Market Response", _sectional_market_response),
        ReportSpec("sectional_clv_analysis.csv", "Sectional CLV Analysis", _sectional_clv_analysis),
    ],
    assumptions=[
        "No true sectional-time feed was present, so the lab uses margin and finishing-efficiency proxies.",
        "This is a feature-discovery lab rather than a validated timing signal.",
        "If useful, the next step is ingesting explicit sectional splits and pace-adjusted closing metrics.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

