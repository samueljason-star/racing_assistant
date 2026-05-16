from __future__ import annotations

from app.research.labs.common import LabConfig, ReportSpec, build_parser, run_lab, summarize_groups


def _field_shape_report(frame, target_spec):
    return summarize_groups(frame, ["field_size_bucket", "market_rank_bucket"], target_spec)


def _field_shape_clv_analysis(frame, target_spec):
    frame["compression_bucket"] = frame["field_compression_proxy"].round(1).astype(str)
    return summarize_groups(frame, ["field_size_bucket", "compression_bucket"], target_spec)


def _field_efficiency_analysis(frame, target_spec):
    return summarize_groups(frame, ["field_size_bucket", "odds_bucket", "pace_regime"], target_spec)


CONFIG = LabConfig(
    slug="field_shape",
    title="Field Shape Lab",
    purpose="Investigate field-size and race-shape dynamics for inefficiency and market behaviour.",
    questions=[
        "Are small fields more efficient?",
        "Do large chaotic fields create inefficiency?",
        "Does field compression matter?",
    ],
    reports=[
        ReportSpec("field_shape_report.csv", "Field Shape Report", _field_shape_report),
        ReportSpec("field_shape_clv_analysis.csv", "Field Shape CLV Analysis", _field_shape_clv_analysis),
        ReportSpec("field_efficiency_analysis.csv", "Field Efficiency Analysis", _field_efficiency_analysis),
    ],
    assumptions=[
        "Field shape uses field size and odds-compression proxies rather than a dedicated race-shape model.",
        "Any large-field effect must survive longshot-noise checks to matter.",
        "If useful, the next step is richer chaos/competitiveness features at race level.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

