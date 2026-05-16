from __future__ import annotations

from app.research.labs.common import LabConfig, ReportSpec, build_parser, run_lab, summarize_groups


def _stable_intent_report(frame, target_spec):
    return summarize_groups(frame, ["prep_stage", "race_class_group"], target_spec)


def _trainer_jockey_patterns(frame, target_spec):
    frame["trainer_jockey_combo"] = frame["trainer"].fillna("unknown").astype(str) + " | " + frame["jockey"].fillna("unknown").astype(str)
    report = summarize_groups(frame, ["trainer_jockey_combo"], target_spec, min_sample=20, sort_by=["average_clv", "actual_shorten_rate"])
    return report.head(100)


def _stable_market_agreement(frame, target_spec):
    frame["intent_bucket"] = frame["stable_intent_proxy"].round(1).astype(str)
    return summarize_groups(frame, ["intent_bucket", "prep_stage"], target_spec)


CONFIG = LabConfig(
    slug="stable_intent",
    title="Stable Intent Lab",
    purpose="Investigate trainer, jockey, and stable intent proxies for market agreement and mispricing.",
    questions=[
        "Are certain placement patterns underpriced?",
        "Are specific trainer-jockey combinations mispriced?",
        "Do stable-intent proxies predict market movement?",
    ],
    reports=[
        ReportSpec("stable_intent_report.csv", "Stable Intent Report", _stable_intent_report),
        ReportSpec("trainer_jockey_patterns.csv", "Trainer Jockey Patterns", _trainer_jockey_patterns),
        ReportSpec("stable_market_agreement.csv", "Stable Market Agreement", _stable_market_agreement),
    ],
    assumptions=[
        "Stable intent is proxied from trainer/jockey stats, class movement, and freshness because explicit stable intel is unavailable.",
        "Combo reports are trimmed to avoid turning one-off pairings into false signals.",
        "If useful, the next step is richer prep-cycle and jockey-switch history features.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

