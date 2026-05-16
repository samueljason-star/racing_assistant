from __future__ import annotations

import pandas as pd

from app.research.labs.common import LabConfig, ReportSpec, build_parser, run_lab, summarize_groups


def _market_agreement_report(frame, target_spec):
    return summarize_groups(frame, ["market_rank_bucket", "steam_flag", "odds_bucket"], target_spec)


def _shortening_monotonicity(frame, target_spec):
    frame["disagreement_bucket"] = pd.cut(
        frame["market_disagreement_proxy"],
        bins=[-float("inf"), 0.01, 0.03, 0.06, float("inf")],
        labels=["tiny", "small", "medium", "large"],
        include_lowest=True,
    ).astype(str)
    return summarize_groups(frame, ["disagreement_bucket", "market_rank_bucket"], target_spec)


def _disagreement_clusters(frame, target_spec):
    frame["signal_cluster"] = (
        frame["market_rank_bucket"].astype(str)
        + " | "
        + frame["odds_bucket"].astype(str)
        + " | "
        + pd.cut(frame["movement_score"], bins=[-float("inf"), -0.5, 0.5, float("inf")], labels=["negative", "flat", "positive"], include_lowest=True).astype(str)
    )
    report = summarize_groups(frame, ["signal_cluster"], target_spec, min_sample=40)
    return report.head(120)


CONFIG = LabConfig(
    slug="market_agreement",
    title="Market Agreement Lab",
    purpose="Investigate where models and markets agree, disagree, and later converge.",
    questions=[
        "When does the market later agree with the model?",
        "Which signals produce shortening?",
        "Which disagreements are just noise?",
    ],
    reports=[
        ReportSpec("market_agreement_report.csv", "Market Agreement Report", _market_agreement_report),
        ReportSpec("shortening_monotonicity.csv", "Shortening Monotonicity", _shortening_monotonicity),
        ReportSpec("disagreement_clusters.csv", "Disagreement Clusters", _disagreement_clusters),
    ],
    assumptions=[
        "Agreement is measured using shortening and CLV rather than raw PnL.",
        "Disagreement clusters are diagnostic groupings, not strategies.",
        "If useful, the next step is plugging in focused CLV model scores directly into these clusters.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()

