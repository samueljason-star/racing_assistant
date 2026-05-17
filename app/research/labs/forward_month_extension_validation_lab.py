from __future__ import annotations

import pandas as pd

from app.research.labs.common import RESEARCH_REPORTS_DIR, build_parser, load_lab_frame, realistic_zone_mask, save_dataframe
from app.research.labs.execution_helpers import (
    DEFAULT_SCENARIOS,
    add_execution_risk_features,
    executed_frame,
    fit_candidate_thresholds,
    frozen_candidate_masks,
    predicted_shortening_report,
)

REPORT_PATH = RESEARCH_REPORTS_DIR / "forward_month_extension_validation.csv"
COMPARE_PATH = RESEARCH_REPORTS_DIR / "forward_month_candidate_comparison.csv"
DECAY_PATH = RESEARCH_REPORTS_DIR / "forward_month_decay_analysis.csv"
LEAKAGE_PATH = RESEARCH_REPORTS_DIR / "forward_month_extension_leakage_audit.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "forward_month_extension_summary.md"


def _metrics(label: str, subset: pd.DataFrame, period: str) -> dict[str, float | int | str]:
    return {
        "candidate_name": label,
        "period": period,
        "selections": int(len(subset)),
        "executed_clv": float(subset["executed_clv"].mean()) if not subset.empty else 0.0,
        "executable_edge": float(subset["executable_edge"].mean()) if not subset.empty else 0.0,
        "roi": float(subset["executed_profit"].sum() / subset["executed_stake"].sum()) if not subset.empty and subset["executed_stake"].sum() else 0.0,
        "drawdown": float((subset["executed_profit"].cumsum().cummax() - subset["executed_profit"].cumsum()).max()) if not subset.empty else 0.0,
        "post_shortening_edge": float(subset["post_shortening_edge"].mean()) if not subset.empty else 0.0,
        "value_persistence": float(subset["executable_edge"].gt(0).mean()) if not subset.empty else 0.0,
        "slippage_sensitivity": float(subset["execution_fragility"].mean()) if not subset.empty else 0.0,
        "positive_day_share": float(subset.groupby("race_date", observed=False)["executed_profit"].sum().gt(0).mean()) if not subset.empty else 0.0,
        "positive_week_share": float(subset.groupby("race_week", observed=False)["executed_profit"].sum().gt(0).mean()) if not subset.empty and "race_week" in subset.columns else 0.0,
        "positive_month_share": float(subset.groupby("race_month", observed=False)["executed_profit"].sum().gt(0).mean()) if not subset.empty else 0.0,
        "average_odds": float(subset["anchor_price"].mean()) if not subset.empty else 0.0,
        "average_market_rank": float(subset["market_rank_current"].mean()) if not subset.empty else 0.0,
    }


def main() -> None:
    args = build_parser("Forward month extension validation lab.").parse_args()
    frame, target_spec = load_lab_frame(args.matched_path, "sixty_to_close")
    frame = frame[realistic_zone_mask(frame)].copy()
    months = sorted(frame["race_month"].dropna().astype(str).unique().tolist())
    leakage = pd.DataFrame(
        [{"assumption": "forward_freeze", "status": "safe", "detail": "Candidate thresholds are fitted on past months only and never tuned on the forward month."}]
    )
    if len(months) < 2:
        empty = pd.DataFrame(
            [{"status": "no_unseen_month", "available_months": len(months), "latest_month": months[-1] if months else "none"}]
        )
        save_dataframe(empty, REPORT_PATH)
        save_dataframe(empty.copy(), COMPARE_PATH)
        save_dataframe(empty.copy(), DECAY_PATH)
        save_dataframe(leakage, LEAKAGE_PATH)
        SUMMARY_PATH.write_text(
            "# Forward Month Extension Validation Lab\n\n- No unseen forward month exists yet. Current data only covers one month, so strict forward validation cannot be performed honestly.\n",
            encoding="utf-8",
        )
        print(f"Wrote: {REPORT_PATH}")
        print(f"Wrote: {COMPARE_PATH}")
        print(f"Wrote: {DECAY_PATH}")
        print(f"Wrote: {LEAKAGE_PATH}")
        print(f"Wrote: {SUMMARY_PATH}")
        return

    rows = []
    decay_rows = []
    for idx in range(1, len(months)):
        train_months = months[:idx]
        test_month = months[idx]
        train_frame = frame[frame["race_month"].astype(str).isin(train_months)].copy()
        test_frame = frame[frame["race_month"].astype(str).eq(test_month)].copy()
        _, train_predictions, _ = predicted_shortening_report(train_frame, target_spec, model_names={"gradient_boosting"})
        _, test_predictions, _ = predicted_shortening_report(test_frame, target_spec, model_names={"gradient_boosting"})
        train_predictions = add_execution_risk_features(train_predictions[train_predictions["model_name"].eq("gradient_boosting")].copy())
        test_predictions = add_execution_risk_features(test_predictions[test_predictions["model_name"].eq("gradient_boosting")].copy())
        thresholds = fit_candidate_thresholds(train_predictions)
        masks_train = frozen_candidate_masks(train_predictions, thresholds)
        masks_test = frozen_candidate_masks(test_predictions, thresholds)
        executed_train = executed_frame(train_predictions, target_spec, DEFAULT_SCENARIOS[2])
        executed_test = executed_frame(test_predictions, target_spec, DEFAULT_SCENARIOS[2])
        for label in ["shortening_plus_compression", "adverse_fill_filtered", "per_race_top_1_shortening", "compression_conditioned_value"]:
            hist_subset = executed_train.loc[masks_train.get(label, pd.Series(False, index=executed_train.index))].copy()
            fwd_subset = executed_test.loc[masks_test.get(label, pd.Series(False, index=executed_test.index))].copy()
            rows.append({**_metrics(label, hist_subset, "historical"), "forward_month": test_month})
            rows.append({**_metrics(label, fwd_subset, "forward_month"), "forward_month": test_month})
            decay_rows.append(
                {
                    "candidate_name": label,
                    "forward_month": test_month,
                    "historical_roi": float(hist_subset["executed_profit"].sum() / hist_subset["executed_stake"].sum()) if not hist_subset.empty and hist_subset["executed_stake"].sum() else 0.0,
                    "forward_roi": float(fwd_subset["executed_profit"].sum() / fwd_subset["executed_stake"].sum()) if not fwd_subset.empty and fwd_subset["executed_stake"].sum() else 0.0,
                    "roi_decay": (
                        float(fwd_subset["executed_profit"].sum() / fwd_subset["executed_stake"].sum()) if not fwd_subset.empty and fwd_subset["executed_stake"].sum() else 0.0
                    ) - (
                        float(hist_subset["executed_profit"].sum() / hist_subset["executed_stake"].sum()) if not hist_subset.empty and hist_subset["executed_stake"].sum() else 0.0
                    ),
                    "clv_decay": (float(fwd_subset["executed_clv"].mean()) if not fwd_subset.empty else 0.0) - (float(hist_subset["executed_clv"].mean()) if not hist_subset.empty else 0.0),
                    "edge_decay": (float(fwd_subset["executable_edge"].mean()) if not fwd_subset.empty else 0.0) - (float(hist_subset["executable_edge"].mean()) if not hist_subset.empty else 0.0),
                }
            )
    report = pd.DataFrame(rows)
    comparison = report.copy()
    decay = pd.DataFrame(decay_rows)
    summary = (
        "# Forward Month Extension Validation Lab\n\n"
        f"- Forward months evaluated: `{report['forward_month'].nunique()}`\n"
        "- This lab freezes candidate thresholds on historical months and evaluates the next unseen month only.\n"
    )
    save_dataframe(report, REPORT_PATH)
    save_dataframe(comparison, COMPARE_PATH)
    save_dataframe(decay, DECAY_PATH)
    save_dataframe(leakage, LEAKAGE_PATH)
    SUMMARY_PATH.write_text(summary, encoding="utf-8")
    print(f"Wrote: {REPORT_PATH}")
    print(f"Wrote: {COMPARE_PATH}")
    print(f"Wrote: {DECAY_PATH}")
    print(f"Wrote: {LEAKAGE_PATH}")
    print(f"Wrote: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
