from __future__ import annotations

import pandas as pd

from app.research.labs.common import RESEARCH_REPORTS_DIR, build_parser, load_lab_frame, realistic_zone_mask, save_dataframe
from app.research.labs.execution_helpers import (
    add_execution_risk_features,
    executed_frame,
    fit_candidate_thresholds,
    frozen_candidate_masks,
    predicted_shortening_report,
    scenario_grid,
)

REPORT_PATH = RESEARCH_REPORTS_DIR / "forward_execution_fragility.csv"
THRESHOLD_PATH = RESEARCH_REPORTS_DIR / "forward_break_even_thresholds.csv"
LEAKAGE_PATH = RESEARCH_REPORTS_DIR / "forward_execution_fragility_leakage_audit.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "forward_execution_fragility_summary.md"


def main() -> None:
    args = build_parser("Forward execution fragility lab.").parse_args()
    frame, target_spec = load_lab_frame(args.matched_path, "sixty_to_close")
    frame = frame[realistic_zone_mask(frame)].copy()
    months = sorted(frame["race_month"].dropna().astype(str).unique().tolist())
    leakage = pd.DataFrame(
        [{"assumption": "forward_fragility", "status": "safe", "detail": "Fragility scenarios are applied only to frozen forward-month candidates."}]
    )
    if len(months) < 2:
        empty = pd.DataFrame([{"status": "no_unseen_month", "available_months": len(months)}])
        save_dataframe(empty, REPORT_PATH)
        save_dataframe(empty.copy(), THRESHOLD_PATH)
        save_dataframe(leakage, LEAKAGE_PATH)
        SUMMARY_PATH.write_text(
            "# Forward Execution Fragility Lab\n\n- No unseen forward month exists yet, so forward fragility cannot be measured honestly.\n",
            encoding="utf-8",
        )
        print(f"Wrote: {REPORT_PATH}")
        print(f"Wrote: {THRESHOLD_PATH}")
        print(f"Wrote: {LEAKAGE_PATH}")
        print(f"Wrote: {SUMMARY_PATH}")
        return

    rows = []
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
        masks = frozen_candidate_masks(test_predictions, thresholds)
        for scenario in scenario_grid():
            executed = executed_frame(test_predictions, target_spec, scenario)
            for label in ["shortening_plus_compression", "adverse_fill_filtered", "per_race_top_1_shortening", "compression_conditioned_value"]:
                subset = executed.loc[masks.get(label, pd.Series(False, index=executed.index))].copy()
                if subset.empty:
                    continue
                rows.append(
                    {
                        "forward_month": test_month,
                        "candidate_name": label,
                        "scenario": scenario.name,
                        "selections": int(len(subset)),
                        "roi": float(subset["executed_profit"].sum() / subset["executed_stake"].sum()) if subset["executed_stake"].sum() else 0.0,
                        "executed_clv": float(subset["executed_clv"].mean()),
                        "executable_edge": float(subset["executable_edge"].mean()),
                        "roi_decay": float(subset["executed_profit"].sum() / subset["executed_stake"].sum()) if subset["executed_stake"].sum() else 0.0,
                        "break_even_slippage": float(scenario.spread_multiplier),
                        "break_even_delay": float(scenario.move_capture),
                        "break_even_fill_quality": float(scenario.fill_fraction),
                    }
                )
    report = pd.DataFrame(rows)
    thresholds = (
        report.groupby(["forward_month", "candidate_name"], observed=False)
        .agg(
            surviving_scenarios=("roi", lambda s: int(pd.Series(s).gt(0).sum())),
            max_break_even_delay=("break_even_delay", "max"),
            max_break_even_slippage=("break_even_slippage", "max"),
            min_break_even_fill_quality=("break_even_fill_quality", "min"),
        )
        .reset_index()
        if not report.empty
        else pd.DataFrame()
    )
    summary = (
        "# Forward Execution Fragility Lab\n\n"
        f"- Forward months evaluated: `{report['forward_month'].nunique()}`\n"
        "- This lab measures how quickly frozen forward-month candidates decay under worse execution assumptions.\n"
        if not report.empty
        else "# Forward Execution Fragility Lab\n\n- No forward rows were produced.\n"
    )
    save_dataframe(report, REPORT_PATH)
    save_dataframe(thresholds, THRESHOLD_PATH)
    save_dataframe(leakage, LEAKAGE_PATH)
    SUMMARY_PATH.write_text(summary, encoding="utf-8")
    print(f"Wrote: {REPORT_PATH}")
    print(f"Wrote: {THRESHOLD_PATH}")
    print(f"Wrote: {LEAKAGE_PATH}")
    print(f"Wrote: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
