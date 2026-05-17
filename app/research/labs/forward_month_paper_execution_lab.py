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

REPORT_PATH = RESEARCH_REPORTS_DIR / "forward_month_paper_execution.csv"
PATHS_PATH = RESEARCH_REPORTS_DIR / "forward_execution_paths.csv"
LEAKAGE_PATH = RESEARCH_REPORTS_DIR / "forward_month_paper_execution_leakage_audit.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "forward_execution_summary.md"


def main() -> None:
    args = build_parser("Forward month paper execution lab.").parse_args()
    frame, target_spec = load_lab_frame(args.matched_path, "sixty_to_close")
    frame = frame[realistic_zone_mask(frame)].copy()
    months = sorted(frame["race_month"].dropna().astype(str).unique().tolist())
    leakage = pd.DataFrame(
        [{"assumption": "forward_paper", "status": "safe", "detail": "Forward paper execution freezes candidate thresholds on historical months before scoring the next month."}]
    )
    if len(months) < 2:
        empty = pd.DataFrame([{"status": "no_unseen_month", "available_months": len(months)}])
        save_dataframe(empty, REPORT_PATH)
        save_dataframe(empty.copy(), PATHS_PATH)
        save_dataframe(leakage, LEAKAGE_PATH)
        SUMMARY_PATH.write_text(
            "# Forward Month Paper Execution Lab\n\n- No unseen month exists yet, so forward paper execution cannot be run honestly.\n",
            encoding="utf-8",
        )
        print(f"Wrote: {REPORT_PATH}")
        print(f"Wrote: {PATHS_PATH}")
        print(f"Wrote: {LEAKAGE_PATH}")
        print(f"Wrote: {SUMMARY_PATH}")
        return

    report_rows = []
    path_rows = []
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
        for scenario in [s for s in scenario_grid() if s.name in {"ideal_fill", "mild_delay", "moderate_delay", "heavy_delay", "adverse_fill"}]:
            executed = executed_frame(test_predictions, target_spec, scenario)
            for label in ["shortening_plus_compression", "adverse_fill_filtered", "per_race_top_1_shortening", "compression_conditioned_value"]:
                subset = executed.loc[masks.get(label, pd.Series(False, index=executed.index))].copy()
                if subset.empty:
                    continue
                report_rows.append(
                    {
                        "forward_month": test_month,
                        "candidate_name": label,
                        "scenario": scenario.name,
                        "selections": int(len(subset)),
                        "executed_clv": float(subset["executed_clv"].mean()),
                        "executable_edge": float(subset["executable_edge"].mean()),
                        "roi": float(subset["executed_profit"].sum() / subset["executed_stake"].sum()) if subset["executed_stake"].sum() else 0.0,
                    }
                )
                path_rows.append(
                    subset[
                        [
                            "race_date",
                            "track_norm",
                            "race_number",
                            "horse_name",
                            "expected_fill_price",
                            "close_price",
                            "executed_clv",
                            "executable_edge",
                            "executed_profit",
                        ]
                    ]
                    .assign(forward_month=test_month, candidate_name=label, scenario=scenario.name)
                )
    report = pd.DataFrame(report_rows)
    paths = pd.concat(path_rows, ignore_index=True) if path_rows else pd.DataFrame()
    summary = (
        "# Forward Month Paper Execution Lab\n\n"
        f"- Forward months evaluated: `{report['forward_month'].nunique()}`\n"
        "- This lab simulates pseudo-live execution on unseen month data only.\n"
        if not report.empty
        else "# Forward Month Paper Execution Lab\n\n- No forward rows were produced.\n"
    )
    save_dataframe(report, REPORT_PATH)
    save_dataframe(paths, PATHS_PATH)
    save_dataframe(leakage, LEAKAGE_PATH)
    SUMMARY_PATH.write_text(summary, encoding="utf-8")
    print(f"Wrote: {REPORT_PATH}")
    print(f"Wrote: {PATHS_PATH}")
    print(f"Wrote: {LEAKAGE_PATH}")
    print(f"Wrote: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
