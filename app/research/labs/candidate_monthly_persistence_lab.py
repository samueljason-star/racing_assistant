from __future__ import annotations

import pandas as pd

from app.research.labs.common import RESEARCH_REPORTS_DIR, build_parser, load_lab_frame, realistic_zone_mask, save_dataframe
from app.research.labs.execution_helpers import DEFAULT_SCENARIOS, add_execution_risk_features, executed_frame, fit_candidate_thresholds, frozen_candidate_masks, predicted_shortening_report

REPORT_PATH = RESEARCH_REPORTS_DIR / "candidate_monthly_persistence.csv"
ROLLING_PATH = RESEARCH_REPORTS_DIR / "rolling_candidate_stability.csv"
DECAY_PATH = RESEARCH_REPORTS_DIR / "monthly_edge_decay.csv"
LEAKAGE_PATH = RESEARCH_REPORTS_DIR / "candidate_monthly_persistence_leakage_audit.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "candidate_monthly_persistence_summary.md"


def _month_rows(label: str, subset: pd.DataFrame) -> list[dict[str, float | str | int]]:
    rows = []
    for month, month_subset in subset.groupby("race_month", observed=False):
        roi = float(month_subset["executed_profit"].sum() / month_subset["executed_stake"].sum()) if month_subset["executed_stake"].sum() else 0.0
        rows.append(
            {
                "candidate_name": label,
                "race_month": str(month),
                "selections": int(len(month_subset)),
                "monthly_roi": roi,
                "monthly_executed_clv": float(month_subset["executed_clv"].mean()),
                "monthly_executable_edge": float(month_subset["executable_edge"].mean()),
                "monthly_drawdown": float((month_subset["executed_profit"].cumsum().cummax() - month_subset["executed_profit"].cumsum()).max()),
                "monthly_hit_rate": float(month_subset["executed_clv"].gt(0).mean()),
                "monthly_value_persistence": float(month_subset["executable_edge"].gt(0).mean()),
            }
        )
    return rows


def main() -> None:
    args = build_parser("Candidate monthly persistence lab.").parse_args()
    frame, target_spec = load_lab_frame(args.matched_path, "sixty_to_close")
    frame = frame[realistic_zone_mask(frame)].copy()
    _, predictions, _ = predicted_shortening_report(frame, target_spec, model_names={"gradient_boosting"})
    predictions = add_execution_risk_features(predictions[predictions["model_name"].eq("gradient_boosting")].copy())
    executed = executed_frame(predictions, target_spec, DEFAULT_SCENARIOS[2])
    masks = frozen_candidate_masks(predictions, fit_candidate_thresholds(predictions))
    candidates = {
        "shortening_plus_compression": masks["shortening_plus_compression"],
        "adverse_fill_filtered": masks["adverse_fill_filtered"],
        "per_race_top_1_shortening": masks["per_race_top_1_shortening"],
        "compression_conditioned_value": masks["compression_conditioned_value"],
    }
    monthly_rows = []
    rolling_rows = []
    decay_rows = []
    for label, mask in candidates.items():
        subset = executed.loc[mask].copy()
        if subset.empty:
            continue
        month_df = pd.DataFrame(_month_rows(label, subset))
        monthly_rows.append(month_df)
        if not month_df.empty:
            decay_rows.append(
                {
                    "candidate_name": label,
                    "month_concentration": float(subset["race_month"].value_counts(normalize=True, dropna=False).max()),
                    "rolling_stability": float(month_df["monthly_roi"].mean()),
                    "rolling_sharpe_like": float(month_df["monthly_roi"].mean() / month_df["monthly_roi"].std(ddof=0)) if len(month_df) > 1 and month_df["monthly_roi"].std(ddof=0) else 0.0,
                    "volatility": float(month_df["monthly_roi"].std(ddof=0)) if len(month_df) > 1 else 0.0,
                    "edge_persistence": float(month_df["monthly_executable_edge"].gt(0).mean()),
                }
            )
            temp = month_df.copy()
            temp["rolling_roi_mean"] = temp["monthly_roi"].expanding().mean()
            temp["rolling_edge_mean"] = temp["monthly_executable_edge"].expanding().mean()
            rolling_rows.append(temp)
    report = pd.concat(monthly_rows, ignore_index=True) if monthly_rows else pd.DataFrame([{"status": "no_monthly_history"}])
    rolling = pd.concat(rolling_rows, ignore_index=True) if rolling_rows else pd.DataFrame([{"status": "no_monthly_history"}])
    decay = pd.DataFrame(decay_rows) if decay_rows else pd.DataFrame([{"status": "no_monthly_history"}])
    leakage = pd.DataFrame(
        [{"assumption": "monthly_persistence", "status": "safe", "detail": "Monthly persistence is descriptive over fixed ex-ante candidates and does not feed future months back into selection."}]
    )
    summary = (
        "# Candidate Monthly Persistence Lab\n\n"
        f"- Candidates with monthly history: `{len(decay_rows)}`\n"
        "- This lab measures whether candidate quality is distributed through months or concentrated in one period.\n"
    )
    save_dataframe(report, REPORT_PATH)
    save_dataframe(rolling, ROLLING_PATH)
    save_dataframe(decay, DECAY_PATH)
    save_dataframe(leakage, LEAKAGE_PATH)
    SUMMARY_PATH.write_text(summary, encoding="utf-8")
    print(f"Wrote: {REPORT_PATH}")
    print(f"Wrote: {ROLLING_PATH}")
    print(f"Wrote: {DECAY_PATH}")
    print(f"Wrote: {LEAKAGE_PATH}")
    print(f"Wrote: {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
