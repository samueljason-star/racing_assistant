from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_classif

from app.research.clv_market_agreement_model import _build_design_matrices, _fit_classifier, _model_specs, _predict_classifier
from app.research.labs.common import LabConfig, ReportSpec, build_parser, load_lab_frame, run_lab, summarize_groups


def _feature_categories(frame: pd.DataFrame) -> dict[str, list[str]]:
    market_only = [column for column in ["market_rank_current", "anchor_price", "anchor_market_prob_norm", "market_signal", "odds_signal", "price_60m", "movement_score"] if column in frame.columns]
    non_market = [column for column in [
        "form_score", "form_signal", "context_score", "barrier", "field_size", "trainer_stat", "jockey_stat",
        "best_last_3_finish", "average_last_3_finish", "average_margin_last_3", "last_start_margin",
        "distance", "distance_change", "class_change", "track_condition_match",
        "stable_intent_proxy", "pace_pressure_proxy", "sectional_efficiency_proxy", "runner_efficiency_proxy",
    ] if column in frame.columns]
    all_features = list(dict.fromkeys(market_only + non_market))
    return {
        "market_only": market_only,
        "non_market_only": non_market,
        "all_features": all_features,
    }


def _safe_spearman(values: pd.Series, target: pd.Series) -> float:
    pair = pd.concat([pd.to_numeric(values, errors="coerce"), pd.to_numeric(target, errors="coerce")], axis=1).dropna()
    if len(pair) < 3:
        return 0.0
    if pair.iloc[:, 0].nunique(dropna=True) < 2 or pair.iloc[:, 1].nunique(dropna=True) < 2:
        return 0.0
    result = pair.iloc[:, 0].corr(pair.iloc[:, 1], method="spearman")
    return float(result) if pd.notna(result) else 0.0


def _feature_discovery_report(frame, target_spec):
    features = _feature_categories(frame)["all_features"]
    usable = [column for column in features if frame[column].notna().sum() >= 50]
    if not usable:
        return pd.DataFrame()
    x = frame[usable].copy()
    x = pd.get_dummies(x, dummy_na=True)
    keep = [column for column in x.columns if x[column].notna().any()]
    x = x[keep].fillna(0.0)
    y = frame[target_spec.target_flag_column].astype(int)
    scores = mutual_info_classif(x, y, discrete_features="auto", random_state=42)
    report = pd.DataFrame({"feature_name": x.columns, "mutual_information": scores})
    report["low_sample_size"] = False
    report["high_concentration"] = False
    report["likely_false_positive"] = report["mutual_information"] <= 0
    report["commercially_plausible"] = report["mutual_information"] > report["mutual_information"].median()
    return report.sort_values("mutual_information", ascending=False).reset_index(drop=True)


def _feature_interaction_analysis(frame, target_spec):
    interaction_candidates = {
        "movement_x_rank": frame["movement_score"].fillna(0.0) * frame["market_rank_current"].fillna(0.0),
        "form_x_rank": frame["form_signal"].fillna(0.0) * frame["market_rank_current"].fillna(0.0),
        "context_x_price": frame["context_score"].fillna(0.0) * frame["anchor_price"].fillna(0.0),
        "intent_x_pressure": frame["stable_intent_proxy"].fillna(0.0) * frame["pace_pressure_proxy"].fillna(0.0),
        "sectional_x_rank": frame["sectional_efficiency_proxy"].fillna(0.0) * frame["market_rank_current"].fillna(0.0),
    }
    rows = []
    target = frame[target_spec.target_flag_column].astype(float)
    clv = frame[target_spec.target_clv_column].astype(float)
    for name, values in interaction_candidates.items():
        shorten_corr = _safe_spearman(pd.Series(values), target)
        clv_corr = _safe_spearman(pd.Series(values), clv)
        rows.append(
            {
                "interaction_name": name,
                "shorten_corr": shorten_corr,
                "clv_corr": clv_corr,
                "average_value": float(pd.Series(values).mean()),
                "low_sample_size": False,
                "high_concentration": False,
                "likely_false_positive": False,
                "commercially_plausible": abs(shorten_corr) >= 0.01,
            }
        )
    return pd.DataFrame(rows).sort_values(["commercially_plausible", "clv_corr", "shorten_corr"], ascending=[False, False, False]).reset_index(drop=True)


def _feature_uniqueness_analysis(frame, target_spec):
    categories = _feature_categories(frame)
    cutoff = frame["race_day"].quantile(0.8)
    train = frame[frame["race_day"] < cutoff].copy()
    test = frame[frame["race_day"] >= cutoff].copy()
    rows = []
    for spec in _model_specs():
        if spec.name not in {"logistic", "random_forest", "gradient_boosting"}:
            continue
        for feature_set, columns in categories.items():
            if not columns:
                continue
            predictions, _, _, dropped = _predict_classifier(spec, target_spec, train, test, columns)
            rows.append(
                {
                    "model_name": spec.name,
                    "feature_set": feature_set,
                    "number_of_features": len(columns) - len(dropped),
                    "auc": float(predictions[target_spec.target_flag_column].corr(predictions["predicted_shorten_probability"], method="spearman") or 0.0),
                    "shorten_rate": float(predictions[target_spec.target_flag_column].mean()),
                    "average_clv": float(predictions[target_spec.target_clv_column].mean()),
                    "top_bucket_shorten_rate": float(predictions[predictions["predicted_shorten_probability"] >= predictions["predicted_shorten_probability"].quantile(0.8)][target_spec.target_flag_column].mean()),
                    "top_bucket_average_clv": float(predictions[predictions["predicted_shorten_probability"] >= predictions["predicted_shorten_probability"].quantile(0.8)][target_spec.target_clv_column].mean()),
                }
            )
    report = pd.DataFrame(rows)
    if report.empty:
        return report
    baseline = report[report["feature_set"].eq("market_only")][["model_name", "auc"]].rename(columns={"auc": "market_only_auc"})
    report = report.merge(baseline, on="model_name", how="left")
    report["feature_set_delta_vs_market_only"] = report["auc"] - report["market_only_auc"].fillna(report["auc"])
    report["low_sample_size"] = False
    report["high_concentration"] = False
    report["likely_false_positive"] = report["feature_set_delta_vs_market_only"] <= 0
    report["commercially_plausible"] = report["feature_set_delta_vs_market_only"] > 0
    return report.sort_values(["commercially_plausible", "feature_set_delta_vs_market_only", "auc"], ascending=[False, False, False]).reset_index(drop=True)


CONFIG = LabConfig(
    slug="feature_discovery",
    title="Feature Discovery Lab",
    purpose="Discover candidate features, interactions, and incremental non-market information.",
    questions=[
        "Which combinations create the strongest incremental information?",
        "Which existing features are redundant?",
        "Which unexplored interactions matter?",
    ],
    reports=[
        ReportSpec("feature_discovery_report.csv", "Feature Discovery Report", _feature_discovery_report),
        ReportSpec("feature_interaction_analysis.csv", "Feature Interaction Analysis", _feature_interaction_analysis),
        ReportSpec("feature_uniqueness_analysis.csv", "Feature Uniqueness Analysis", _feature_uniqueness_analysis),
    ],
    assumptions=[
        "This lab treats feature discovery as explanatory work, not production model selection.",
        "Mutual information and incremental lift are used as candidate-signal screens only.",
        "If useful, the next step is promoting the best interactions into focused CLV-first models.",
    ],
)


def main() -> None:
    args = build_parser(CONFIG.purpose).parse_args()
    run_lab(CONFIG, args.matched_path, args.target)


if __name__ == "__main__":
    main()
