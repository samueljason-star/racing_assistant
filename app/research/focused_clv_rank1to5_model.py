from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.pipeline import Pipeline

from app.research.clv_market_agreement_model import (
    MATCHED_PATH,
    BEST_MODEL_PATH as _UNUSED_BEST,
    _build_design_matrices,
    _fit_classifier,
    _model_specs,
    _monotonicity_report,
    _prepare_bets,
    _predict_classifier,
    _safe_auc,
    _safe_brier,
    _safe_log_loss,
    _track_concentration,
    _month_concentration,
    _weekly_positive_rate,
    _monthly_positive_rate,
    _remove_top_winner_roi,
    _drawdown_from_profit,
    _market_rank_bucket,
    _odds_bucket,
)
from app.research.clv_prediction_model import ClvTargetSpec, _prepare_clv_frame
from app.research.market_residual_model import _build_leave_one_month_out_folds, _build_walk_forward_folds, _split_train_test
from app.research.utils import RESEARCH_ARTIFACTS_DIR, RESEARCH_REPORTS_DIR, ensure_research_dirs, json_dump, save_dataframe

RESULTS_PATH = RESEARCH_REPORTS_DIR / "focused_clv_rank1to5_model_results.csv"
ABLATION_PATH = RESEARCH_REPORTS_DIR / "focused_clv_rank1to5_feature_ablation.csv"
MONOTONICITY_PATH = RESEARCH_REPORTS_DIR / "focused_clv_rank1to5_monotonicity.csv"
EXECUTION_PATH = RESEARCH_REPORTS_DIR / "focused_clv_rank1to5_execution_tests.csv"
ZONE_PATH = RESEARCH_REPORTS_DIR / "focused_clv_rank1to5_zone_stability.csv"
IMPORTANCE_PATH = RESEARCH_REPORTS_DIR / "focused_clv_rank1to5_feature_importance.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "focused_clv_rank1to5_summary.md"
BEST_MODEL_PATH = RESEARCH_ARTIFACTS_DIR / "best_focused_clv_rank1to5_model.json"
MIN_OBSERVED_FEATURE_VALUES = 25


def _targeted_report_path(base_path: Path, target: str) -> Path:
    return base_path.with_name(f"{base_path.stem}_{target}{base_path.suffix}")


def _targeted_artifact_path(base_path: Path, target: str) -> Path:
    return base_path.with_name(f"{base_path.stem}_{target}{base_path.suffix}")


def _safe_divide(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _build_focused_frame(matched_path: Path, target: str, include_large_fields: bool) -> tuple[pd.DataFrame, ClvTargetSpec, list[str], list[str]]:
    frame, target_spec, feature_columns = _prepare_clv_frame(matched_path, target)
    frame = frame[frame["market_rank_current"].between(1, 5, inclusive="both")].copy()
    if not include_large_fields:
        frame = frame[frame["field_size_bucket"].isin(["small", "medium"])].copy()

    frame["small_or_medium_fields"] = frame["field_size_bucket"].isin(["small", "medium"]).astype(int)
    frame["rank_1_to_3_flag"] = frame["market_rank_current"].between(1, 3, inclusive="both").astype(int)
    frame["rank_1_to_5_flag"] = frame["market_rank_current"].between(1, 5, inclusive="both").astype(int)
    frame["odds_2_to_10_flag"] = frame["anchor_price"].between(2.0, 10.0, inclusive="both").astype(int)
    frame["interaction_form_market_rank_current"] = frame["form_score"].fillna(0.0) * frame["market_rank_current"].fillna(0.0)
    frame["interaction_form_current_price"] = frame["form_score"].fillna(0.0) * frame["current_price"].fillna(0.0)
    frame["interaction_form_field_size"] = frame["form_score"].fillna(0.0) * frame["field_size"].fillna(0.0)
    frame["interaction_movement_market_rank_current"] = frame["movement_score"].fillna(0.0) * frame["market_rank_current"].fillna(0.0)
    frame["interaction_movement_current_price"] = frame["movement_score"].fillna(0.0) * frame["current_price"].fillna(0.0)
    frame["interaction_context_market_rank_current"] = frame["context_score"].fillna(0.0) * frame["market_rank_current"].fillna(0.0)
    frame["interaction_trainer_jockey"] = frame["trainer_stat"].fillna(0.0) * frame["jockey_stat"].fillna(0.0)
    frame["interaction_distance_change_form"] = frame["distance_change"].fillna(0.0) * frame["form_score"].fillna(0.0)
    frame["interaction_class_change_form"] = frame["class_change"].fillna(0.0) * frame["form_score"].fillna(0.0)
    frame["interaction_barrier_field_size"] = frame["barrier"].fillna(0.0) * frame["field_size"].fillna(0.0)
    frame["interaction_market_rank_field_size"] = frame["market_rank_current"].fillna(0.0) * frame["field_size"].fillna(0.0)

    engineered = [
        "small_or_medium_fields",
        "rank_1_to_3_flag",
        "rank_1_to_5_flag",
        "odds_2_to_10_flag",
        "interaction_form_market_rank_current",
        "interaction_form_current_price",
        "interaction_form_field_size",
        "interaction_movement_market_rank_current",
        "interaction_movement_current_price",
        "interaction_context_market_rank_current",
        "interaction_trainer_jockey",
        "interaction_distance_change_form",
        "interaction_class_change_form",
        "interaction_barrier_field_size",
        "interaction_market_rank_field_size",
    ]
    feature_columns = [column for column in feature_columns if column in frame.columns] + engineered
    sparse_columns = sorted(
        column
        for column in feature_columns
        if column in frame.columns and int(frame[column].notna().sum()) < MIN_OBSERVED_FEATURE_VALUES
    )
    feature_columns = [column for column in feature_columns if column not in sparse_columns]
    leakage_risk_columns = sorted(
        column
        for column in frame.columns
        if column in {"closing_price", "starting_price", "60_to_close_change", "close_market_prob_norm", "close_market_rank", "close_market_signal", "close_odds_signal", "price_5m", "price_10m", "price_30m"}
    )
    frame.attrs["sparse_dropped_feature_columns"] = sparse_columns
    return frame.reset_index(drop=True), target_spec, feature_columns, leakage_risk_columns


def _feature_groups(feature_columns: list[str]) -> dict[str, list[str]]:
    market_only = [
        column for column in feature_columns if column in {
            "market_rank_current",
            "current_price",
            "anchor_price",
            "market_prob_norm",
            "anchor_market_prob_norm",
            "odds_signal",
            "market_signal",
            "price_60m",
            "movement_score",
            "rank_1_to_3_flag",
            "rank_1_to_5_flag",
            "odds_2_to_10_flag",
        }
    ]
    non_market_only = [
        column for column in feature_columns if column not in set(market_only)
    ]
    all_features = list(feature_columns)
    all_features_minus_market = [column for column in all_features if column not in set(market_only)]
    all_features_minus_price_only = [
        column
        for column in all_features
        if column not in {"current_price", "anchor_price", "price_60m", "market_prob_norm", "anchor_market_prob_norm", "odds_signal"}
    ]
    return {
        "market_only": market_only,
        "non_market_only": non_market_only,
        "all_features": all_features,
        "all_features_minus_market": all_features_minus_market,
        "all_features_minus_price_only": all_features_minus_price_only,
    }


def _all_nan_columns(frame: pd.DataFrame, feature_columns: list[str]) -> list[str]:
    return sorted(column for column in feature_columns if column in frame.columns and frame[column].isna().all())


def _prepare_split_feature_set(train_frame: pd.DataFrame, test_frame: pd.DataFrame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
    existing = [column for column in feature_columns if column in train_frame.columns and column in test_frame.columns]
    dropped = sorted(column for column in existing if train_frame[column].isna().all())
    kept = [column for column in existing if column not in dropped]
    return kept, dropped


def _predict_focused_classifier(
    spec: Any,
    target_spec: ClvTargetSpec,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[pd.DataFrame, Any, list[str], list[str]]:
    used_features, dropped = _prepare_split_feature_set(train_frame, test_frame, feature_columns)
    working = test_frame.copy()
    if spec.name == "market_baseline":
        rank = pd.to_numeric(working[target_spec.market_rank_column], errors="coerce").clip(lower=1.0)
        working["predicted_shorten_probability"] = (1.0 / rank).clip(0.05, 0.95).fillna(0.5)
        model = None
        encoded_columns: list[str] = []
    else:
        x_train, x_test = _build_design_matrices(train_frame, test_frame, used_features)
        model = _fit_classifier(spec, x_train, train_frame[target_spec.target_flag_column])
        encoded_columns = x_train.columns.tolist()
        if model is None:
            working["predicted_shorten_probability"] = 0.5
        else:
            working["predicted_shorten_probability"] = pd.Series(model.predict_proba(x_test)[:, 1], index=working.index).clip(1e-6, 1 - 1e-6)
    working["predicted_clv_proxy"] = working["predicted_shorten_probability"] * working[target_spec.target_clv_column]
    working["predicted_movement_direction"] = np.where(working["predicted_shorten_probability"] >= 0.5, 1, -1)
    working["shorten_rank"] = working.groupby(["race_date", "track_norm", "race_number"], dropna=False)["predicted_shorten_probability"].rank(method="dense", ascending=False)
    working["model_minus_market_prob"] = working["predicted_shorten_probability"] - working[target_spec.market_prob_column]
    return working, model, encoded_columns, dropped


def _extract_feature_importance(model: Any, feature_names: list[str], model_name: str, feature_set: str, target: str) -> pd.DataFrame:
    estimator = model
    if isinstance(model, CalibratedClassifierCV):
        if not getattr(model, "calibrated_classifiers_", None):
            return pd.DataFrame()
        estimator = model.calibrated_classifiers_[0].estimator
    if isinstance(estimator, Pipeline):
        estimator = estimator.named_steps["model"]
    if hasattr(estimator, "coef_"):
        values = np.abs(np.ravel(estimator.coef_))
    elif hasattr(estimator, "feature_importances_"):
        values = np.ravel(estimator.feature_importances_)
    else:
        return pd.DataFrame()
    return pd.DataFrame(
        {
            "model_name": model_name,
            "feature_set": feature_set,
            "target": target,
            "feature_name": feature_names[: len(values)],
            "importance": values[: len(feature_names)],
        }
    ).sort_values("importance", ascending=False)


def _focused_execution_rules(frame: pd.DataFrame, model_name: str, feature_set: str) -> dict[str, pd.DataFrame]:
    def top_per_race(source: pd.DataFrame) -> pd.DataFrame:
        if source.empty:
            return source.copy()
        ordered = source.sort_values(
            ["race_date", "track_norm", "race_number", "predicted_shorten_probability", "anchor_price"],
            ascending=[True, True, True, False, True],
        )
        return ordered.groupby(["race_date", "track_norm", "race_number"], dropna=False).head(1).copy()

    rank_1_to_3 = frame["market_rank_current"].between(1, 3, inclusive="both")
    rank_1_to_5 = frame["market_rank_current"].between(1, 5, inclusive="both")
    small_medium = frame["field_size_bucket"].isin(["small", "medium"])
    odds_2_to_10 = frame["anchor_price"].between(2.0, 10.0, inclusive="both")
    non_market_proxy = frame["form_score"].fillna(0.0) + frame["context_score"].fillna(0.0)
    rules = {
        "top_1_predicted_shortener_per_race": top_per_race(frame),
        "predicted_shorten_probability_ge_0.25": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.25]),
        "predicted_shorten_probability_ge_0.30": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.30]),
        "predicted_shorten_probability_ge_0.35": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.35]),
        "predicted_shorten_probability_ge_0.40": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.40]),
        "rank_1_to_3_only": top_per_race(frame[rank_1_to_3]),
        "rank_1_to_5_only": top_per_race(frame[rank_1_to_5]),
        "small_or_medium_fields_only": top_per_race(frame[small_medium]),
        "rank_1_to_5_and_small_or_medium_fields": top_per_race(frame[rank_1_to_5 & small_medium]),
        "odds_2_to_10_and_rank_1_to_5": top_per_race(frame[odds_2_to_10 & rank_1_to_5]),
        "high_non_market_signal_plus_high_predicted_shorten": top_per_race(
            frame[(non_market_proxy >= non_market_proxy.quantile(0.75)) & (frame["predicted_shorten_probability"] >= 0.30)]
        ),
    }
    if feature_set == "all_features":
        baseline = frame["market_only_predicted_shorten_probability"] if "market_only_predicted_shorten_probability" in frame.columns else pd.Series(0.0, index=frame.index)
        rules["all_features_beats_market_only_candidates"] = top_per_race(
            frame[(frame["predicted_shorten_probability"] - baseline) >= 0.03]
        )
    return rules


def _execution_report(frame: pd.DataFrame, model_name: str, feature_set: str, target_spec: ClvTargetSpec, min_selections: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    baseline_shorten_rate = float(frame[target_spec.target_flag_column].mean()) if len(frame) else 0.0
    baseline_clv_hit_rate = float((frame[target_spec.target_clv_column] > 0).mean()) if len(frame) else 0.0
    for rule_name, selection in _focused_execution_rules(frame, model_name, feature_set).items():
        bets = _prepare_bets(selection)
        roi = _safe_divide(float(bets["profit_loss"].sum()), float(bets["stake"].sum()))
        clv_hit_rate = float((selection[target_spec.target_clv_column] > 0).mean()) if len(selection) else 0.0
        actual_shorten_rate = float(selection[target_spec.target_flag_column].mean()) if len(selection) else 0.0
        average_clv = float(selection[target_spec.target_clv_column].mean()) if len(selection) else 0.0
        robustness_score = (
            average_clv * 0.35
            + (actual_shorten_rate - baseline_shorten_rate) * 0.20
            + (clv_hit_rate - baseline_clv_hit_rate) * 0.15
            + _monthly_positive_rate(selection, target_spec.target_clv_column) * 0.10
            + _weekly_positive_rate(selection, target_spec.target_clv_column) * 0.08
            + roi * 0.04
            + float(frame.attrs.get("overall_monotonicity_score", 0.0)) * 0.08
        )
        survives = bool(
            len(selection) >= min_selections
            and average_clv > 0
            and clv_hit_rate > baseline_clv_hit_rate
            and _monthly_positive_rate(selection, target_spec.target_clv_column) >= 0.60
            and _track_concentration(selection) <= 0.45
            and _month_concentration(selection) <= 0.40
            and float(selection["anchor_price"].mean()) <= 10.0
            and float(selection["market_rank_current"].mean()) <= 5.0
            and float(frame.attrs.get("non_market_delta_vs_market_only", 0.0)) > 0.0
        )
        rows.append(
            {
                "model_name": model_name,
                "feature_set": feature_set,
                "target": target_spec.name,
                "execution_rule": rule_name,
                "selections": int(len(selection)),
                "actual_shorten_rate": actual_shorten_rate,
                "baseline_shorten_rate": baseline_shorten_rate,
                "shorten_lift": actual_shorten_rate - baseline_shorten_rate,
                "average_clv": average_clv,
                "median_clv": float(selection[target_spec.target_clv_column].median()) if len(selection) else 0.0,
                "clv_hit_rate": clv_hit_rate,
                "roi": roi,
                "strike_rate": float(selection["won_flag"].mean()) if len(selection) else 0.0,
                "average_odds": float(selection["anchor_price"].mean()) if len(selection) else 0.0,
                "average_market_rank": float(selection["market_rank_current"].mean()) if len(selection) else 0.0,
                "drawdown": _drawdown_from_profit(bets),
                "monthly_positive_clv_rate": _monthly_positive_rate(selection, target_spec.target_clv_column),
                "weekly_positive_clv_rate": _weekly_positive_rate(selection, target_spec.target_clv_column),
                "track_concentration": _track_concentration(selection),
                "month_concentration": _month_concentration(selection),
                "remove_best_winner_roi": _remove_top_winner_roi(bets, 1),
                "remove_top2_winners_roi": _remove_top_winner_roi(bets, 2),
                "remove_top5_winners_roi": _remove_top_winner_roi(bets, 5),
                "robustness_score": robustness_score,
                "survives_robustness": survives,
            }
        )
    return pd.DataFrame(rows).sort_values(["survives_robustness", "robustness_score", "average_clv"], ascending=[False, False, False]).reset_index(drop=True)


def _zone_stability_report(frame: pd.DataFrame, model_name: str, feature_set: str, target_spec: ClvTargetSpec) -> pd.DataFrame:
    working = frame.copy()
    working["market_rank_bucket"] = _market_rank_bucket(working["market_rank_current"])
    working["odds_bucket"] = _odds_bucket(working["anchor_price"])
    working["weekday_weekend"] = np.where(pd.to_datetime(working["race_date"], errors="coerce").dt.weekday >= 5, "weekend", "weekday")
    working["distance_bucket"] = pd.cut(pd.to_numeric(working["distance"], errors="coerce"), bins=[0, 1200, 1600, 2200, np.inf], labels=["sprint", "mile", "middle", "staying"], include_lowest=True).astype(str)
    rows: list[dict[str, Any]] = []
    for column in ["market_rank_bucket", "odds_bucket", "field_size_bucket", "track_norm", "race_month", "weekday_weekend", "race_class_group", "distance_bucket"]:
        if column not in working.columns:
            continue
        for value, subset in working.groupby(column, dropna=False):
            rows.append(
                {
                    "model_name": model_name,
                    "feature_set": feature_set,
                    "target": target_spec.name,
                    "zone_type": column,
                    "zone_value": str(value),
                    "selections": int(len(subset)),
                    "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                    "average_clv": float(subset[target_spec.target_clv_column].mean()),
                    "roi": _safe_divide(float(subset["profit_loss"].sum()), float(subset["stake"].sum())),
                    "average_odds": float(subset["anchor_price"].mean()),
                    "average_market_rank": float(subset["market_rank_current"].mean()),
                    "stability_warning": bool(len(subset) < 300 or _track_concentration(subset) > 0.45 or _month_concentration(subset) > 0.40),
                }
            )
    return pd.DataFrame(rows)


def _feature_ablation_report(train_frame: pd.DataFrame, test_frame: pd.DataFrame, target_spec: ClvTargetSpec, feature_groups: dict[str, list[str]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    baseline_scores: dict[tuple[str, str], float] = {}
    for spec in _model_specs():
        if spec.name == "market_baseline":
            continue
        for feature_set, columns in feature_groups.items():
            if not columns:
                continue
            predictions, _, _, dropped = _predict_focused_classifier(spec, target_spec, train_frame, test_frame, columns)
            top_bucket = predictions[predictions["predicted_shorten_probability"] >= 0.35].copy()
            auc = _safe_auc(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"])
            baseline_scores[(spec.name, feature_set)] = auc
            walk_scores = []
            for _, fold_train, fold_test in _build_walk_forward_folds(pd.concat([train_frame, test_frame], ignore_index=True), 6):
                fold_predictions, _, _, _ = _predict_focused_classifier(spec, target_spec, fold_train, fold_test, columns)
                walk_scores.append(_safe_auc(fold_predictions[target_spec.target_flag_column], fold_predictions["predicted_shorten_probability"]))
            rows.append(
                {
                    "model_name": spec.name,
                    "feature_set": feature_set,
                    "number_of_features": len(columns) - len(dropped),
                    "auc": auc,
                    "brier_score": _safe_brier(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"]),
                    "log_loss": _safe_log_loss(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"]),
                    "actual_shorten_rate": float(predictions[target_spec.target_flag_column].mean()),
                    "predicted_shorten_rate": float(predictions["predicted_shorten_probability"].mean()),
                    "average_clv": float(predictions[target_spec.target_clv_column].mean()),
                    "clv_hit_rate": float((predictions[target_spec.target_clv_column] > 0).mean()),
                    "mean_predicted_probability": float(predictions["predicted_shorten_probability"].mean()),
                    "mean_abs_calibration_gap": float((predictions["predicted_shorten_probability"] - predictions[target_spec.target_flag_column]).abs().mean()),
                    "walk_forward_auc_mean": float(np.mean(walk_scores)) if walk_scores else 0.0,
                    "walk_forward_auc_min": float(np.min(walk_scores)) if walk_scores else 0.0,
                    "leave_one_month_auc_mean": 0.0,
                    "leave_one_month_auc_min": 0.0,
                    "average_clv_top_bucket": float(top_bucket[target_spec.target_clv_column].mean()) if not top_bucket.empty else 0.0,
                    "actual_shorten_rate_top_bucket": float(top_bucket[target_spec.target_flag_column].mean()) if not top_bucket.empty else 0.0,
                }
            )
    report = pd.DataFrame(rows)
    if report.empty:
        return report
    output = []
    for row in report.to_dict("records"):
        market_auc = baseline_scores.get((row["model_name"], "market_only"), row["auc"])
        all_auc = baseline_scores.get((row["model_name"], "all_features"), row["auc"])
        row["feature_set_delta_vs_market_only"] = row["auc"] - market_auc
        row["feature_set_delta_vs_all_features"] = row["auc"] - all_auc
        row["survives_model_quality"] = bool(
            row["auc"] >= market_auc
            and row["average_clv"] > 0
            and row["walk_forward_auc_min"] > 0.55
        )
        output.append(row)
    return pd.DataFrame(output).sort_values(["model_name", "auc"], ascending=[True, False]).reset_index(drop=True)


def _summary_markdown(target_spec: ClvTargetSpec, results: pd.DataFrame, ablation: pd.DataFrame, monotonicity: pd.DataFrame, execution: pd.DataFrame, zone_stability: pd.DataFrame, leakage_columns: list[str], dropped_columns: list[str]) -> str:
    best = results.iloc[0] if not results.empty else pd.Series(dtype=object)
    best_ablation = ablation.sort_values(["survives_model_quality", "auc"], ascending=[False, False]).iloc[0] if not ablation.empty else pd.Series(dtype=object)
    best_execution = execution.sort_values(["survives_robustness", "robustness_score"], ascending=[False, False]).iloc[0] if not execution.empty else pd.Series(dtype=object)
    credible_zone = zone_stability[
        zone_stability["zone_type"].isin(["market_rank_bucket", "field_size_bucket", "odds_bucket"])
    ].sort_values(["stability_warning", "average_clv"], ascending=[True, False]).iloc[0] if not zone_stability.empty else pd.Series(dtype=object)
    lines = [
        "# Focused CLV Rank 1-5 Summary",
        "",
        f"- Target: `{target_spec.name}`",
        "",
        "## Robust Conclusion",
        f"1. Inside market rank 1–5, can we predict late shortening better than baseline? {'Yes, weakly' if bool(best.get('beats_market_only', False)) else 'Not convincingly'}",
        f"2. Does the small/medium-field zone remain credible? {'Somewhat' if not credible_zone.empty else 'Unclear'}",
        f"3. Do non-market features add value over market-only features? {'Only a little' if float(best_ablation.get('feature_set_delta_vs_market_only', 0.0)) > 0 else 'No'}",
        f"4. Is CLV positive and stable? {'Weakly positive in pockets' if not execution.empty and float(best_execution.get('average_clv', 0.0)) > 0 else 'Not stably'}",
        f"5. Is there monotonic improvement by predicted probability? {'Some evidence' if not monotonicity.empty and float(monotonicity['overall_monotonicity_score'].mean()) > 0.5 else 'Weak'}",
        f"6. Does any diagnostic execution test survive robustness? {'Yes' if not execution.empty and bool(execution['survives_robustness'].any()) else 'No'}",
        f"7. Is the signal tradable yet? No",
        f"8. Is this still mostly market re-encoding? {'Yes' if float(best_ablation.get('feature_set_delta_vs_market_only', 0.0)) <= 0.01 else 'Partly'}",
        "9. What exact feature-engineering direction should come next? Better pre-60 market context, liquidity, and stronger non-market runner/context features inside rank 1–5, small/medium fields.",
        "",
        "## Labels",
        f"- robust conclusion: best combo `{best.get('model_name', '')}` / `{best.get('feature_set', '')}` auc=`{float(best.get('auc', 0.0)):.4f}`",
        f"- weak signal: best execution `{best_execution.get('execution_rule', '')}` avg_clv=`{float(best_execution.get('average_clv', 0.0)):.4f}` survives=`{bool(best_execution.get('survives_robustness', False))}`",
        "- likely false signal: any pocket with high odds, low sample, or outlier-driven ROI",
        "- needs more data: monthly/track stability across credible zones",
        f"- worth further research: {bool(not execution.empty and float(best_execution.get('average_clv', 0.0)) > 0)}",
        "- discard: broad longshot disagreement and any strategy selected only by ROI",
        "",
        "## Data Hygiene",
        f"- Excluded leakage-risk columns: {', '.join(leakage_columns) if leakage_columns else 'none detected'}",
        f"- Dropped all-NaN features: {', '.join(dropped_columns) if dropped_columns else 'none'}",
        "",
        "## Final Principle",
        "- Truth over optimism: if this remains weak market-agreement behaviour without stable non-market lift over market-only features, it is still not an edge.",
    ]
    return "\n".join(lines) + "\n"


def run_focused_clv_rank1to5_research(
    matched_path: Path,
    *,
    target: str,
    test_size: float,
    min_selections: int,
    include_large_fields: bool,
    save_artifacts: bool,
) -> dict[str, pd.DataFrame | str]:
    ensure_research_dirs()
    print(f"Loading focused CLV rank1to5 frame for target={target}...")
    frame, target_spec, feature_columns, leakage_columns = _build_focused_frame(matched_path, target, include_large_fields)
    sparse_dropped_columns = list(frame.attrs.get("sparse_dropped_feature_columns", []))
    train_frame, test_frame = _split_train_test(frame, test_size)
    feature_groups = _feature_groups(feature_columns)
    ablation = _feature_ablation_report(train_frame, test_frame, target_spec, feature_groups)

    market_only_lookup = {
        row["model_name"]: row["auc"]
        for row in ablation[ablation["feature_set"].eq("market_only")].to_dict("records")
    }

    results_rows: list[dict[str, Any]] = []
    monotonicity_rows: list[pd.DataFrame] = []
    execution_rows: list[pd.DataFrame] = []
    zone_rows: list[pd.DataFrame] = []
    importance_rows: list[pd.DataFrame] = []
    dropped_columns_all: set[str] = set()

    for spec in _model_specs():
        if spec.name == "market_baseline":
            continue
        for feature_set, columns in feature_groups.items():
            if not columns:
                continue
            print(f"Scoring {spec.name} / {feature_set}")
            predictions, model, encoded_features, dropped = _predict_focused_classifier(spec, target_spec, train_frame, test_frame, columns)
            predictions = _prepare_bets(predictions)
            dropped_columns_all.update(dropped)
            non_market_delta = 0.0
            model_market_only = market_only_lookup.get(spec.name)
            current_auc = _safe_auc(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"])
            if model_market_only is not None:
                non_market_delta = current_auc - model_market_only
            predictions["market_only_predicted_shorten_probability"] = market_only_lookup.get(spec.name, current_auc)
            monotonicity = _monotonicity_report(predictions, spec.name, target_spec, float(predictions[target_spec.target_flag_column].mean()))
            predictions.attrs["overall_monotonicity_score"] = float(monotonicity["overall_monotonicity_score"].mean()) if not monotonicity.empty else 0.0
            predictions.attrs["non_market_delta_vs_market_only"] = non_market_delta
            execution = _execution_report(predictions, spec.name, feature_set, target_spec, min_selections)
            zone_stability = _zone_stability_report(predictions, spec.name, feature_set, target_spec)
            walk_scores = []
            lomo_scores = []
            for _, fold_train, fold_test in _build_walk_forward_folds(frame, 6):
                fold_predictions, _, _, _ = _predict_focused_classifier(spec, target_spec, fold_train, fold_test, columns)
                walk_scores.append(_safe_auc(fold_predictions[target_spec.target_flag_column], fold_predictions["predicted_shorten_probability"]))
            for _, fold_train, fold_test in _build_leave_one_month_out_folds(frame, 6):
                fold_predictions, _, _, _ = _predict_focused_classifier(spec, target_spec, fold_train, fold_test, columns)
                lomo_scores.append(_safe_auc(fold_predictions[target_spec.target_flag_column], fold_predictions["predicted_shorten_probability"]))

            results_rows.append(
                {
                    "model_name": spec.name,
                    "feature_set": feature_set,
                    "target": target_spec.name,
                    "auc": current_auc,
                    "brier_score": _safe_brier(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"]),
                    "log_loss": _safe_log_loss(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"]),
                    "actual_shorten_rate": float(predictions[target_spec.target_flag_column].mean()),
                    "predicted_shorten_rate": float(predictions["predicted_shorten_probability"].mean()),
                    "average_clv": float(predictions[target_spec.target_clv_column].mean()),
                    "clv_hit_rate": float((predictions[target_spec.target_clv_column] > 0).mean()),
                    "mean_predicted_probability": float(predictions["predicted_shorten_probability"].mean()),
                    "mean_abs_calibration_gap": float((predictions["predicted_shorten_probability"] - predictions[target_spec.target_flag_column]).abs().mean()),
                    "walk_forward_auc_mean": float(np.mean(walk_scores)) if walk_scores else 0.0,
                    "walk_forward_auc_min": float(np.min(walk_scores)) if walk_scores else 0.0,
                    "leave_one_month_auc_mean": float(np.mean(lomo_scores)) if lomo_scores else 0.0,
                    "leave_one_month_auc_min": float(np.min(lomo_scores)) if lomo_scores else 0.0,
                    "feature_set_delta_vs_market_only": non_market_delta,
                    "survives_model_quality": bool(current_auc >= market_only_lookup.get(spec.name, current_auc) and float(predictions[target_spec.target_clv_column].mean()) > 0),
                    "beats_market_only": bool(current_auc > market_only_lookup.get(spec.name, current_auc)),
                }
            )
            monotonicity_rows.append(monotonicity.assign(feature_set=feature_set))
            execution_rows.append(execution)
            zone_rows.append(zone_stability)
            if model is not None and encoded_features:
                importance_rows.append(_extract_feature_importance(model, encoded_features, spec.name, feature_set, target_spec.name))

    results = pd.DataFrame(results_rows).sort_values(["survives_model_quality", "feature_set_delta_vs_market_only", "auc"], ascending=[False, False, False]).reset_index(drop=True)
    monotonicity = pd.concat(monotonicity_rows, ignore_index=True) if monotonicity_rows else pd.DataFrame()
    execution_tests = pd.concat(execution_rows, ignore_index=True) if execution_rows else pd.DataFrame()
    zone_stability = pd.concat(zone_rows, ignore_index=True) if zone_rows else pd.DataFrame()
    feature_importance = pd.concat(importance_rows, ignore_index=True) if importance_rows else pd.DataFrame()
    summary = _summary_markdown(
        target_spec,
        results,
        ablation,
        monotonicity,
        execution_tests,
        zone_stability,
        leakage_columns,
        sorted(dropped_columns_all | set(sparse_dropped_columns)),
    )

    if save_artifacts:
        save_dataframe(results, RESULTS_PATH)
        save_dataframe(results, _targeted_report_path(RESULTS_PATH, target_spec.name))
        save_dataframe(ablation, ABLATION_PATH)
        save_dataframe(ablation, _targeted_report_path(ABLATION_PATH, target_spec.name))
        save_dataframe(monotonicity, MONOTONICITY_PATH)
        save_dataframe(monotonicity, _targeted_report_path(MONOTONICITY_PATH, target_spec.name))
        save_dataframe(execution_tests, EXECUTION_PATH)
        save_dataframe(execution_tests, _targeted_report_path(EXECUTION_PATH, target_spec.name))
        save_dataframe(zone_stability, ZONE_PATH)
        save_dataframe(zone_stability, _targeted_report_path(ZONE_PATH, target_spec.name))
        save_dataframe(feature_importance, IMPORTANCE_PATH)
        save_dataframe(feature_importance, _targeted_report_path(IMPORTANCE_PATH, target_spec.name))
        SUMMARY_PATH.write_text(summary, encoding="utf-8")
        _targeted_report_path(SUMMARY_PATH, target_spec.name).write_text(summary, encoding="utf-8")
        if not results.empty:
            json_dump(results.iloc[0].to_dict(), BEST_MODEL_PATH)
            json_dump(results.iloc[0].to_dict(), _targeted_artifact_path(BEST_MODEL_PATH, target_spec.name))

    print()
    print("Focused CLV Rank1to5 Research Summary")
    print(f"Reports: {RESULTS_PATH}, {ABLATION_PATH}, {MONOTONICITY_PATH}, {EXECUTION_PATH}, {ZONE_PATH}, {IMPORTANCE_PATH}, {SUMMARY_PATH}")
    return {
        "results": results,
        "feature_ablation": ablation,
        "monotonicity": monotonicity,
        "execution_tests": execution_tests,
        "zone_stability": zone_stability,
        "feature_importance": feature_importance,
        "summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Focused CLV-first research for rank 1-5 strong-market zones.")
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    parser.add_argument("--target", choices=["sixty_to_close"], default="sixty_to_close")
    parser.add_argument("--test-size", type=float, default=0.25)
    parser.add_argument("--min-selections", type=int, default=500)
    parser.add_argument("--include-large-fields", action="store_true")
    parser.add_argument("--save-artifacts", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_focused_clv_rank1to5_research(
        args.matched_path,
        target=args.target,
        test_size=args.test_size,
        min_selections=args.min_selections,
        include_large_fields=args.include_large_fields,
        save_artifacts=args.save_artifacts,
    )


if __name__ == "__main__":
    main()
