from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from app.research.clv_prediction_model import (
    ClvTargetSpec,
    MATCHED_PATH,
    ModelSpec,
    _prepare_bets,
    _prepare_clv_frame,
)
from app.research.market_residual_model import _build_leave_one_month_out_folds, _build_walk_forward_folds, _split_train_test
from app.research.utils import RESEARCH_ARTIFACTS_DIR, RESEARCH_REPORTS_DIR, ensure_research_dirs, json_dump, save_dataframe

try:
    from lightgbm import LGBMClassifier, LGBMRegressor
except ImportError:
    LGBMClassifier = None
    LGBMRegressor = None

try:
    from xgboost import XGBClassifier, XGBRegressor
except ImportError:
    XGBClassifier = None
    XGBRegressor = None

RESULTS_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_model_results.csv"
SEGMENTS_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_segments.csv"
EXECUTION_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_execution_tests.csv"
CALIBRATION_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_calibration.csv"
FEATURE_IMPORTANCE_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_feature_importance.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_summary.md"
BEST_MODEL_PATH = RESEARCH_ARTIFACTS_DIR / "best_clv_market_agreement_model.json"

MONOTONICITY_PATH = RESEARCH_REPORTS_DIR / "clv_monotonicity_curve.csv"
BUCKET_QUALITY_PATH = RESEARCH_REPORTS_DIR / "clv_prediction_bucket_quality.csv"
ZONE_QUALITY_PATH = RESEARCH_REPORTS_DIR / "clv_market_zone_quality.csv"
REGRESSION_PATH = RESEARCH_REPORTS_DIR / "clv_market_movement_regression.csv"
CANDIDATE_FORENSICS_PATH = RESEARCH_REPORTS_DIR / "clv_candidate_signal_forensics.csv"
FEATURE_UNIQUENESS_PATH = RESEARCH_REPORTS_DIR / "clv_feature_uniqueness.csv"
DEEP_SUMMARY_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_deep_summary.md"

PROBABILITY_BUCKETS = [-np.inf, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, np.inf]
PROBABILITY_LABELS = ["0.00-0.10", "0.10-0.15", "0.15-0.20", "0.20-0.25", "0.25-0.30", "0.30-0.35", "0.35-0.40", "0.40+"]
COARSE_BUCKETS = [-np.inf, 0.45, 0.50, 0.55, 0.60, 0.65, np.inf]
COARSE_LABELS = ["0.00-0.45", "0.45-0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65+"]


def _targeted_report_path(base_path: Path, target: str) -> Path:
    return base_path.with_name(f"{base_path.stem}_{target}{base_path.suffix}")


def _write_report(frame: pd.DataFrame, base_path: Path, target: str) -> None:
    save_dataframe(frame, base_path)
    save_dataframe(frame, _targeted_report_path(base_path, target))


def _write_text(text: str, base_path: Path, target: str) -> None:
    base_path.write_text(text, encoding="utf-8")
    _targeted_report_path(base_path, target).write_text(text, encoding="utf-8")


def _write_artifact(payload: dict[str, Any], base_path: Path, target: str) -> None:
    json_dump(payload, base_path)
    json_dump(payload, _targeted_report_path(base_path, target))


def _safe_auc(y_true: pd.Series, y_pred: pd.Series) -> float:
    truth = pd.to_numeric(y_true, errors="coerce").fillna(0.0)
    pred = pd.to_numeric(y_pred, errors="coerce").fillna(0.0)
    if truth.nunique(dropna=True) < 2:
        return 0.5
    try:
        return float(roc_auc_score(truth, pred))
    except ValueError:
        return 0.5


def _safe_brier(y_true: pd.Series, y_pred: pd.Series) -> float:
    truth = pd.to_numeric(y_true, errors="coerce").fillna(0.0)
    pred = pd.to_numeric(y_pred, errors="coerce").clip(0.0, 1.0).fillna(0.0)
    return float(brier_score_loss(truth, pred)) if len(truth) else 0.0


def _safe_log_loss(y_true: pd.Series, y_pred: pd.Series) -> float:
    truth = pd.to_numeric(y_true, errors="coerce").fillna(0.0)
    pred = pd.to_numeric(y_pred, errors="coerce").clip(1e-6, 1 - 1e-6).fillna(0.5)
    if not len(truth):
        return 0.0
    try:
        return float(log_loss(truth, pred, labels=[0, 1]))
    except ValueError:
        return 0.0


def _safe_corr(series_a: pd.Series, series_b: pd.Series, method: str) -> float:
    pair = pd.concat([pd.to_numeric(series_a, errors="coerce"), pd.to_numeric(series_b, errors="coerce")], axis=1).dropna()
    if len(pair) < 3:
        return 0.0
    value = pair.iloc[:, 0].corr(pair.iloc[:, 1], method=method)
    return float(value) if pd.notna(value) else 0.0


def _safe_divide(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _market_rank_bucket(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        numeric,
        bins=[0, 1, 2, 3, 5, np.inf],
        labels=["rank_1", "rank_2", "rank_3", "rank_4_to_5", "rank_6_plus"],
        include_lowest=True,
    ).astype(str)


def _odds_bucket(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        numeric,
        bins=[0, 2, 4, 6, 10, np.inf],
        labels=["odds_1_to_2", "odds_2_to_4", "odds_4_to_6", "odds_6_to_10", "odds_10_plus"],
        include_lowest=True,
    ).astype(str)


def _prediction_bucket(series: pd.Series) -> pd.Series:
    return pd.cut(pd.to_numeric(series, errors="coerce"), bins=PROBABILITY_BUCKETS, labels=PROBABILITY_LABELS, include_lowest=True).astype(str)


def _coarse_prediction_bucket(series: pd.Series) -> pd.Series:
    return pd.cut(pd.to_numeric(series, errors="coerce"), bins=COARSE_BUCKETS, labels=COARSE_LABELS, include_lowest=True).astype(str)


def _weekly_positive_rate(frame: pd.DataFrame, value_column: str) -> float:
    if frame.empty:
        return 0.0
    weekly = frame.groupby("race_week", dropna=False)[value_column].mean()
    return float((weekly > 0).mean()) if len(weekly) else 0.0


def _monthly_positive_rate(frame: pd.DataFrame, value_column: str) -> float:
    if frame.empty:
        return 0.0
    monthly = frame.groupby("race_month", dropna=False)[value_column].mean()
    return float((monthly > 0).mean()) if len(monthly) else 0.0


def _track_concentration(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    return float(frame["track_norm"].value_counts(normalize=True, dropna=False).max())


def _month_concentration(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    return float(frame["race_month"].value_counts(normalize=True, dropna=False).max())


def _drawdown_from_profit(bets: pd.DataFrame) -> float:
    if bets.empty:
        return 0.0
    curve = 1000.0 + bets["profit_loss"].cumsum().to_numpy()
    peaks = np.maximum.accumulate(curve)
    return float(np.max(peaks - curve))


def _remove_top_winner_roi(bets: pd.DataFrame, winners_to_remove: int) -> float:
    if bets.empty:
        return 0.0
    winners = bets[bets["profit_loss"] > 0].sort_values("profit_loss", ascending=False)
    remaining = bets.drop(index=winners.head(winners_to_remove).index)
    return _safe_divide(float(remaining["profit_loss"].sum()), float(remaining["stake"].sum()))


def _remove_top_clv_average(frame: pd.DataFrame, clv_column: str, count: int) -> float:
    if frame.empty:
        return 0.0
    trimmed = frame.sort_values(clv_column, ascending=False).iloc[count:].copy()
    return float(trimmed[clv_column].mean()) if len(trimmed) else 0.0


def _model_specs() -> list[ModelSpec]:
    specs = [
        ModelSpec("market_baseline", "baseline", False),
        ModelSpec("logistic", "logistic", False),
        ModelSpec("calibrated_logistic", "logistic", True),
        ModelSpec("random_forest", "random_forest", True),
        ModelSpec("gradient_boosting", "gradient_boosting", True),
    ]
    if LGBMClassifier is not None:
        specs.append(ModelSpec("lightgbm", "lightgbm", False))
    else:
        print("LightGBM not installed; skipping optional classifier.")
    if XGBClassifier is not None:
        specs.append(ModelSpec("xgboost", "xgboost", False))
    else:
        print("XGBoost not installed; skipping optional classifier.")
    return specs


def _all_nan_columns(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    return sorted(column for column in columns if column in frame.columns and frame[column].isna().all())


def _prepare_feature_set(frame: pd.DataFrame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
    existing = [column for column in feature_columns if column in frame.columns]
    dropped = _all_nan_columns(frame, existing)
    kept = [column for column in existing if column not in dropped]
    return kept, dropped


def _build_design_matrices(train_frame: pd.DataFrame, test_frame: pd.DataFrame, feature_columns: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_features = train_frame[feature_columns].copy()
    test_features = test_frame[feature_columns].copy()
    combined = pd.concat([train_features, test_features], axis=0, ignore_index=True)
    combined = pd.get_dummies(combined, dummy_na=True)
    keep = [column for column in combined.columns if combined[column].notna().any()]
    combined = combined[keep].copy()
    x_train = combined.iloc[: len(train_features)].reset_index(drop=True)
    x_test = combined.iloc[len(train_features) :].reset_index(drop=True)
    return x_train, x_test


def _fit_classifier(spec: ModelSpec, x_train: pd.DataFrame, y_train: pd.Series) -> Any:
    if spec.name == "market_baseline":
        return None
    if spec.estimator_type == "logistic":
        estimator = LogisticRegression(max_iter=1000, class_weight="balanced")
    elif spec.estimator_type == "random_forest":
        estimator = RandomForestClassifier(
            n_estimators=250,
            max_depth=8,
            min_samples_leaf=5,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1,
        )
    elif spec.estimator_type == "gradient_boosting":
        estimator = HistGradientBoostingClassifier(max_iter=250, learning_rate=0.05, max_depth=5, random_state=42)
    elif spec.estimator_type == "lightgbm" and LGBMClassifier is not None:
        estimator = LGBMClassifier(n_estimators=250, learning_rate=0.05, max_depth=6, random_state=42)
    elif spec.estimator_type == "xgboost" and XGBClassifier is not None:
        estimator = XGBClassifier(
            n_estimators=250,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            eval_metric="logloss",
            random_state=42,
        )
    else:
        return None
    steps: list[tuple[str, Any]] = [("imputer", SimpleImputer(strategy="median"))]
    if spec.estimator_type == "logistic":
        steps.append(("scaler", StandardScaler()))
    pipeline = Pipeline(steps + [("model", estimator)])
    if spec.calibrated:
        model = CalibratedClassifierCV(pipeline, method="sigmoid", cv=3)
    else:
        model = pipeline
    model.fit(x_train, y_train)
    return model


def _predict_classifier(spec: ModelSpec, target_spec: ClvTargetSpec, train_frame: pd.DataFrame, test_frame: pd.DataFrame, feature_columns: list[str]) -> tuple[pd.DataFrame, Any, list[str], list[str]]:
    used_features, dropped = _prepare_feature_set(pd.concat([train_frame, test_frame], ignore_index=True), feature_columns)
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


def _fit_regressor(kind: str, x_train: pd.DataFrame, y_train: pd.Series) -> Any:
    if kind == "ridge":
        model = Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler()), ("model", Ridge(alpha=1.0))])
    elif kind == "random_forest":
        model = Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", RandomForestRegressor(n_estimators=250, max_depth=8, min_samples_leaf=5, random_state=42, n_jobs=-1))])
    elif kind == "gradient_boosting":
        model = Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", HistGradientBoostingRegressor(max_iter=250, learning_rate=0.05, max_depth=5, random_state=42))])
    elif kind == "lightgbm" and LGBMRegressor is not None:
        model = Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", LGBMRegressor(n_estimators=250, learning_rate=0.05, max_depth=6, random_state=42))])
    elif kind == "xgboost" and XGBRegressor is not None:
        model = Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", XGBRegressor(n_estimators=250, max_depth=5, learning_rate=0.05, subsample=0.9, colsample_bytree=0.9, random_state=42))])
    else:
        return None
    model.fit(x_train, y_train)
    return model


def _fit_regression_models(train_frame: pd.DataFrame, test_frame: pd.DataFrame, feature_columns: list[str], target_spec: ClvTargetSpec) -> tuple[pd.DataFrame, pd.DataFrame]:
    used_features, _ = _prepare_feature_set(pd.concat([train_frame, test_frame], ignore_index=True), feature_columns)
    x_train, x_test = _build_design_matrices(train_frame, test_frame, used_features)
    models = ["ridge", "random_forest", "gradient_boosting"]
    if LGBMRegressor is not None:
        models.append("lightgbm")
    if XGBRegressor is not None:
        models.append("xgboost")

    rows: list[dict[str, Any]] = []
    prediction_columns: list[pd.Series] = []
    y_true = train_frame[target_spec.target_clv_column]
    holdout_true = test_frame[target_spec.target_clv_column]

    for name in models:
        model = _fit_regressor(name, x_train, y_true)
        if model is None:
            continue
        predicted = pd.Series(model.predict(x_test), index=test_frame.index)
        prediction_columns.append(predicted.rename(f"{name}_predicted_clv"))
        residuals = predicted - holdout_true
        decile = pd.qcut(predicted.rank(method="first"), q=min(10, max(2, len(predicted) // 300)), duplicates="drop")
        decile_summary = (
            pd.DataFrame({"pred": predicted, "actual": holdout_true, "bucket": decile})
            .groupby("bucket", observed=False)
            .agg(pred_mean=("pred", "mean"), actual_mean=("actual", "mean"))
            .reset_index()
        )
        rows.append(
            {
                "model_name": name,
                "target": target_spec.name,
                "mae": float(mean_absolute_error(holdout_true, predicted)),
                "rmse": float(np.sqrt(mean_squared_error(holdout_true, predicted))),
                "spearman_corr": _safe_corr(predicted, holdout_true, "spearman"),
                "pearson_corr": _safe_corr(predicted, holdout_true, "pearson"),
                "top_decile_average_actual_clv": float(decile_summary["actual_mean"].iloc[-1]) if not decile_summary.empty else 0.0,
                "bottom_decile_average_actual_clv": float(decile_summary["actual_mean"].iloc[0]) if not decile_summary.empty else 0.0,
                "decile_spread": float(decile_summary["actual_mean"].iloc[-1] - decile_summary["actual_mean"].iloc[0]) if len(decile_summary) >= 2 else 0.0,
                "monotonic_decile_score": float((decile_summary["actual_mean"].diff().fillna(0) >= 0).mean()) if not decile_summary.empty else 0.0,
            }
        )
    report = pd.DataFrame(rows).sort_values(["spearman_corr", "decile_spread"], ascending=[False, False]).reset_index(drop=True)
    predictions = pd.concat(prediction_columns, axis=1) if prediction_columns else pd.DataFrame(index=test_frame.index)
    return report, predictions


def _feature_groups(feature_columns: list[str]) -> dict[str, list[str]]:
    market_only = [
        column for column in feature_columns if column in {
            "market_rank_current",
            "market_signal",
            "odds_signal",
            "anchor_market_prob_norm",
            "anchor_price",
            "opening_price",
            "price_60m",
            "open_market_rank",
            "open_market_signal",
            "open_market_prob_norm",
            "sixty_market_rank",
            "sixty_market_signal",
            "sixty_market_prob_norm",
        }
    ]
    non_market = [
        column for column in feature_columns if column not in set(market_only)
    ]
    all_features = list(feature_columns)
    all_minus_market = [column for column in all_features if column not in set(market_only)]
    all_minus_movement = [column for column in all_features if "movement" not in column]
    return {
        "market_only_features": market_only,
        "non_market_features_only": non_market,
        "all_features": all_features,
        "all_features_minus_market": all_minus_market,
        "all_features_minus_movement": all_minus_movement,
    }


def _feature_uniqueness_report(train_frame: pd.DataFrame, test_frame: pd.DataFrame, target_spec: ClvTargetSpec, feature_columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    grouped_features = _feature_groups(feature_columns)
    model_specs = [ModelSpec("logistic", "logistic", False), ModelSpec("random_forest", "random_forest", True), ModelSpec("gradient_boosting", "gradient_boosting", True)]

    baseline_scores: dict[tuple[str, str], dict[str, float]] = {}
    for model_spec in model_specs:
        for feature_set_name, columns in grouped_features.items():
            if not columns:
                continue
            predictions, _, _, _ = _predict_classifier(model_spec, target_spec, train_frame, test_frame, columns)
            top_bucket = predictions.copy()
            top_bucket["bucket"] = _coarse_prediction_bucket(top_bucket["predicted_shorten_probability"])
            top_subset = top_bucket[top_bucket["bucket"].isin(["0.60-0.65", "0.65+"])]
            row = {
                "model_name": model_spec.name,
                "feature_set": feature_set_name,
                "number_of_features": len(columns),
                "auc": _safe_auc(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"]),
                "brier_score": _safe_brier(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"]),
                "log_loss": _safe_log_loss(predictions[target_spec.target_flag_column], predictions["predicted_shorten_probability"]),
                "average_clv_top_bucket": float(top_subset[target_spec.target_clv_column].mean()) if not top_subset.empty else 0.0,
                "actual_shorten_rate_top_bucket": float(top_subset[target_spec.target_flag_column].mean()) if not top_subset.empty else 0.0,
            }
            baseline_scores[(model_spec.name, feature_set_name)] = row
            rows.append(row)
    report = pd.DataFrame(rows)
    if report.empty:
        return report
    output_rows: list[dict[str, Any]] = []
    for row in report.to_dict("records"):
        market_ref = baseline_scores.get((row["model_name"], "market_only_features"), {})
        all_ref = baseline_scores.get((row["model_name"], "all_features"), {})
        row["feature_set_delta_vs_market_only"] = float(row["auc"] - market_ref.get("auc", row["auc"]))
        row["feature_set_delta_vs_all_features"] = float(row["auc"] - all_ref.get("auc", row["auc"]))
        output_rows.append(row)
    return pd.DataFrame(output_rows).sort_values(["model_name", "auc"], ascending=[True, False]).reset_index(drop=True)


def _monotonicity_report(frame: pd.DataFrame, model_name: str, target_spec: ClvTargetSpec, baseline_shorten_rate: float) -> pd.DataFrame:
    working = frame.copy()
    working["bucket"] = _prediction_bucket(working["predicted_shorten_probability"])
    grouped = working.groupby("bucket", dropna=False, observed=False)
    rows: list[dict[str, Any]] = []
    for bucket, subset in grouped:
        rows.append(
            {
                "model_name": model_name,
                "target": target_spec.name,
                "bucket": str(bucket),
                "runners": int(len(subset)),
                "races": int(subset[["race_date", "track_norm", "race_number"]].drop_duplicates().shape[0]),
                "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                "baseline_shorten_rate": baseline_shorten_rate,
                "shorten_lift": float(subset[target_spec.target_flag_column].mean() - baseline_shorten_rate),
                "average_clv": float(subset[target_spec.target_clv_column].mean()),
                "median_clv": float(subset[target_spec.target_clv_column].median()),
                "clv_hit_rate": float((subset[target_spec.target_clv_column] > 0).mean()),
                "average_roi": _safe_divide(float(subset["profit_loss"].sum()), float(subset["stake"].sum())),
                "strike_rate": float(subset["won_flag"].mean()),
                "average_odds": float(subset[target_spec.anchor_price_column].mean()),
                "average_market_rank": float(subset[target_spec.market_rank_column].mean()),
                "average_field_size": float(subset["field_size"].mean()),
                "month_count": int(subset["race_month"].nunique()),
                "track_count": int(subset["track_norm"].nunique()),
                "concentration_warning": bool(_track_concentration(subset) > 0.45 or _month_concentration(subset) > 0.40),
            }
        )
    report = pd.DataFrame(rows)
    if report.empty:
        return report
    report = report.sort_values("bucket").reset_index(drop=True)
    report["monotonic_shorten_rate_pass"] = (report["actual_shorten_rate"].diff().fillna(0) >= -0.01).cummin()
    report["monotonic_average_clv_pass"] = (report["average_clv"].diff().fillna(0) >= -0.05).cummin()
    report["monotonic_clv_hit_rate_pass"] = (report["clv_hit_rate"].diff().fillna(0) >= -0.02).cummin()
    report["overall_monotonicity_score"] = (
        report["monotonic_shorten_rate_pass"].astype(float)
        + report["monotonic_average_clv_pass"].astype(float)
        + report["monotonic_clv_hit_rate_pass"].astype(float)
    ) / 3.0
    return report


def _curve_report(frame: pd.DataFrame, model_name: str, validation_label: str, target_spec: ClvTargetSpec) -> pd.DataFrame:
    working = frame.copy()
    working["prediction_bucket"] = _coarse_prediction_bucket(working["predicted_shorten_probability"])
    working["market_rank_bucket"] = _market_rank_bucket(working[target_spec.market_rank_column])
    rows: list[dict[str, Any]] = []
    for bucket, subset in working.groupby("prediction_bucket", dropna=False, observed=False):
        rows.append(
            {
                "model_name": model_name,
                "validation_label": validation_label,
                "target": target_spec.name,
                "prediction_bucket": str(bucket),
                "runners": int(len(subset)),
                "races": int(subset[["race_date", "track_norm", "race_number"]].drop_duplicates().shape[0]),
                "predicted_shorten_probability": float(subset["predicted_shorten_probability"].mean()),
                "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                "average_clv": float(subset[target_spec.target_clv_column].mean()),
                "roi": _safe_divide(float(subset["profit_loss"].sum()), float(subset["stake"].sum())),
                "average_odds": float(subset[target_spec.anchor_price_column].mean()),
                "average_market_rank": float(subset[target_spec.market_rank_column].mean()),
                "market_rank_1_share": float((subset["market_rank_bucket"] == "rank_1").mean()),
                "market_rank_2_share": float((subset["market_rank_bucket"] == "rank_2").mean()),
                "market_rank_3_share": float((subset["market_rank_bucket"] == "rank_3").mean()),
                "market_rank_4_to_5_share": float((subset["market_rank_bucket"] == "rank_4_to_5").mean()),
                "market_rank_6_plus_share": float((subset["market_rank_bucket"] == "rank_6_plus").mean()),
                "calibration_gap": float(subset["predicted_shorten_probability"].mean() - subset[target_spec.target_flag_column].mean()),
            }
        )
    return pd.DataFrame(rows)


def _zone_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    rank = pd.to_numeric(frame["market_rank_current"], errors="coerce")
    odds = pd.to_numeric(frame["anchor_price"], errors="coerce")
    field_bucket = frame["field_size_bucket"].astype(str)
    return {
        "rank_1": rank.eq(1),
        "rank_2": rank.eq(2),
        "rank_3": rank.eq(3),
        "rank_4_to_5": rank.between(4, 5, inclusive="both"),
        "rank_1_to_5": rank.between(1, 5, inclusive="both"),
        "rank_6_plus": rank >= 6,
        "odds_1_to_2": odds.between(1.0, 2.0, inclusive="both"),
        "odds_2_to_4": odds.between(2.0, 4.0, inclusive="both"),
        "odds_4_to_6": odds.between(4.0, 6.0, inclusive="both"),
        "odds_6_to_10": odds.between(6.0, 10.0, inclusive="both"),
        "odds_3_to_10": odds.between(3.0, 10.0, inclusive="both"),
        "odds_10_plus": odds > 10.0,
        "small": field_bucket.eq("small"),
        "medium": field_bucket.eq("medium"),
        "large": field_bucket.eq("large"),
        "market_rank_1_to_5_AND_odds_3_to_10": rank.between(1, 5, inclusive="both") & odds.between(3.0, 10.0, inclusive="both"),
        "market_rank_1_to_5_AND_odds_2_to_8": rank.between(1, 5, inclusive="both") & odds.between(2.0, 8.0, inclusive="both"),
        "market_rank_1_to_3_AND_odds_2_to_6": rank.between(1, 3, inclusive="both") & odds.between(2.0, 6.0, inclusive="both"),
        "market_rank_4_to_5_AND_odds_4_to_10": rank.between(4, 5, inclusive="both") & odds.between(4.0, 10.0, inclusive="both"),
        "medium_fields_AND_odds_3_to_10": field_bucket.eq("medium") & odds.between(3.0, 10.0, inclusive="both"),
        "small_or_medium_fields_AND_market_rank_1_to_5": field_bucket.isin(["small", "medium"]) & rank.between(1, 5, inclusive="both"),
    }


def _zone_quality_report(frame: pd.DataFrame, model_name: str, target_spec: ClvTargetSpec, overall_auc: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for zone_name, mask in _zone_masks(frame).items():
        subset = frame[mask].copy()
        if subset.empty:
            continue
        zone_auc = _safe_auc(subset[target_spec.target_flag_column], subset["predicted_shorten_probability"])
        rows.append(
            {
                "model_name": model_name,
                "zone_name": zone_name,
                "runners": int(len(subset)),
                "races": int(subset[["race_date", "track_norm", "race_number"]].drop_duplicates().shape[0]),
                "auc": zone_auc,
                "brier_score": _safe_brier(subset[target_spec.target_flag_column], subset["predicted_shorten_probability"]),
                "log_loss": _safe_log_loss(subset[target_spec.target_flag_column], subset["predicted_shorten_probability"]),
                "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                "predicted_shorten_rate": float(subset["predicted_shorten_probability"].mean()),
                "average_clv": float(subset[target_spec.target_clv_column].mean()),
                "clv_hit_rate": float((subset[target_spec.target_clv_column] > 0).mean()),
                "average_roi": _safe_divide(float(subset["profit_loss"].sum()), float(subset["stake"].sum())),
                "average_odds": float(subset[target_spec.anchor_price_column].mean()),
                "average_market_rank": float(subset[target_spec.market_rank_column].mean()),
                "walk_forward_auc_mean": overall_auc,
                "walk_forward_auc_min": overall_auc,
                "monthly_positive_clv_rate": _monthly_positive_rate(subset, target_spec.target_clv_column),
                "survives_zone_quality": bool(
                    len(subset) >= 500
                    and float(subset[target_spec.target_clv_column].mean()) > 0
                    and zone_auc >= overall_auc - 0.01
                    and _monthly_positive_rate(subset, target_spec.target_clv_column) >= 0.55
                    and float(subset[target_spec.market_rank_column].mean()) <= 5.5
                    and float(subset[target_spec.anchor_price_column].mean()) <= 10.0
                ),
            }
        )
    return pd.DataFrame(rows)


def _basic_segment_report(frame: pd.DataFrame, model_name: str, target_spec: ClvTargetSpec) -> pd.DataFrame:
    working = frame.copy()
    working["market_rank_bucket"] = _market_rank_bucket(working[target_spec.market_rank_column])
    working["odds_bucket"] = _odds_bucket(working[target_spec.anchor_price_column])
    rows: list[dict[str, Any]] = []
    for column in ["field_size_bucket", "market_rank_bucket", "odds_bucket"]:
        for value, subset in working.groupby(column, dropna=False):
            rows.append(
                {
                    "model_name": model_name,
                    "target": target_spec.name,
                    "segment_type": column,
                    "segment_value": str(value),
                    "runners": int(len(subset)),
                    "races": int(subset[["race_date", "track_norm", "race_number"]].drop_duplicates().shape[0]),
                    "predicted_shorten_probability": float(subset["predicted_shorten_probability"].mean()),
                    "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                    "average_clv": float(subset[target_spec.target_clv_column].mean()),
                    "average_odds": float(subset[target_spec.anchor_price_column].mean()),
                    "average_market_rank": float(subset[target_spec.market_rank_column].mean()),
                }
            )
    return pd.DataFrame(rows)


def _candidate_signal_forensics(frame: pd.DataFrame, target_spec: ClvTargetSpec) -> pd.DataFrame:
    candidate = frame[
        frame["anchor_price"].between(3.0, 10.0, inclusive="both")
        & frame["model_name"].eq("random_forest")
    ].copy()
    if candidate.empty:
        return pd.DataFrame()
    bets = _prepare_bets(candidate)
    monthly = candidate.groupby("race_month", dropna=False).agg(roi=("profit_loss", lambda x: _safe_divide(float(x.sum()), float(candidate.loc[x.index, "stake"].sum()))), clv=(target_spec.target_clv_column, "mean"))
    weekly = candidate.groupby("race_week", dropna=False).agg(roi=("profit_loss", lambda x: _safe_divide(float(x.sum()), float(candidate.loc[x.index, "stake"].sum()))), clv=(target_spec.target_clv_column, "mean"))
    by_odds = candidate.groupby(_odds_bucket(candidate["anchor_price"]), dropna=False).size()
    by_rank = candidate.groupby(_market_rank_bucket(candidate["market_rank_current"]), dropna=False).size()
    row = {
        "model_name": "random_forest",
        "zone_name": "odds_3_to_10_only",
        "total_selections": int(len(candidate)),
        "wins": int(candidate["won_flag"].sum()),
        "strike_rate": float(candidate["won_flag"].mean()),
        "roi": _safe_divide(float(bets["profit_loss"].sum()), float(bets["stake"].sum())),
        "average_clv": float(candidate[target_spec.target_clv_column].mean()),
        "median_clv": float(candidate[target_spec.target_clv_column].median()),
        "clv_hit_rate": float((candidate[target_spec.target_clv_column] > 0).mean()),
        "average_odds": float(candidate[target_spec.anchor_price_column].mean()),
        "average_market_rank": float(candidate[target_spec.market_rank_column].mean()),
        "drawdown": _drawdown_from_profit(bets),
        "monthly_roi_mean": float(monthly["roi"].mean()) if not monthly.empty else 0.0,
        "monthly_clv_mean": float(monthly["clv"].mean()) if not monthly.empty else 0.0,
        "weekly_roi_mean": float(weekly["roi"].mean()) if not weekly.empty else 0.0,
        "weekly_clv_mean": float(weekly["clv"].mean()) if not weekly.empty else 0.0,
        "track_concentration": _track_concentration(candidate),
        "month_concentration": _month_concentration(candidate),
        "odds_bucket_contribution": by_odds.to_dict(),
        "market_rank_contribution": by_rank.to_dict(),
        "remove_best_winner_roi": _remove_top_winner_roi(bets, 1),
        "remove_top2_winners_roi": _remove_top_winner_roi(bets, 2),
        "remove_top5_winners_roi": _remove_top_winner_roi(bets, 5),
        "remove_best_clv_outlier_average_clv": _remove_top_clv_average(candidate, target_spec.target_clv_column, 1),
        "remove_top5_clv_outliers_average_clv": _remove_top_clv_average(candidate, target_spec.target_clv_column, 5),
        "positive_month_share": float((monthly["roi"] > 0).mean()) if not monthly.empty else 0.0,
        "positive_clv_month_share": float((monthly["clv"] > 0).mean()) if not monthly.empty else 0.0,
        "first_half_roi": _safe_divide(float(bets.iloc[: len(bets) // 2]["profit_loss"].sum()), float(bets.iloc[: len(bets) // 2]["stake"].sum())),
        "second_half_roi": _safe_divide(float(bets.iloc[len(bets) // 2 :]["profit_loss"].sum()), float(bets.iloc[len(bets) // 2 :]["stake"].sum())),
        "first_half_clv": float(candidate.iloc[: len(candidate) // 2][target_spec.target_clv_column].mean()) if len(candidate) else 0.0,
        "second_half_clv": float(candidate.iloc[len(candidate) // 2 :][target_spec.target_clv_column].mean()) if len(candidate) else 0.0,
    }
    row["high_month_concentration"] = bool(row["month_concentration"] > 0.40)
    row["high_track_concentration"] = bool(row["track_concentration"] > 0.45)
    row["low_sample_size"] = bool(row["total_selections"] < 500)
    row["poor_clv_stability"] = bool(row["positive_clv_month_share"] < 0.60)
    row["roi_outlier_driven"] = bool(row["remove_top2_winners_roi"] < 0)
    row["clv_outlier_driven"] = bool(row["remove_top5_clv_outliers_average_clv"] <= 0)
    row["likely_false_positive"] = bool(row["roi_outlier_driven"] or row["poor_clv_stability"] or row["high_month_concentration"])
    row["worth_further_research"] = bool(
        row["total_selections"] >= 500
        and row["average_clv"] > 0
        and row["positive_clv_month_share"] >= 0.60
        and not row["likely_false_positive"]
    )
    return pd.DataFrame([row])


def _execution_rules(frame: pd.DataFrame, regression_column: str | None) -> dict[str, pd.DataFrame]:
    def top_per_race(source: pd.DataFrame) -> pd.DataFrame:
        if source.empty:
            return source.copy()
        ordered = source.sort_values(
            ["race_date", "track_norm", "race_number", "predicted_shorten_probability", "anchor_price"],
            ascending=[True, True, True, False, True],
        )
        return ordered.groupby(["race_date", "track_norm", "race_number"], dropna=False).head(1).copy()

    rank_1_5 = frame["market_rank_current"].between(1, 5, inclusive="both")
    odds_3_10 = frame["anchor_price"].between(3.0, 10.0, inclusive="both")
    small_medium = frame["field_size_bucket"].isin(["small", "medium"])
    rules = {
        "top_1_predicted_shortener_per_race": top_per_race(frame),
        "predicted_shorten_probability_ge_0.25": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.25]),
        "predicted_shorten_probability_ge_0.30": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.30]),
        "predicted_shorten_probability_ge_0.35": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.35]),
        "predicted_shorten_probability_ge_0.40": top_per_race(frame[frame["predicted_shorten_probability"] >= 0.40]),
        "market_rank_1_to_5_only": top_per_race(frame[rank_1_5]),
        "odds_3_to_10_only": top_per_race(frame[odds_3_10]),
        "market_rank_1_to_5_and_odds_3_to_10": top_per_race(frame[rank_1_5 & odds_3_10]),
        "small_or_medium_fields_and_market_rank_1_to_5": top_per_race(frame[small_medium & rank_1_5]),
    }
    if regression_column and regression_column in frame.columns:
        top10_cut = frame[regression_column].quantile(0.9)
        top20_cut = frame[regression_column].quantile(0.8)
        rules["high_predicted_clv_regression_score_top_10_percent"] = top_per_race(frame[frame[regression_column] >= top10_cut])
        rules["high_predicted_clv_regression_score_top_20_percent"] = top_per_race(frame[frame[regression_column] >= top20_cut])
    return rules


def _execution_report(frame: pd.DataFrame, target_spec: ClvTargetSpec, regression_column: str | None, min_selections: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    baseline_hit_rate = float((frame[target_spec.target_clv_column] > 0).mean()) if len(frame) else 0.0
    for rule_name, selection in _execution_rules(frame, regression_column).items():
        bets = _prepare_bets(selection)
        stake = float(bets["stake"].sum())
        roi = _safe_divide(float(bets["profit_loss"].sum()), stake)
        clv_hit_rate = float((selection[target_spec.target_clv_column] > 0).mean()) if len(selection) else 0.0
        robustness_score = (
            float(selection[target_spec.target_clv_column].mean()) * 0.35
            + clv_hit_rate * 0.20
            + _monthly_positive_rate(selection, target_spec.target_clv_column) * 0.15
            + _weekly_positive_rate(selection, target_spec.target_clv_column) * 0.10
            + roi * 0.05
            - _track_concentration(selection) * 0.03
            - _month_concentration(selection) * 0.02
        ) if len(selection) else 0.0
        survives = bool(
            len(selection) >= min_selections
            and float(selection[target_spec.target_clv_column].mean()) > 0
            and clv_hit_rate > baseline_hit_rate
            and _monthly_positive_rate(selection, target_spec.target_clv_column) >= 0.60
            and _track_concentration(selection) <= 0.45
            and _month_concentration(selection) <= 0.40
            and float(selection[target_spec.anchor_price_column].mean()) <= 10.0
        )
        rows.append(
            {
                "execution_rule": rule_name,
                "selections": int(len(selection)),
                "actual_shorten_rate": float(selection[target_spec.target_flag_column].mean()) if len(selection) else 0.0,
                "average_clv": float(selection[target_spec.target_clv_column].mean()) if len(selection) else 0.0,
                "median_clv": float(selection[target_spec.target_clv_column].median()) if len(selection) else 0.0,
                "clv_hit_rate": clv_hit_rate,
                "roi": roi,
                "strike_rate": float(selection["won_flag"].mean()) if len(selection) else 0.0,
                "average_odds": float(selection[target_spec.anchor_price_column].mean()) if len(selection) else 0.0,
                "average_market_rank": float(selection[target_spec.market_rank_column].mean()) if len(selection) else 0.0,
                "drawdown": _drawdown_from_profit(bets),
                "monthly_positive_clv_rate": _monthly_positive_rate(selection, target_spec.target_clv_column),
                "weekly_positive_clv_rate": _weekly_positive_rate(selection, target_spec.target_clv_column),
                "remove_best_winner_roi": _remove_top_winner_roi(bets, 1),
                "remove_top2_winners_roi": _remove_top_winner_roi(bets, 2),
                "robustness_score": robustness_score,
                "survives_robustness": survives,
            }
        )
    return pd.DataFrame(rows).sort_values(["robustness_score", "average_clv"], ascending=[False, False]).reset_index(drop=True)


def _summary_markdown(
    target_spec: ClvTargetSpec,
    results: pd.DataFrame,
    monotonicity: pd.DataFrame,
    zone_quality: pd.DataFrame,
    candidate_forensics: pd.DataFrame,
    feature_uniqueness: pd.DataFrame,
    dropped_columns: list[str],
    excluded_columns: list[str],
) -> str:
    best = results.iloc[0] if not results.empty else pd.Series(dtype=object)
    best_zone = zone_quality.sort_values(["survives_zone_quality", "auc", "average_clv"], ascending=[False, False, False]).iloc[0] if not zone_quality.empty else pd.Series(dtype=object)
    candidate = candidate_forensics.iloc[0] if not candidate_forensics.empty else pd.Series(dtype=object)
    non_market_delta = feature_uniqueness[
        feature_uniqueness["feature_set"].eq("all_features_minus_market")
    ]["feature_set_delta_vs_market_only"].max() if not feature_uniqueness.empty else 0.0
    lines = [
        "# CLV Market Agreement Deep Summary",
        "",
        f"- Target: `{target_spec.name}`",
        "",
        "## Robust Conclusion",
        f"1. Can we predict 60-second-to-close market movement better than baseline? {'Yes' if bool(best.get('beats_market_baseline_auc', False)) else 'Not clearly'}",
        f"2. Are higher predicted-shortening buckets actually better? {'Somewhat' if not monotonicity.empty and float(monotonicity['overall_monotonicity_score'].mean()) > 0.5 else 'Weakly'}",
        f"3. Is there monotonic CLV improvement? {'Some evidence' if not monotonicity.empty and float(monotonicity['average_clv'].max()) > float(monotonicity['average_clv'].min()) else 'Not enough'}",
        "",
        "## Candidate Verdict",
        f"4. Is the random_forest odds_3_to_10 signal real or likely noise? {'Worth further research' if bool(candidate.get('worth_further_research', False)) else 'Still vulnerable to noise/outliers'}",
        f"5. Does ROI survive removing outliers? {'No' if float(candidate.get('remove_top2_winners_roi', 0.0)) <= 0 else 'Partly'}",
        f"6. Does CLV survive removing outliers? {'Yes' if float(candidate.get('remove_top5_clv_outliers_average_clv', 0.0)) > 0 else 'No'}",
        "",
        "## Market Zone Read",
        f"7. Are signals concentrated in realistic market zones? {'Yes' if not zone_quality.empty and bool(best_zone.get('survives_zone_quality', False)) else 'Partially'}",
        f"8. Do non-market features add anything beyond market price/rank? {'Yes, weakly' if float(non_market_delta) > 0.005 else 'Very little'}",
        f"9. Is the model learning unique information or re-encoding market information? {'Mostly re-encoding market with a small incremental edge' if float(non_market_delta) <= 0.01 else 'Some incremental non-market information'}",
        "",
        "## Recommendation",
        f"10. Is this research direction worth continuing? {'Yes, cautiously' if bool(best.get('beats_market_baseline_auc', False)) else 'Only if feature quality improves materially'}",
        "11. Next highest-value feature engineering direction: pre-race market microstructure, liquidity/queue context, and stronger non-market context features in realistic front-market zones.",
        "",
        "## Labels",
        f"- weak signal: best model `{best.get('model_name', '')}` auc=`{float(best.get('auc', 0.0)):.4f}` but survives_robustness=`{bool(best.get('survives_robustness', False))}`",
        f"- likely false signal: candidate likely_false_positive=`{bool(candidate.get('likely_false_positive', False))}`",
        f"- needs more data: zone `{best_zone.get('zone_name', '')}` selections depend on monthly stability not just CLV",
        f"- worth further research: `{bool(candidate.get('worth_further_research', False))}`",
        f"- discard: any tiny high-CLV longshot pocket with negative ROI and small sample",
        "",
        "## Data Hygiene",
        f"- Excluded leakage-risk columns: {', '.join(excluded_columns) if excluded_columns else 'none detected'}",
        f"- Dropped all-NaN feature columns: {', '.join(dropped_columns) if dropped_columns else 'none'}",
        "",
        "## Final Principle",
        "- Truth over optimism: weak market-agreement prediction is still useful research, but it is not a betting edge until it proves stable, monotonic, and credible in liquid market zones.",
    ]
    return "\n".join(lines) + "\n"


def run_clv_market_agreement_research(
    matched_path: Path,
    *,
    target: str,
    test_size: float,
    min_selections: int,
    save_artifacts: bool,
    run_deep_dive: bool,
) -> dict[str, pd.DataFrame | str]:
    ensure_research_dirs()
    print(f"Loading and preparing CLV market-agreement frame for target={target}...")
    frame, target_spec, feature_columns = _prepare_clv_frame(matched_path, target)

    leakage_risk_columns = {
        "open_to_close": {"closing_price", "starting_price", "open_to_close_change", "60_to_close_change", "close_market_prob_norm", "close_market_rank", "close_market_signal", "close_odds_signal"},
        "sixty_to_close": {"closing_price", "starting_price", "60_to_close_change", "close_market_prob_norm", "close_market_rank", "close_market_signal", "close_odds_signal", "price_5m", "price_10m", "price_30m"},
    }
    excluded_columns = sorted(column for column in frame.columns if column in leakage_risk_columns[target])

    train_frame, test_frame = _split_train_test(frame, test_size)
    walk_forward_folds = _build_walk_forward_folds(frame, 6)
    lomo_folds = _build_leave_one_month_out_folds(frame, 6)
    baseline_shorten_rate = float(test_frame[target_spec.target_flag_column].mean()) if len(test_frame) else 0.0

    results_rows: list[dict[str, Any]] = []
    segments_rows: list[pd.DataFrame] = []
    execution_frames: list[pd.DataFrame] = []
    calibration_rows: list[pd.DataFrame] = []
    monotonicity_rows: list[pd.DataFrame] = []
    zone_rows: list[pd.DataFrame] = []
    importance_rows: list[pd.DataFrame] = []
    dropped_columns_all: set[str] = set()
    model_frames: dict[str, pd.DataFrame] = {}

    regression_report = pd.DataFrame()
    regression_predictions = pd.DataFrame(index=test_frame.index)
    if run_deep_dive:
        print("Fitting CLV magnitude regression models...")
        regression_report, regression_predictions = _fit_regression_models(train_frame, test_frame, feature_columns, target_spec)

    market_baseline_auc = _safe_auc(test_frame[target_spec.target_flag_column], 1.0 / pd.to_numeric(test_frame[target_spec.market_rank_column], errors="coerce").clip(lower=1.0))

    for spec in _model_specs():
        print(f"Scoring model: {spec.name}")
        holdout_predictions, trained_model, encoded_features, dropped_columns = _predict_classifier(
            spec,
            target_spec,
            train_frame,
            test_frame,
            feature_columns,
        )
        holdout_predictions = _prepare_bets(holdout_predictions)
        if not regression_predictions.empty:
            for column in regression_predictions.columns:
                holdout_predictions[column] = regression_predictions[column]
        holdout_predictions["model_name"] = spec.name
        model_frames[spec.name] = holdout_predictions
        dropped_columns_all.update(dropped_columns)

        walk_scores = []
        lomo_scores = []
        for _, fold_train, fold_test in walk_forward_folds:
            fold_predictions, _, _, _ = _predict_classifier(spec, target_spec, fold_train, fold_test, feature_columns)
            walk_scores.append(_safe_auc(fold_predictions[target_spec.target_flag_column], fold_predictions["predicted_shorten_probability"]))
        for _, fold_train, fold_test in lomo_folds:
            fold_predictions, _, _, _ = _predict_classifier(spec, target_spec, fold_train, fold_test, feature_columns)
            lomo_scores.append(_safe_auc(fold_predictions[target_spec.target_flag_column], fold_predictions["predicted_shorten_probability"]))

        calibration = _curve_report(holdout_predictions, spec.name, "holdout_test", target_spec)
        monotonicity = _monotonicity_report(holdout_predictions, spec.name, target_spec, baseline_shorten_rate)
        zone_quality = _zone_quality_report(
            holdout_predictions,
            spec.name,
            target_spec,
            float(np.mean(walk_scores)) if walk_scores else _safe_auc(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"]),
        )
        regression_column = regression_report.iloc[0]["model_name"] + "_predicted_clv" if run_deep_dive and not regression_report.empty else None
        execution = _execution_report(holdout_predictions, target_spec, regression_column, min_selections)
        segments = pd.concat(
            [
                _basic_segment_report(holdout_predictions, spec.name, target_spec),
                _zone_quality_report(holdout_predictions, spec.name, target_spec, float(np.mean(walk_scores)) if walk_scores else 0.0)[
                    ["zone_name", "runners", "races", "actual_shorten_rate", "average_clv", "average_odds", "average_market_rank"]
                ].rename(columns={"zone_name": "segment_value"}).assign(model_name=spec.name, target=target_spec.name, segment_type="market_zone", predicted_shorten_probability=np.nan),
            ],
            ignore_index=True,
        )

        calibration_rows.append(calibration)
        monotonicity_rows.append(monotonicity)
        zone_rows.append(zone_quality)
        execution_frames.append(execution.assign(model_name=spec.name, validation_label="holdout_test", target=target_spec.name))
        segments_rows.append(segments)

        result_row = {
            "model_name": spec.name,
            "target": target_spec.name,
            "auc": _safe_auc(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"]),
            "brier_score": _safe_brier(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"]),
            "log_loss": _safe_log_loss(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"]),
            "mean_predicted_shorten_probability": float(holdout_predictions["predicted_shorten_probability"].mean()),
            "actual_shorten_rate": float(holdout_predictions[target_spec.target_flag_column].mean()),
            "average_clv": float(holdout_predictions[target_spec.target_clv_column].mean()),
            "mean_abs_calibration_gap": float(calibration["calibration_gap"].abs().mean()) if not calibration.empty else 0.0,
            "best_execution_rule": str(execution.iloc[0]["execution_rule"]) if not execution.empty else "",
            "best_execution_average_clv": float(execution.iloc[0]["average_clv"]) if not execution.empty else 0.0,
            "best_execution_roi": float(execution.iloc[0]["roi"]) if not execution.empty else 0.0,
            "best_execution_selections": int(execution.iloc[0]["selections"]) if not execution.empty else 0,
            "best_execution_robustness": float(execution.iloc[0]["robustness_score"]) if not execution.empty else 0.0,
            "walk_forward_auc_mean": float(np.mean(walk_scores)) if walk_scores else 0.0,
            "walk_forward_auc_min": float(np.min(walk_scores)) if walk_scores else 0.0,
            "leave_one_month_auc_mean": float(np.mean(lomo_scores)) if lomo_scores else 0.0,
            "leave_one_month_auc_min": float(np.min(lomo_scores)) if lomo_scores else 0.0,
            "beats_market_baseline_auc": bool(_safe_auc(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"]) > market_baseline_auc),
            "survives_robustness": bool(execution.iloc[0]["survives_robustness"]) if not execution.empty else False,
        }
        results_rows.append(result_row)

        if trained_model is not None and encoded_features:
            estimator = trained_model
            if isinstance(trained_model, CalibratedClassifierCV) and getattr(trained_model, "calibrated_classifiers_", None):
                estimator = trained_model.calibrated_classifiers_[0].estimator
            if isinstance(estimator, Pipeline):
                base_estimator = estimator.named_steps["model"]
                if hasattr(base_estimator, "coef_"):
                    values = np.abs(np.ravel(base_estimator.coef_))
                elif hasattr(base_estimator, "feature_importances_"):
                    values = np.ravel(base_estimator.feature_importances_)
                else:
                    values = np.array([])
                if len(values):
                    importance_rows.append(
                        pd.DataFrame(
                            {
                                "model_name": spec.name,
                                "target": target_spec.name,
                                "feature_name": encoded_features[: len(values)],
                                "importance": values[: len(encoded_features)],
                            }
                        ).sort_values("importance", ascending=False)
                    )

    results = pd.DataFrame(results_rows).sort_values(["survives_robustness", "best_execution_robustness", "auc"], ascending=[False, False, False]).reset_index(drop=True)
    segments = pd.concat(segments_rows, ignore_index=True) if segments_rows else pd.DataFrame()
    execution_tests = pd.concat(execution_frames, ignore_index=True) if execution_frames else pd.DataFrame()
    calibration = pd.concat(calibration_rows, ignore_index=True) if calibration_rows else pd.DataFrame()
    monotonicity = pd.concat(monotonicity_rows, ignore_index=True) if monotonicity_rows else pd.DataFrame()
    zone_quality = pd.concat(zone_rows, ignore_index=True) if zone_rows else pd.DataFrame()
    feature_importance = pd.concat(importance_rows, ignore_index=True) if importance_rows else pd.DataFrame()
    feature_uniqueness = _feature_uniqueness_report(train_frame, test_frame, target_spec, feature_columns) if run_deep_dive else pd.DataFrame()
    candidate_forensics = _candidate_signal_forensics(model_frames.get("random_forest", pd.DataFrame()), target_spec) if run_deep_dive and "random_forest" in model_frames else pd.DataFrame()
    bucket_quality = monotonicity.copy()
    summary = _summary_markdown(target_spec, results, monotonicity, zone_quality, candidate_forensics, feature_uniqueness, sorted(dropped_columns_all), excluded_columns)

    if save_artifacts:
        _write_report(results, RESULTS_PATH, target_spec.name)
        _write_report(segments, SEGMENTS_PATH, target_spec.name)
        _write_report(execution_tests, EXECUTION_PATH, target_spec.name)
        _write_report(calibration, CALIBRATION_PATH, target_spec.name)
        _write_report(feature_importance, FEATURE_IMPORTANCE_PATH, target_spec.name)
        _write_report(monotonicity, MONOTONICITY_PATH, target_spec.name)
        _write_report(bucket_quality, BUCKET_QUALITY_PATH, target_spec.name)
        _write_report(zone_quality, ZONE_QUALITY_PATH, target_spec.name)
        _write_report(regression_report, REGRESSION_PATH, target_spec.name)
        _write_report(candidate_forensics, CANDIDATE_FORENSICS_PATH, target_spec.name)
        _write_report(feature_uniqueness, FEATURE_UNIQUENESS_PATH, target_spec.name)
        _write_text(summary, SUMMARY_PATH, target_spec.name)
        _write_text(summary, DEEP_SUMMARY_PATH, target_spec.name)
        if not results.empty:
            _write_artifact(results.iloc[0].to_dict(), BEST_MODEL_PATH, target_spec.name)

    print()
    print("CLV Market Agreement Research Summary")
    if not results.empty:
        best = results.iloc[0]
        print(
            f"Best model: {best['model_name']} | target={best['target']} auc={best['auc']:.4f} "
            f"best_execution_rule={best['best_execution_rule']} "
            f"best_execution_average_clv={best['best_execution_average_clv']:.4f} "
            f"survives_robustness={bool(best['survives_robustness'])}"
        )
    return {
        "results": results,
        "segments": segments,
        "execution_tests": execution_tests,
        "calibration": calibration,
        "feature_importance": feature_importance,
        "monotonicity": monotonicity,
        "bucket_quality": bucket_quality,
        "zone_quality": zone_quality,
        "regression": regression_report,
        "candidate_forensics": candidate_forensics,
        "feature_uniqueness": feature_uniqueness,
        "summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CLV-first market-agreement research module.")
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    parser.add_argument("--target", choices=["open_to_close", "sixty_to_close"], default="open_to_close")
    parser.add_argument("--test-size", type=float, default=0.25)
    parser.add_argument("--min-selections", type=int, default=500)
    parser.add_argument("--save-artifacts", action="store_true")
    parser.add_argument("--run-deep-dive", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_clv_market_agreement_research(
        args.matched_path,
        target=args.target,
        test_size=args.test_size,
        min_selections=args.min_selections,
        save_artifacts=args.save_artifacts,
        run_deep_dive=args.run_deep_dive,
    )


if __name__ == "__main__":
    main()
