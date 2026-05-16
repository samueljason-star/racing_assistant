from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from app.betting.market_helpers import closing_line_metrics, commission_adjusted_market_probability, raw_market_probability
from app.research.form_score_optimizer import apply_form_formula, prepare_form_features
from app.research.utils import (
    RESEARCH_ARTIFACTS_DIR,
    RESEARCH_DATA_DIR,
    RESEARCH_REPORTS_DIR,
    attach_common_labels,
    compute_max_drawdown,
    ensure_research_dirs,
    json_dump,
    save_dataframe,
)

try:
    from lightgbm import LGBMClassifier
except ImportError:
    LGBMClassifier = None

try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"

RESULTS_PATH = RESEARCH_REPORTS_DIR / "market_residual_model_results.csv"
SEGMENTS_PATH = RESEARCH_REPORTS_DIR / "market_residual_segments.csv"
EXECUTION_PATH = RESEARCH_REPORTS_DIR / "market_residual_execution_tests.csv"
CALIBRATION_PATH = RESEARCH_REPORTS_DIR / "market_residual_calibration.csv"
BEST_MODEL_PATH = RESEARCH_ARTIFACTS_DIR / "best_market_residual_model.json"

RACE_KEYS = ["race_date", "track_norm", "race_number"]
COMMISSION_RATE = 0.08


@dataclass(frozen=True)
class ModelSpec:
    name: str
    estimator_type: str
    calibrated: bool


def _safe_numeric(series: pd.Series | None, fill: float = 0.0) -> pd.Series:
    if series is None:
        output = pd.Series(dtype=float)
    else:
        output = pd.to_numeric(series, errors="coerce")
    return output.fillna(fill)


def _first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace({0.0: np.nan})


def _safe_auc(y_true: pd.Series, y_prob: pd.Series) -> float:
    truth = _safe_numeric(y_true)
    probs = _safe_numeric(y_prob)
    if truth.nunique(dropna=True) < 2:
        return 0.5
    try:
        return float(roc_auc_score(truth, probs))
    except ValueError:
        return 0.5


def _safe_log_loss(y_true: pd.Series, y_prob: pd.Series) -> float:
    truth = _safe_numeric(y_true)
    probs = _safe_numeric(y_prob).clip(1e-6, 1 - 1e-6)
    if truth.empty:
        return 0.0
    try:
        return float(log_loss(truth, probs, labels=[0, 1]))
    except ValueError:
        return 0.0


def _safe_brier(y_true: pd.Series, y_prob: pd.Series) -> float:
    truth = _safe_numeric(y_true)
    probs = _safe_numeric(y_prob).clip(0.0, 1.0)
    if truth.empty:
        return 0.0
    return float(brier_score_loss(truth, probs))


def _zscore(series: pd.Series, invert: bool = False) -> pd.Series:
    numeric = _safe_numeric(series, fill=np.nan)
    mean = numeric.mean()
    std = numeric.std(ddof=0)
    if pd.isna(std) or std == 0:
        values = pd.Series(0.0, index=series.index)
    else:
        values = (numeric - mean) / std
    values = values.clip(-4, 4).fillna(0.0)
    return -values if invert else values


def _bucket_field_size(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "unknown"
    value = int(value)
    if value <= 7:
        return "small"
    if value <= 11:
        return "medium"
    return "large"


def _bucket_odds(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "unknown"
    if value < 4:
        return "short"
    if value <= 10:
        return "mid"
    return "long"


def _compute_market_probabilities(frame: pd.DataFrame, price_column: str) -> pd.DataFrame:
    working = frame.copy()
    prices = _safe_numeric(working[price_column], fill=np.nan)
    working["market_prob_raw"] = prices.map(raw_market_probability)
    race_totals = working.groupby(RACE_KEYS, dropna=False)["market_prob_raw"].transform("sum")
    working["market_prob_norm"] = _safe_divide(working["market_prob_raw"], race_totals).fillna(0.0)
    working["market_prob_commission_adj"] = prices.map(
        lambda value: commission_adjusted_market_probability(value, COMMISSION_RATE)
    ).fillna(0.0)
    working["market_rank_current"] = prices.groupby(
        [working[key] for key in RACE_KEYS], dropna=False
    ).rank(method="dense", ascending=True)
    return working


def _derive_context_score(frame: pd.DataFrame) -> pd.Series:
    field_component = _zscore(frame["field_size"], invert=True)
    market_component = _zscore(frame["market_rank_current"], invert=True)
    return (field_component + market_component) / 2.0


def _prepare_research_frame(matched_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(matched_path, low_memory=False)
    if frame.empty:
        raise RuntimeError("Matched dataset is empty.")

    working = attach_common_labels(frame)
    working = prepare_form_features(working)
    working = apply_form_formula(working, {
        "finish_weight": 2.0,
        "margin_weight": 1.0,
        "distance_weight": 0.5,
        "class_weight": 0.25,
        "barrier_weight": 0.5,
        "trainer_weight": 0.0,
        "jockey_weight": 0.0,
    })
    working["won_flag"] = (_safe_numeric(working.get("finish_position"), fill=0) == 1).astype(int)
    working["race_day"] = pd.to_datetime(working["race_date"], errors="coerce")
    working = working[working["race_day"].notna()].copy()
    working["race_month"] = working["race_day"].dt.to_period("M").astype(str)
    working["race_week"] = working["race_day"].dt.to_period("W").astype(str)

    current_price_column = _first_existing_column(
        working,
        ["price_1m", "price_3m", "price_5m", "price_10m", "closing_price", "starting_price"],
    )
    if current_price_column is None:
        raise RuntimeError("No usable market odds column found.")
    working["current_price"] = _safe_numeric(working.get(current_price_column), fill=np.nan)
    working["opening_price"] = _safe_numeric(
        working.get(_first_existing_column(working, ["opening_price", "price_open", "open_price"])),
        fill=np.nan,
    )
    working["price_60m"] = _safe_numeric(working.get("price_60m"), fill=np.nan)
    working["price_30m"] = _safe_numeric(working.get("price_30m"), fill=np.nan)
    working["price_10m"] = _safe_numeric(working.get("price_10m"), fill=np.nan)
    working["price_5m"] = _safe_numeric(working.get("price_5m"), fill=np.nan)
    working["price_3m"] = _safe_numeric(working.get("price_3m"), fill=np.nan)

    working = _compute_market_probabilities(working, "current_price")
    working["field_size"] = working.groupby(RACE_KEYS, dropna=False)["horse_name"].transform("size")
    working["field_size_bucket"] = working["field_size"].map(_bucket_field_size)
    working["odds_regime"] = working["current_price"].map(_bucket_odds)
    working["movement_score"] = (
        _safe_numeric(working.get("movement_score"), fill=np.nan)
        .fillna((_safe_numeric(working["opening_price"], fill=np.nan) - working["current_price"]).fillna(0.0))
    )
    working["edge_score"] = _safe_numeric(
        working.get(_first_existing_column(working, ["model_edge", "edge", "edge_score"])),
        fill=0.0,
    )
    working["market_signal"] = _zscore(working["market_rank_current"], invert=True)
    working["odds_signal"] = _zscore(working["current_price"], invert=True)
    working["form_signal"] = _zscore(working["form_score"].fillna(0.0))
    working["context_score"] = _derive_context_score(working)
    working["residual_target"] = working["won_flag"] - working["market_prob_norm"]
    working["region"] = working.get("region", pd.Series("unknown", index=working.index)).fillna("unknown")
    working["race_class_group"] = working.get("race_class_group", pd.Series("unknown", index=working.index)).fillna("unknown")

    # Explicitly label this as late-market research because current_price may use near-jump prices.
    working["research_profile"] = "late_market_residual"
    return working.sort_values(RACE_KEYS).reset_index(drop=True)


def _candidate_feature_columns(frame: pd.DataFrame) -> list[str]:
    numeric_candidates = [
        "form_score",
        "form_signal",
        "movement_score",
        "edge_score",
        "context_score",
        "market_rank_current",
        "market_signal",
        "current_price",
        "odds_signal",
        "market_prob_norm",
        "field_size",
        "barrier",
        "last_start_finish",
        "best_last_3_finish",
        "average_last_3_finish",
        "average_margin_last_3",
        "last_start_margin",
        "days_since_last_start",
        "trainer_stat",
        "jockey_stat",
        "distance",
        "distance_change",
        "class_change",
        "track_condition_match",
        "price_60m",
        "price_30m",
        "price_10m",
        "price_5m",
        "price_3m",
        "opening_price",
    ]
    available = [column for column in numeric_candidates if column in frame.columns and frame[column].notna().any()]
    categorical_candidates = [
        "field_size_bucket",
        "odds_regime",
        "region",
        "race_class_group",
        "track_condition",
        "track_norm",
    ]
    available.extend([column for column in categorical_candidates if column in frame.columns and frame[column].notna().any()])
    return available


def _add_interaction_columns(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    working["interaction_form_market_rank"] = working["form_signal"] * working["market_signal"]
    working["interaction_movement_market_rank"] = working["movement_score"] * working["market_signal"]
    working["interaction_form_odds"] = working["form_signal"] * working["odds_signal"]
    working["interaction_edge_movement"] = working["edge_score"] * working["movement_score"]
    working["interaction_field_size_market_rank"] = working["field_size"] * working["market_signal"]
    working["interaction_odds_movement"] = working["odds_signal"] * working["movement_score"]
    return working


def _build_design_matrices(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_features = train_frame[feature_columns].copy()
    test_features = test_frame[feature_columns].copy()
    combined = pd.concat([train_features, test_features], axis=0, ignore_index=True)
    combined = pd.get_dummies(combined, dummy_na=True)
    x_train = combined.iloc[: len(train_features)].reset_index(drop=True)
    x_test = combined.iloc[len(train_features) :].reset_index(drop=True)
    return x_train, x_test


def _split_train_test(frame: pd.DataFrame, test_size: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    unique_days = frame["race_day"].dropna().sort_values().unique()
    cutoff_index = max(1, int(len(unique_days) * (1 - test_size))) - 1
    cutoff_day = unique_days[cutoff_index]
    train_frame = frame[frame["race_day"] <= cutoff_day].copy()
    test_frame = frame[frame["race_day"] > cutoff_day].copy()
    if test_frame.empty:
        test_frame = train_frame.copy()
    return train_frame, test_frame


def _build_walk_forward_folds(frame: pd.DataFrame, limit: int) -> list[tuple[str, pd.DataFrame, pd.DataFrame]]:
    months = sorted(frame["race_month"].dropna().unique())
    folds: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []
    for index, month in enumerate(months[1:], start=1):
        train_months = months[:index]
        train_slice = frame[frame["race_month"].isin(train_months)].copy()
        test_slice = frame[frame["race_month"] == month].copy()
        if train_slice.empty or test_slice.empty:
            continue
        folds.append((f"wf_{month}", train_slice, test_slice))
    return folds[-limit:] if limit > 0 else folds


def _build_leave_one_month_out_folds(frame: pd.DataFrame, limit: int) -> list[tuple[str, pd.DataFrame, pd.DataFrame]]:
    months = sorted(frame["race_month"].dropna().unique())
    folds: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []
    for month in months:
        train_slice = frame[frame["race_month"] != month].copy()
        test_slice = frame[frame["race_month"] == month].copy()
        if train_slice.empty or test_slice.empty:
            continue
        folds.append((f"lomo_{month}", train_slice, test_slice))
    return folds[-limit:] if limit > 0 else folds


def _fit_estimator(spec: ModelSpec, x_train: pd.DataFrame, y_train: pd.Series) -> Any:
    if spec.name == "market_baseline":
        return None
    if spec.estimator_type == "baseline":
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


def _predict_frame(
    spec: ModelSpec,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    feature_columns: list[str],
) -> pd.DataFrame:
    working = test_frame.copy()
    if spec.name == "market_baseline":
        working["predicted_win_probability"] = working["market_prob_norm"]
    elif spec.estimator_type == "baseline":
        raw_score = (
            working["form_signal"] * 0.45
            + working["edge_score"] * 0.2
            + working["movement_score"] * 0.15
            + working["market_signal"] * 0.2
        )
        working["predicted_win_probability"] = (
            np.exp(raw_score.clip(-6, 6))
            / np.exp(raw_score.clip(-6, 6)).groupby([working[key] for key in RACE_KEYS], dropna=False).transform("sum")
        ).fillna(0.0)
    else:
        x_train, x_test = _build_design_matrices(train_frame, test_frame, feature_columns)
        model = _fit_estimator(spec, x_train, train_frame["won_flag"])
        if model is None:
            working["predicted_win_probability"] = working["market_prob_norm"]
        else:
            probs = model.predict_proba(x_test)[:, 1]
            working["predicted_win_probability"] = pd.Series(probs, index=working.index).clip(1e-6, 1 - 1e-6)
    working["residual_score"] = working["predicted_win_probability"] - working["market_prob_norm"]
    working["model_rank"] = working.groupby(RACE_KEYS, dropna=False)["residual_score"].rank(method="dense", ascending=False)
    working["probability_rank"] = working.groupby(RACE_KEYS, dropna=False)["predicted_win_probability"].rank(method="dense", ascending=False)
    working["rank_overlay"] = working["market_rank_current"] - working["model_rank"]
    return working


def _calibration_table(frame: pd.DataFrame, model_name: str, validation_label: str) -> pd.DataFrame:
    working = frame.copy()
    ranked = working["predicted_win_probability"].rank(method="first")
    bucket_count = min(10, max(2, len(working) // 150))
    working["probability_bucket"] = pd.qcut(ranked, q=bucket_count, duplicates="drop")
    grouped = working.groupby("probability_bucket", dropna=False, observed=False)
    table = grouped.agg(
        runners=("won_flag", "size"),
        predicted_probability=("predicted_win_probability", "mean"),
        actual_win_rate=("won_flag", "mean"),
        mean_residual=("residual_score", "mean"),
    ).reset_index()
    table["calibration_gap"] = table["predicted_probability"] - table["actual_win_rate"]
    table["auc"] = _safe_auc(working["won_flag"], working["predicted_win_probability"])
    table["brier_score"] = _safe_brier(working["won_flag"], working["predicted_win_probability"])
    table["log_loss"] = _safe_log_loss(working["won_flag"], working["predicted_win_probability"])
    table["model_name"] = model_name
    table["validation_label"] = validation_label
    table["probability_bucket"] = table["probability_bucket"].astype(str)
    return table


def _top_residual_bucket_stats(frame: pd.DataFrame) -> dict[str, float]:
    working = frame.copy()
    ranked = working["residual_score"].rank(method="first")
    bucket_count = min(10, max(2, len(working) // 150))
    working["residual_bucket"] = pd.qcut(ranked, q=bucket_count, duplicates="drop")
    top_bucket = working["residual_bucket"].astype(str).max()
    top = working[working["residual_bucket"].astype(str) == top_bucket].copy()
    if top.empty:
        return {"top_residual_bucket_roi": 0.0, "top_residual_bucket_bets": 0}
    bets = _prepare_bets(top)
    return {
        "top_residual_bucket_roi": float(bets["profit_loss"].sum() / bets["stake"].sum()) if bets["stake"].sum() else 0.0,
        "top_residual_bucket_bets": int(len(bets)),
    }


def _prepare_bets(selection: pd.DataFrame) -> pd.DataFrame:
    if selection.empty:
        return selection.copy()
    working = selection.copy()
    working["stake"] = 100.0
    working["profit_loss"] = np.where(
        working["won_flag"] == 1,
        (working["current_price"] - 1.0) * (1.0 - COMMISSION_RATE) * working["stake"],
        -working["stake"],
    )
    return working


def _weekly_positive_rate(bets: pd.DataFrame) -> tuple[float, float]:
    if bets.empty:
        return 0.0, 0.0
    weekly = bets.groupby("race_week", dropna=False).agg(profit_loss=("profit_loss", "sum"), stake=("stake", "sum"))
    weekly["roi"] = _safe_divide(weekly["profit_loss"], weekly["stake"]).fillna(0.0)
    return float((weekly["roi"] > 0).mean()), float(weekly["roi"].std(ddof=0)) if len(weekly) else 0.0


def _monthly_positive_rate(bets: pd.DataFrame) -> tuple[float, float]:
    if bets.empty:
        return 0.0, 0.0
    monthly = bets.groupby("race_month", dropna=False).agg(profit_loss=("profit_loss", "sum"), stake=("stake", "sum"))
    monthly["roi"] = _safe_divide(monthly["profit_loss"], monthly["stake"]).fillna(0.0)
    return float((monthly["roi"] > 0).mean()), float(monthly["roi"].std(ddof=0)) if len(monthly) else 0.0


def _remove_top_winner_roi(bets: pd.DataFrame, winners_to_remove: int) -> float:
    if bets.empty:
        return 0.0
    winners = bets[bets["profit_loss"] > 0].sort_values("profit_loss", ascending=False)
    if winners.empty:
        return float(bets["profit_loss"].sum() / bets["stake"].sum()) if bets["stake"].sum() else 0.0
    remaining = bets.drop(index=winners.head(winners_to_remove).index)
    if remaining["stake"].sum() <= 0:
        return 0.0
    return float(remaining["profit_loss"].sum() / remaining["stake"].sum())


def _clv_proxy_stats(selection: pd.DataFrame) -> dict[str, float]:
    if selection.empty:
        return {
            "average_clv_percent": 0.0,
            "shorten_rate_open": 0.0,
            "shorten_rate_60": 0.0,
        }
    metrics = [
        closing_line_metrics(opening if pd.notna(opening) else current, current)
        for opening, current in zip(selection["opening_price"], selection["current_price"])
    ]
    clv_percent = [metric["clv_percent"] for metric in metrics if metric["clv_percent"] is not None]
    shorten_open = float(((_safe_numeric(selection["opening_price"], fill=np.nan) - selection["current_price"]) > 0).mean())
    shorten_60 = float(((_safe_numeric(selection["price_60m"], fill=np.nan) - selection["current_price"]) > 0).mean())
    return {
        "average_clv_percent": float(np.mean(clv_percent)) if clv_percent else 0.0,
        "shorten_rate_open": shorten_open,
        "shorten_rate_60": shorten_60,
    }


def _execution_rules(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    def top_residual(source: pd.DataFrame) -> pd.DataFrame:
        if source.empty:
            return source.copy()
        ordered = source.sort_values([*RACE_KEYS, "residual_score", "current_price"], ascending=[True, True, True, False, True])
        return ordered.groupby(RACE_KEYS, dropna=False).head(1).copy()

    positive = frame[frame["residual_score"] > 0].copy()
    return {
        "top_1_positive_residual_per_race": top_residual(positive),
        "positive_residual_inside_market_rank_1_to_5": top_residual(
            positive[positive["market_rank_current"].between(1, 5, inclusive="both")]
        ),
        "positive_residual_inside_odds_2_to_8": top_residual(
            positive[positive["current_price"].between(2.0, 8.0, inclusive="both")]
        ),
        "positive_residual_inside_odds_3_to_10": top_residual(
            positive[positive["current_price"].between(3.0, 10.0, inclusive="both")]
        ),
        "positive_residual_in_medium_fields_only": top_residual(positive[positive["field_size_bucket"] == "medium"]),
        "positive_residual_in_small_fields_only": top_residual(positive[positive["field_size_bucket"] == "small"]),
        "residual_score_threshold_ge_0.01": top_residual(frame[frame["residual_score"] >= 0.01]),
        "residual_score_threshold_ge_0.03": top_residual(frame[frame["residual_score"] >= 0.03]),
        "residual_score_threshold_ge_0.05": top_residual(frame[frame["residual_score"] >= 0.05]),
        "positive_residual_with_positive_movement_support": top_residual(
            positive[positive["movement_score"] > 0]
        ),
        "positive_residual_with_form_and_movement_support": top_residual(
            positive[(positive["movement_score"] > 0) & (positive["form_score"] >= positive["form_score"].median())]
        ),
    }


def _track_concentration(bets: pd.DataFrame) -> float:
    if bets.empty:
        return 0.0
    return float(bets["track_norm"].value_counts(normalize=True, dropna=False).max())


def _month_concentration(bets: pd.DataFrame) -> float:
    if bets.empty:
        return 0.0
    return float(bets["race_month"].value_counts(normalize=True, dropna=False).max())


def _summarise_execution(
    frame: pd.DataFrame,
    model_name: str,
    validation_label: str,
    min_bets: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rule_name, selection in _execution_rules(frame).items():
        bets = _prepare_bets(selection)
        total_staked = float(bets["stake"].sum()) if not bets.empty else 0.0
        roi = float(bets["profit_loss"].sum() / total_staked) if total_staked else 0.0
        weekly_positive_rate, weekly_roi_std = _weekly_positive_rate(bets)
        monthly_positive_rate, monthly_roi_std = _monthly_positive_rate(bets)
        clv_stats = _clv_proxy_stats(selection)
        remove_best = _remove_top_winner_roi(bets, 1)
        remove_top2 = _remove_top_winner_roi(bets, 2)
        track_concentration = _track_concentration(bets)
        month_concentration = _month_concentration(bets)
        robustness_score = (
            roi * 0.25
            + weekly_positive_rate * 0.1
            + monthly_positive_rate * 0.15
            + clv_stats["shorten_rate_open"] * 0.1
            + clv_stats["shorten_rate_60"] * 0.05
            + remove_best * 0.15
            + remove_top2 * 0.1
            - monthly_roi_std * 0.08
            - weekly_roi_std * 0.05
            - track_concentration * 0.06
            - month_concentration * 0.06
        )
        survives = bool(
            len(bets) >= min_bets
            and roi > -0.02
            and remove_top2 > -0.05
            and weekly_positive_rate >= 0.4
            and monthly_positive_rate >= 0.4
            and track_concentration <= 0.45
            and month_concentration <= 0.4
            and clv_stats["shorten_rate_open"] >= 0.45
        )
        rows.append(
            {
                "model_name": model_name,
                "validation_label": validation_label,
                "execution_rule": rule_name,
                "bets": int(len(bets)),
                "wins": int(bets["won_flag"].sum()) if not bets.empty else 0,
                "strike_rate": float(bets["won_flag"].mean()) if not bets.empty else 0.0,
                "roi": roi,
                "profit_loss": float(bets["profit_loss"].sum()) if not bets.empty else 0.0,
                "average_odds": float(selection["current_price"].mean()) if not selection.empty else 0.0,
                "average_market_rank": float(selection["market_rank_current"].mean()) if not selection.empty else 0.0,
                "average_residual_score": float(selection["residual_score"].mean()) if not selection.empty else 0.0,
                "drawdown": compute_max_drawdown([1000.0] + (1000.0 + bets["profit_loss"].cumsum()).tolist()) if not bets.empty else 0.0,
                "weekly_positive_rate": weekly_positive_rate,
                "monthly_positive_rate": monthly_positive_rate,
                "track_concentration": track_concentration,
                "month_concentration": month_concentration,
                "remove_best_winner_roi": remove_best,
                "remove_top2_winners_roi": remove_top2,
                "average_clv_percent": clv_stats["average_clv_percent"],
                "shorten_rate_open": clv_stats["shorten_rate_open"],
                "shorten_rate_60": clv_stats["shorten_rate_60"],
                "robustness_score": robustness_score,
                "survives_robustness": survives,
            }
        )
    return pd.DataFrame(rows)


def _segment_report(frame: pd.DataFrame, model_name: str, validation_label: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for column in ["field_size_bucket", "odds_regime", "region", "race_class_group"]:
        if column not in frame.columns:
            continue
        for value, subset in frame.groupby(column, dropna=False):
            rows.append(
                {
                    "model_name": model_name,
                    "validation_label": validation_label,
                    "segment_type": column,
                    "segment_value": value,
                    "runners": int(len(subset)),
                    "races": int(subset[RACE_KEYS].drop_duplicates().shape[0]),
                    "mean_residual_score": float(subset["residual_score"].mean()),
                    "mean_predicted_probability": float(subset["predicted_win_probability"].mean()),
                    "mean_market_probability": float(subset["market_prob_norm"].mean()),
                    "win_rate": float(subset["won_flag"].mean()),
                    "average_market_rank": float(subset["market_rank_current"].mean()),
                }
            )
    return pd.DataFrame(rows)


def _model_summary_row(
    model_name: str,
    holdout_frame: pd.DataFrame,
    calibration_table: pd.DataFrame,
    execution_rows: pd.DataFrame,
    walk_forward_scores: list[float],
    lomo_scores: list[float],
) -> dict[str, Any]:
    bucket_stats = _top_residual_bucket_stats(holdout_frame)
    best_execution = (
        execution_rows.sort_values(["robustness_score", "roi"], ascending=[False, False]).iloc[0]
        if not execution_rows.empty
        else pd.Series(dtype=object)
    )
    return {
        "model_name": model_name,
        "auc": _safe_auc(holdout_frame["won_flag"], holdout_frame["predicted_win_probability"]),
        "brier_score": _safe_brier(holdout_frame["won_flag"], holdout_frame["predicted_win_probability"]),
        "log_loss": _safe_log_loss(holdout_frame["won_flag"], holdout_frame["predicted_win_probability"]),
        "mean_residual_score": float(holdout_frame["residual_score"].mean()),
        "mean_market_probability": float(holdout_frame["market_prob_norm"].mean()),
        "mean_predicted_probability": float(holdout_frame["predicted_win_probability"].mean()),
        "mean_abs_calibration_gap": float(calibration_table["calibration_gap"].abs().mean()) if not calibration_table.empty else 0.0,
        "top_residual_bucket_roi": bucket_stats["top_residual_bucket_roi"],
        "top_residual_bucket_bets": bucket_stats["top_residual_bucket_bets"],
        "best_execution_rule": str(best_execution.get("execution_rule", "")),
        "best_execution_roi": float(best_execution.get("roi", 0.0)),
        "best_execution_bets": int(best_execution.get("bets", 0)),
        "best_execution_robustness": float(best_execution.get("robustness_score", 0.0)),
        "walk_forward_auc_mean": float(np.mean(walk_forward_scores)) if walk_forward_scores else 0.0,
        "walk_forward_auc_min": float(np.min(walk_forward_scores)) if walk_forward_scores else 0.0,
        "leave_one_month_auc_mean": float(np.mean(lomo_scores)) if lomo_scores else 0.0,
        "leave_one_month_auc_min": float(np.min(lomo_scores)) if lomo_scores else 0.0,
        "beats_market_baseline_auc": False,
        "survives_robustness": bool(best_execution.get("survives_robustness", False)),
    }


def run_market_residual_research(
    matched_path: Path,
    *,
    min_bets: int,
    test_size: float,
    walk_forward_limit: int,
    save_artifacts: bool,
) -> dict[str, pd.DataFrame]:
    ensure_research_dirs()
    print("Loading and preparing market-residual research frame...")
    frame = _add_interaction_columns(_prepare_research_frame(matched_path))
    feature_columns = _candidate_feature_columns(frame) + [
        "interaction_form_market_rank",
        "interaction_movement_market_rank",
        "interaction_form_odds",
        "interaction_edge_movement",
        "interaction_field_size_market_rank",
        "interaction_odds_movement",
    ]

    train_frame, test_frame = _split_train_test(frame, test_size)
    walk_forward_folds = _build_walk_forward_folds(frame, walk_forward_limit)
    lomo_folds = _build_leave_one_month_out_folds(frame, walk_forward_limit)

    model_specs = [
        ModelSpec("market_baseline", "baseline", False),
        ModelSpec("simple_residual_baseline", "baseline", False),
        ModelSpec("calibrated_logistic", "logistic", True),
        ModelSpec("random_forest", "random_forest", True),
        ModelSpec("gradient_boosting", "gradient_boosting", True),
    ]
    if LGBMClassifier is not None:
        model_specs.append(ModelSpec("lightgbm", "lightgbm", False))
    else:
        print("LightGBM not installed; skipping optional model.")
    if XGBClassifier is not None:
        model_specs.append(ModelSpec("xgboost", "xgboost", False))
    else:
        print("XGBoost not installed; skipping optional model.")

    result_rows: list[dict[str, Any]] = []
    calibration_rows: list[pd.DataFrame] = []
    segment_rows: list[pd.DataFrame] = []
    execution_rows: list[pd.DataFrame] = []

    market_baseline_auc = _safe_auc(test_frame["won_flag"], test_frame["market_prob_norm"])

    for spec in model_specs:
        print(f"Scoring model: {spec.name}")
        holdout_predictions = _predict_frame(spec, train_frame, test_frame, feature_columns)
        calibration_table = _calibration_table(holdout_predictions, spec.name, "holdout_test")
        segments = _segment_report(holdout_predictions, spec.name, "holdout_test")
        executions = _summarise_execution(holdout_predictions, spec.name, "holdout_test", min_bets)

        walk_scores: list[float] = []
        for fold_name, fold_train, fold_test in walk_forward_folds:
            fold_predictions = _predict_frame(spec, fold_train, fold_test, feature_columns)
            walk_scores.append(_safe_auc(fold_predictions["won_flag"], fold_predictions["predicted_win_probability"]))
            segment_rows.append(_segment_report(fold_predictions, spec.name, fold_name))

        lomo_scores: list[float] = []
        for fold_name, fold_train, fold_test in lomo_folds:
            fold_predictions = _predict_frame(spec, fold_train, fold_test, feature_columns)
            lomo_scores.append(_safe_auc(fold_predictions["won_flag"], fold_predictions["predicted_win_probability"]))
            segment_rows.append(_segment_report(fold_predictions, spec.name, fold_name))

        summary = _model_summary_row(spec.name, holdout_predictions, calibration_table, executions, walk_scores, lomo_scores)
        summary["beats_market_baseline_auc"] = summary["auc"] > market_baseline_auc
        result_rows.append(summary)
        calibration_rows.append(calibration_table)
        segment_rows.append(segments)
        execution_rows.append(executions)

    results = pd.DataFrame(result_rows).sort_values(
        ["survives_robustness", "best_execution_robustness", "auc"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    calibration_report = pd.concat(calibration_rows, ignore_index=True) if calibration_rows else pd.DataFrame()
    segment_report = pd.concat(segment_rows, ignore_index=True) if segment_rows else pd.DataFrame()
    execution_report = pd.concat(execution_rows, ignore_index=True) if execution_rows else pd.DataFrame()
    if not execution_report.empty:
        execution_report = execution_report.sort_values(
            ["survives_robustness", "robustness_score", "roi"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    if save_artifacts:
        save_dataframe(results, RESULTS_PATH)
        save_dataframe(segment_report, SEGMENTS_PATH)
        save_dataframe(execution_report, EXECUTION_PATH)
        save_dataframe(calibration_report, CALIBRATION_PATH)
        if not results.empty:
            json_dump(results.iloc[0].to_dict(), BEST_MODEL_PATH)

    print()
    print("Market Residual Research Summary")
    if not results.empty:
        best = results.iloc[0]
        print(
            f"Best model: {best['model_name']} | auc={best['auc']:.4f} "
            f"best_execution_rule={best['best_execution_rule']} "
            f"best_execution_roi={best['best_execution_roi']:.4f} "
            f"survives_robustness={bool(best['survives_robustness'])}"
        )
    return {
        "results": results,
        "segments": segment_report,
        "execution_tests": execution_report,
        "calibration": calibration_report,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only market residual model framework.")
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    parser.add_argument("--min-bets", type=int, default=50)
    parser.add_argument("--test-size", type=float, default=0.3)
    parser.add_argument("--walk-forward", type=int, default=6)
    parser.add_argument("--save-artifacts", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_market_residual_research(
        args.matched_path,
        min_bets=args.min_bets,
        test_size=args.test_size,
        walk_forward_limit=args.walk_forward,
        save_artifacts=args.save_artifacts,
    )


if __name__ == "__main__":
    main()
