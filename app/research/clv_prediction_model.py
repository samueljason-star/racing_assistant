from __future__ import annotations

import argparse
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

from app.betting.market_helpers import closing_line_metrics, raw_market_probability
from app.research.form_score_optimizer import apply_form_formula, prepare_form_features
from app.research.market_residual_model import (
    MATCHED_PATH,
    RACE_KEYS,
    _bucket_field_size,
    _bucket_odds,
    _build_leave_one_month_out_folds,
    _build_walk_forward_folds,
    _safe_divide,
    _safe_numeric,
    _split_train_test,
    _zscore,
)
from app.research.utils import (
    RESEARCH_ARTIFACTS_DIR,
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

RESULTS_PATH = RESEARCH_REPORTS_DIR / "clv_prediction_model_results.csv"
SEGMENTS_PATH = RESEARCH_REPORTS_DIR / "clv_prediction_segments.csv"
EXECUTION_PATH = RESEARCH_REPORTS_DIR / "clv_prediction_execution_tests.csv"
FEATURE_IMPORTANCE_PATH = RESEARCH_REPORTS_DIR / "clv_feature_importance.csv"
CURVE_PATH = RESEARCH_REPORTS_DIR / "clv_market_agreement_curve.csv"
BEST_MODEL_PATH = RESEARCH_ARTIFACTS_DIR / "best_clv_prediction_model.json"


@dataclass(frozen=True)
class ClvTargetSpec:
    name: str
    anchor_price_column: str
    target_flag_column: str
    target_clv_column: str
    market_rank_column: str
    market_prob_column: str
    market_signal_column: str
    odds_signal_column: str
    movement_score_column: str
    profile_label: str


@dataclass(frozen=True)
class ModelSpec:
    name: str
    estimator_type: str
    calibrated: bool


def _targeted_report_path(base_path: Path, target: str) -> Path:
    if not target:
        return base_path
    return base_path.with_name(f"{base_path.stem}_{target}{base_path.suffix}")


def _targeted_artifact_path(base_path: Path, target: str) -> Path:
    if not target:
        return base_path
    return base_path.with_name(f"{base_path.stem}_{target}{base_path.suffix}")


def _safe_auc(y_true: pd.Series, y_prob: pd.Series) -> float:
    truth = _safe_numeric(y_true)
    probs = _safe_numeric(y_prob)
    if truth.nunique(dropna=True) < 2:
        return 0.5
    try:
        return float(roc_auc_score(truth, probs))
    except ValueError:
        return 0.5


def _safe_brier(y_true: pd.Series, y_prob: pd.Series) -> float:
    truth = _safe_numeric(y_true)
    probs = _safe_numeric(y_prob).clip(0.0, 1.0)
    if truth.empty:
        return 0.0
    return float(brier_score_loss(truth, probs))


def _safe_log_loss(y_true: pd.Series, y_prob: pd.Series) -> float:
    truth = _safe_numeric(y_true)
    probs = _safe_numeric(y_prob).clip(1e-6, 1 - 1e-6)
    if truth.empty:
        return 0.0
    try:
        return float(log_loss(truth, probs, labels=[0, 1]))
    except ValueError:
        return 0.0


def _first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in frame.columns and frame[column].notna().any():
            return column
    return None


def _compute_price_rank_features(frame: pd.DataFrame, price_column: str, prefix: str) -> pd.DataFrame:
    working = frame.copy()
    price = _safe_numeric(working.get(price_column), fill=np.nan)
    working[f"{prefix}_price"] = price
    working[f"{prefix}_market_prob_raw"] = price.map(raw_market_probability)
    total_prob = working.groupby(RACE_KEYS, dropna=False)[f"{prefix}_market_prob_raw"].transform("sum")
    working[f"{prefix}_market_prob_norm"] = _safe_divide(working[f"{prefix}_market_prob_raw"], total_prob).fillna(0.0)
    working[f"{prefix}_market_rank"] = price.groupby([working[key] for key in RACE_KEYS], dropna=False).rank(
        method="dense",
        ascending=True,
    )
    working[f"{prefix}_market_signal"] = _zscore(working[f"{prefix}_market_rank"], invert=True)
    working[f"{prefix}_odds_signal"] = _zscore(price, invert=True)
    return working


def _prepare_clv_frame(matched_path: Path, target: str) -> tuple[pd.DataFrame, ClvTargetSpec, list[str]]:
    raw = pd.read_csv(matched_path, low_memory=False)
    if raw.empty:
        raise RuntimeError("Matched dataset is empty.")

    frame = attach_common_labels(raw)
    frame = prepare_form_features(frame)
    frame = apply_form_formula(
        frame,
        {
            "finish_weight": 2.0,
            "margin_weight": 1.0,
            "distance_weight": 0.5,
            "class_weight": 0.25,
            "barrier_weight": 0.5,
            "trainer_weight": 0.0,
            "jockey_weight": 0.0,
        },
    )
    frame["won_flag"] = (_safe_numeric(frame.get("finish_position"), fill=0) == 1).astype(int)
    frame["race_day"] = pd.to_datetime(frame["race_date"], errors="coerce")
    frame = frame[frame["race_day"].notna()].copy()
    frame["race_month"] = frame["race_day"].dt.to_period("M").astype(str)
    frame["race_week"] = frame["race_day"].dt.to_period("W").astype(str)
    frame["field_size"] = frame.groupby(RACE_KEYS, dropna=False)["horse_name"].transform("size")
    frame["field_size_bucket"] = frame["field_size"].map(_bucket_field_size)
    frame["region"] = frame.get("region", pd.Series("unknown", index=frame.index)).fillna("unknown")
    frame["race_class_group"] = frame.get("race_class_group", pd.Series("unknown", index=frame.index)).fillna("unknown")

    close_column = _first_existing_column(frame, ["closing_price", "starting_price", "price_5m", "price_10m"])
    if close_column is None:
        raise RuntimeError("No usable close/jump price column found.")
    open_column = _first_existing_column(frame, ["opening_price", "price_open", "open_price"])
    if open_column is None:
        raise RuntimeError("No usable opening price column found.")
    sixty_column = _first_existing_column(frame, ["price_60m"])
    if sixty_column is None:
        raise RuntimeError("No usable 60-minute price column found.")

    frame["opening_price"] = _safe_numeric(frame.get(open_column), fill=np.nan)
    frame["price_60m"] = _safe_numeric(frame.get(sixty_column), fill=np.nan)
    frame["price_30m"] = _safe_numeric(frame.get("price_30m"), fill=np.nan)
    frame["price_10m"] = _safe_numeric(frame.get("price_10m"), fill=np.nan)
    frame["price_5m"] = _safe_numeric(frame.get("price_5m"), fill=np.nan)
    frame["closing_price"] = _safe_numeric(frame.get(close_column), fill=np.nan)

    frame = _compute_price_rank_features(frame, "opening_price", "open")
    frame = _compute_price_rank_features(frame, "price_60m", "sixty")
    frame = _compute_price_rank_features(frame, "closing_price", "close")

    frame["form_signal"] = _zscore(frame["form_score"].fillna(0.0))
    frame["context_score"] = (_zscore(frame["field_size"], invert=True) + _zscore(frame["barrier"], invert=True)) / 2.0
    frame["odds_regime"] = frame["opening_price"].map(_bucket_odds)
    frame["movement_open_to_60"] = (frame["opening_price"] - frame["price_60m"]).fillna(0.0)
    frame["movement_60_to_close"] = (frame["price_60m"] - frame["closing_price"]).fillna(0.0)
    frame["open_to_close_change"] = _safe_numeric(frame.get("open_to_close_change"), fill=np.nan).fillna(
        (frame["opening_price"] - frame["closing_price"]).fillna(0.0)
    )
    frame["60_to_close_change"] = _safe_numeric(frame.get("60_to_close_change"), fill=np.nan).fillna(
        (frame["price_60m"] - frame["closing_price"]).fillna(0.0)
    )
    frame["shortened_from_open"] = (frame["closing_price"] < frame["opening_price"]).astype(int)
    frame["shortened_from_60"] = (frame["closing_price"] < frame["price_60m"]).astype(int)
    frame["clv_open_to_close"] = _safe_divide(frame["opening_price"], frame["closing_price"]).replace([np.inf, -np.inf], np.nan) - 1.0
    frame["clv_60_to_close"] = _safe_divide(frame["price_60m"], frame["closing_price"]).replace([np.inf, -np.inf], np.nan) - 1.0
    frame["clv_open_to_close"] = frame["clv_open_to_close"].fillna(0.0)
    frame["clv_60_to_close"] = frame["clv_60_to_close"].fillna(0.0)
    frame["close_shorten_rate_proxy"] = (frame["closing_price"] < frame["opening_price"]).astype(int)
    frame["market_prob_norm"] = frame["close_market_prob_norm"]
    frame["track_condition_match"] = _safe_numeric(frame.get("track_condition_match"), fill=0.0)
    frame["class_change"] = _safe_numeric(frame.get("class_change"), fill=0.0)
    frame["distance_change"] = _safe_numeric(frame.get("distance_change"), fill=0.0)
    frame["trainer_stat"] = _safe_numeric(frame.get("trainer_stat"), fill=0.0)
    frame["jockey_stat"] = _safe_numeric(frame.get("jockey_stat"), fill=0.0)
    frame["best_last_3_finish"] = _safe_numeric(frame.get("best_last_3_finish"), fill=0.0)
    frame["average_margin_last_3"] = _safe_numeric(frame.get("average_margin_last_3"), fill=0.0)
    frame["last_start_margin"] = _safe_numeric(frame.get("last_start_margin"), fill=0.0)
    frame["days_since_last_start"] = _safe_numeric(frame.get("days_since_last_start"), fill=0.0)
    frame["edge_score"] = _safe_numeric(frame.get("edge_score"), fill=0.0)

    common_features = [
        "form_score",
        "form_signal",
        "context_score",
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
        "field_size_bucket",
        "region",
        "race_class_group",
        "track_condition",
        "track_norm",
    ]

    if target == "open_to_close":
        frame["market_rank_current"] = frame["open_market_rank"]
        frame["market_signal"] = frame["open_market_signal"]
        frame["odds_signal"] = frame["open_odds_signal"]
        frame["anchor_market_prob_norm"] = frame["open_market_prob_norm"]
        frame["anchor_price"] = frame["opening_price"]
        frame["movement_score"] = 0.0
        frame["current_price"] = frame["opening_price"]
        frame["residual_score"] = np.nan
        feature_columns = common_features + [
            "opening_price",
            "open_market_rank",
            "open_market_signal",
            "open_odds_signal",
            "open_market_prob_norm",
        ]
        spec = ClvTargetSpec(
            name="open_to_close",
            anchor_price_column="opening_price",
            target_flag_column="shortened_from_open",
            target_clv_column="clv_open_to_close",
            market_rank_column="open_market_rank",
            market_prob_column="open_market_prob_norm",
            market_signal_column="open_market_signal",
            odds_signal_column="open_odds_signal",
            movement_score_column="movement_score",
            profile_label="open_safe_clv_prediction",
        )
    else:
        frame["market_rank_current"] = frame["sixty_market_rank"]
        frame["market_signal"] = frame["sixty_market_signal"]
        frame["odds_signal"] = frame["sixty_odds_signal"]
        frame["anchor_market_prob_norm"] = frame["sixty_market_prob_norm"]
        frame["anchor_price"] = frame["price_60m"]
        frame["movement_score"] = frame["movement_open_to_60"]
        frame["current_price"] = frame["price_60m"]
        frame["residual_score"] = np.nan
        feature_columns = common_features + [
            "opening_price",
            "price_60m",
            "movement_open_to_60",
            "open_market_rank",
            "open_market_signal",
            "open_market_prob_norm",
            "sixty_market_rank",
            "sixty_market_signal",
            "sixty_odds_signal",
            "sixty_market_prob_norm",
        ]
        spec = ClvTargetSpec(
            name="sixty_to_close",
            anchor_price_column="price_60m",
            target_flag_column="shortened_from_60",
            target_clv_column="clv_60_to_close",
            market_rank_column="sixty_market_rank",
            market_prob_column="sixty_market_prob_norm",
            market_signal_column="sixty_market_signal",
            odds_signal_column="sixty_odds_signal",
            movement_score_column="movement_open_to_60",
            profile_label="sixty_safe_clv_prediction",
        )

    frame["odds_regime"] = frame["anchor_price"].map(_bucket_odds)
    frame["interaction_form_market_rank"] = frame["form_signal"] * frame[spec.market_signal_column]
    frame["interaction_movement_market_rank"] = frame[spec.movement_score_column] * frame[spec.market_signal_column]
    frame["interaction_form_odds"] = frame["form_signal"] * frame[spec.odds_signal_column]
    frame["interaction_field_size_market_rank"] = frame["field_size"] * frame[spec.market_signal_column]
    frame["interaction_odds_movement"] = frame[spec.odds_signal_column] * frame[spec.movement_score_column]
    frame["interaction_residual_market_rank"] = frame["residual_score"].fillna(0.0) * frame[spec.market_signal_column]
    frame["interaction_residual_odds"] = frame["residual_score"].fillna(0.0) * frame[spec.odds_signal_column]
    feature_columns.extend(
        [
            "odds_regime",
            "interaction_form_market_rank",
            "interaction_movement_market_rank",
            "interaction_form_odds",
            "interaction_field_size_market_rank",
            "interaction_odds_movement",
            "interaction_residual_market_rank",
            "interaction_residual_odds",
        ]
    )
    feature_columns = [column for column in feature_columns if column in frame.columns]
    frame["research_profile"] = spec.profile_label
    frame = frame[frame[spec.anchor_price_column].notna() & frame["closing_price"].notna()].copy()
    return frame.sort_values(RACE_KEYS).reset_index(drop=True), spec, feature_columns


def _build_design_matrices(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_features = train_frame[feature_columns].copy()
    test_features = test_frame[feature_columns].copy()
    combined = pd.concat([train_features, test_features], axis=0, ignore_index=True)
    combined = pd.get_dummies(combined, dummy_na=True)
    keep_columns = [column for column in combined.columns if combined[column].notna().any()]
    combined = combined[keep_columns].copy()
    x_train = combined.iloc[: len(train_features)].reset_index(drop=True)
    x_test = combined.iloc[len(train_features) :].reset_index(drop=True)
    return x_train, x_test


def _fit_estimator(spec: ModelSpec, x_train: pd.DataFrame, y_train: pd.Series) -> Any:
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


def _predict_frame(
    model_spec: ModelSpec,
    target_spec: ClvTargetSpec,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    feature_columns: list[str],
) -> pd.DataFrame:
    working = test_frame.copy()
    if model_spec.name == "market_baseline":
        anchor_rank = _safe_numeric(working[target_spec.market_rank_column], fill=np.nan)
        working["predicted_shorten_probability"] = (1.0 / anchor_rank.clip(lower=1.0)).clip(0.05, 0.95).fillna(0.5)
    else:
        x_train, x_test = _build_design_matrices(train_frame, test_frame, feature_columns)
        model = _fit_estimator(model_spec, x_train, train_frame[target_spec.target_flag_column])
        if model is None:
            working["predicted_shorten_probability"] = 0.5
        else:
            working["predicted_shorten_probability"] = pd.Series(model.predict_proba(x_test)[:, 1], index=working.index).clip(1e-6, 1 - 1e-6)
    working["predicted_clv_proxy"] = working["predicted_shorten_probability"] * working[target_spec.target_clv_column]
    working["predicted_movement_direction"] = np.where(working["predicted_shorten_probability"] >= 0.5, 1, -1)
    working["shorten_rank"] = working.groupby(RACE_KEYS, dropna=False)["predicted_shorten_probability"].rank(
        method="dense",
        ascending=False,
    )
    working["model_minus_market_prob"] = working["predicted_shorten_probability"] - working[target_spec.market_prob_column]
    return working


def _curve_report(frame: pd.DataFrame, model_name: str, validation_label: str, target_spec: ClvTargetSpec) -> pd.DataFrame:
    working = frame.copy()
    ranked = working["predicted_shorten_probability"].rank(method="first")
    bucket_count = min(10, max(3, len(working) // 150))
    working["prediction_bucket"] = pd.qcut(ranked, q=bucket_count, duplicates="drop")
    grouped = working.groupby("prediction_bucket", dropna=False, observed=False)
    table = grouped.agg(
        runners=(target_spec.target_flag_column, "size"),
        predicted_shorten_probability=("predicted_shorten_probability", "mean"),
        actual_shorten_rate=(target_spec.target_flag_column, "mean"),
        average_clv=(target_spec.target_clv_column, "mean"),
        average_odds=(target_spec.anchor_price_column, "mean"),
        average_market_rank=(target_spec.market_rank_column, "mean"),
        roi=("profit_loss", "mean"),
    ).reset_index()
    table["calibration_gap"] = table["predicted_shorten_probability"] - table["actual_shorten_rate"]
    table["model_name"] = model_name
    table["validation_label"] = validation_label
    table["target"] = target_spec.name
    table["prediction_bucket"] = table["prediction_bucket"].astype(str)
    return table


def _prepare_bets(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    if working.empty:
        return working
    working["stake"] = 100.0
    working["profit_loss"] = np.where(
        working["won_flag"] == 1,
        (working["closing_price"] - 1.0) * working["stake"],
        -working["stake"],
    )
    return working


def _monthly_positive_clv_rate(frame: pd.DataFrame, clv_column: str) -> float:
    if frame.empty:
        return 0.0
    monthly = frame.groupby("race_month", dropna=False)[clv_column].mean()
    return float((monthly > 0).mean()) if len(monthly) else 0.0


def _track_concentration(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    return float(frame["track_norm"].value_counts(normalize=True, dropna=False).max())


def _market_rank_concentration(frame: pd.DataFrame, rank_column: str) -> float:
    if frame.empty:
        return 0.0
    bucketed = pd.cut(
        _safe_numeric(frame[rank_column], fill=np.nan),
        bins=[0, 2, 5, 8, np.inf],
        labels=["1-2", "3-5", "6-8", "9+"],
        include_lowest=True,
    ).astype(str)
    return float(bucketed.value_counts(normalize=True, dropna=False).max()) if len(bucketed) else 0.0


def _execution_rules(frame: pd.DataFrame, model_name: str) -> dict[str, pd.DataFrame]:
    def top_shortener(source: pd.DataFrame) -> pd.DataFrame:
        if source.empty:
            return source.copy()
        ordered = source.sort_values(
            [*RACE_KEYS, "predicted_shorten_probability", "anchor_price"],
            ascending=[True, True, True, False, True],
        )
        return ordered.groupby(RACE_KEYS, dropna=False).head(1).copy()

    positive_residual = frame[frame["model_minus_market_prob"] > 0].copy()
    rules = {
        "top_1_predicted_shortener_per_race": top_shortener(frame),
        "predicted_shorten_probability_ge_0.55": top_shortener(frame[frame["predicted_shorten_probability"] >= 0.55]),
        "predicted_shorten_probability_ge_0.60": top_shortener(frame[frame["predicted_shorten_probability"] >= 0.60]),
        "predicted_shorten_probability_ge_0.65": top_shortener(frame[frame["predicted_shorten_probability"] >= 0.65]),
        "market_rank_1_to_5_only": top_shortener(frame[frame["market_rank_current"].between(1, 5, inclusive="both")]),
        "odds_2_to_8_only": top_shortener(frame[frame["anchor_price"].between(2.0, 8.0, inclusive="both")]),
        "odds_3_to_10_only": top_shortener(frame[frame["anchor_price"].between(3.0, 10.0, inclusive="both")]),
        "medium_fields_only": top_shortener(frame[frame["field_size_bucket"] == "medium"]),
        "positive_residual_plus_high_shorten_probability": top_shortener(
            positive_residual[positive_residual["predicted_shorten_probability"] >= 0.60]
        ),
    }
    if model_name == "gradient_boosting":
        rules["gradient_boosting_top5_market_rank_plus_high_shorten_probability"] = top_shortener(
            frame[
                frame["market_rank_current"].between(1, 5, inclusive="both")
                & (frame["predicted_shorten_probability"] >= 0.60)
            ]
        )
    return rules


def _summarise_execution(
    frame: pd.DataFrame,
    model_name: str,
    validation_label: str,
    target_spec: ClvTargetSpec,
    min_bets: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rule_name, selection in _execution_rules(frame, model_name).items():
        bets = _prepare_bets(selection)
        stake = float(bets["stake"].sum()) if not bets.empty else 0.0
        roi = float(bets["profit_loss"].sum() / stake) if stake else 0.0
        avg_clv = float(selection[target_spec.target_clv_column].mean()) if not selection.empty else 0.0
        shorten_rate = float(selection[target_spec.target_flag_column].mean()) if not selection.empty else 0.0
        monthly_positive_clv = _monthly_positive_clv_rate(selection, target_spec.target_clv_column)
        track_concentration = _track_concentration(selection)
        market_rank_concentration = _market_rank_concentration(selection, target_spec.market_rank_column)
        remove_best = 0.0
        if not bets.empty:
            winners = bets[bets["profit_loss"] > 0].sort_values("profit_loss", ascending=False)
            trimmed = bets.drop(index=winners.head(2).index)
            if float(trimmed["stake"].sum()) > 0:
                remove_best = float(trimmed["profit_loss"].sum() / trimmed["stake"].sum())
        robustness_score = (
            avg_clv * 0.35
            + shorten_rate * 0.2
            + monthly_positive_clv * 0.15
            + roi * 0.15
            + remove_best * 0.05
            - track_concentration * 0.06
            - market_rank_concentration * 0.04
        )
        survives = bool(
            len(selection) >= min_bets
            and avg_clv > 0
            and shorten_rate >= 0.5
            and monthly_positive_clv >= 0.45
            and track_concentration <= 0.45
            and market_rank_concentration <= 0.6
        )
        rows.append(
            {
                "model_name": model_name,
                "validation_label": validation_label,
                "target": target_spec.name,
                "execution_rule": rule_name,
                "selections": int(len(selection)),
                "actual_shorten_rate": shorten_rate,
                "average_clv": avg_clv,
                "roi": roi,
                "average_odds": float(selection[target_spec.anchor_price_column].mean()) if not selection.empty else 0.0,
                "average_market_rank": float(selection[target_spec.market_rank_column].mean()) if not selection.empty else 0.0,
                "monthly_positive_clv_rate": monthly_positive_clv,
                "track_concentration": track_concentration,
                "market_rank_concentration": market_rank_concentration,
                "drawdown": compute_max_drawdown([1000.0] + (1000.0 + bets["profit_loss"].cumsum()).tolist()) if not bets.empty else 0.0,
                "survives_robustness": survives,
                "robustness_score": robustness_score,
            }
        )
    return pd.DataFrame(rows)


def _segment_report(frame: pd.DataFrame, model_name: str, validation_label: str, target_spec: ClvTargetSpec) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    segment_columns = [
        ("field_size_bucket", "field_size_bucket"),
        ("odds_regime", "odds_regime"),
        ("market_rank_bucket", pd.cut(_safe_numeric(frame[target_spec.market_rank_column], fill=np.nan), bins=[0, 2, 4, 6, np.inf], labels=["1-2", "3-4", "5-6", "7+"], include_lowest=True).astype(str)),
    ]
    for name, source in segment_columns:
        if isinstance(source, str):
            grouped_items = frame.groupby(source, dropna=False)
        else:
            grouped_items = frame.assign(_segment_value=source).groupby("_segment_value", dropna=False)
        for value, subset in grouped_items:
            rows.append(
                {
                    "model_name": model_name,
                    "validation_label": validation_label,
                    "target": target_spec.name,
                    "segment_type": name,
                    "segment_value": str(value),
                    "runners": int(len(subset)),
                    "races": int(subset[RACE_KEYS].drop_duplicates().shape[0]),
                    "predicted_shorten_probability": float(subset["predicted_shorten_probability"].mean()),
                    "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                    "average_clv": float(subset[target_spec.target_clv_column].mean()),
                    "average_odds": float(subset[target_spec.anchor_price_column].mean()),
                    "average_market_rank": float(subset[target_spec.market_rank_column].mean()),
                }
            )
    return pd.DataFrame(rows)


def _extract_feature_importance(model: Any, feature_names: list[str], model_name: str, target: str) -> pd.DataFrame:
    estimator = model
    if isinstance(model, CalibratedClassifierCV):
        if not getattr(model, "calibrated_classifiers_", None):
            return pd.DataFrame()
        estimator = model.calibrated_classifiers_[0].estimator
    if isinstance(estimator, Pipeline):
        estimator = estimator.named_steps["model"]
    if hasattr(estimator, "coef_"):
        importances = np.abs(np.ravel(estimator.coef_))
    elif hasattr(estimator, "feature_importances_"):
        importances = np.ravel(estimator.feature_importances_)
    else:
        return pd.DataFrame()
    return pd.DataFrame(
        {
            "model_name": model_name,
            "target": target,
            "feature_name": feature_names[: len(importances)],
            "importance": importances[: len(feature_names)],
        }
    ).sort_values("importance", ascending=False)


def run_clv_prediction_research(
    matched_path: Path,
    *,
    min_bets: int,
    test_size: float,
    walk_forward_limit: int,
    save_artifacts: bool,
    target: str,
) -> dict[str, pd.DataFrame]:
    ensure_research_dirs()
    print(f"Loading and preparing CLV research frame for target={target}...")
    frame, target_spec, feature_columns = _prepare_clv_frame(matched_path, target)
    train_frame, test_frame = _split_train_test(frame, test_size)
    walk_forward_folds = _build_walk_forward_folds(frame, walk_forward_limit)
    lomo_folds = _build_leave_one_month_out_folds(frame, walk_forward_limit)

    model_specs = [
        ModelSpec("market_baseline", "baseline", False),
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
    segment_rows: list[pd.DataFrame] = []
    execution_rows: list[pd.DataFrame] = []
    curve_rows: list[pd.DataFrame] = []
    importance_rows: list[pd.DataFrame] = []
    baseline_auc = _safe_auc(test_frame[target_spec.target_flag_column], 1.0 / _safe_numeric(test_frame[target_spec.market_rank_column], fill=999).clip(lower=1.0))

    for spec in model_specs:
        print(f"Scoring model: {spec.name}")
        holdout_predictions = _predict_frame(spec, target_spec, train_frame, test_frame, feature_columns)
        holdout_predictions = _prepare_bets(holdout_predictions)
        holdout_auc = _safe_auc(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"])
        holdout_brier = _safe_brier(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"])
        holdout_log_loss = _safe_log_loss(holdout_predictions[target_spec.target_flag_column], holdout_predictions["predicted_shorten_probability"])
        executions = _summarise_execution(holdout_predictions, spec.name, "holdout_test", target_spec, min_bets)
        segments = _segment_report(holdout_predictions, spec.name, "holdout_test", target_spec)
        curve = _curve_report(holdout_predictions, spec.name, "holdout_test", target_spec)

        walk_scores: list[float] = []
        lomo_scores: list[float] = []
        for fold_name, fold_train, fold_test in walk_forward_folds:
            fold_predictions = _predict_frame(spec, target_spec, fold_train, fold_test, feature_columns)
            walk_scores.append(_safe_auc(fold_predictions[target_spec.target_flag_column], fold_predictions["predicted_shorten_probability"]))
            segment_rows.append(_segment_report(fold_predictions, spec.name, fold_name, target_spec))
        for fold_name, fold_train, fold_test in lomo_folds:
            fold_predictions = _predict_frame(spec, target_spec, fold_train, fold_test, feature_columns)
            lomo_scores.append(_safe_auc(fold_predictions[target_spec.target_flag_column], fold_predictions["predicted_shorten_probability"]))
            segment_rows.append(_segment_report(fold_predictions, spec.name, fold_name, target_spec))

        best_execution = executions.sort_values(["robustness_score", "average_clv"], ascending=[False, False]).iloc[0] if not executions.empty else pd.Series(dtype=object)
        result_rows.append(
            {
                "model_name": spec.name,
                "target": target_spec.name,
                "auc": holdout_auc,
                "brier_score": holdout_brier,
                "log_loss": holdout_log_loss,
                "mean_predicted_shorten_probability": float(holdout_predictions["predicted_shorten_probability"].mean()),
                "actual_shorten_rate": float(holdout_predictions[target_spec.target_flag_column].mean()),
                "mean_clv": float(holdout_predictions[target_spec.target_clv_column].mean()),
                "mean_abs_calibration_gap": float(curve["calibration_gap"].abs().mean()) if not curve.empty else 0.0,
                "best_execution_rule": str(best_execution.get("execution_rule", "")),
                "best_execution_average_clv": float(best_execution.get("average_clv", 0.0)),
                "best_execution_roi": float(best_execution.get("roi", 0.0)),
                "best_execution_selections": int(best_execution.get("selections", 0)),
                "best_execution_robustness": float(best_execution.get("robustness_score", 0.0)),
                "walk_forward_auc_mean": float(np.mean(walk_scores)) if walk_scores else 0.0,
                "walk_forward_auc_min": float(np.min(walk_scores)) if walk_scores else 0.0,
                "leave_one_month_auc_mean": float(np.mean(lomo_scores)) if lomo_scores else 0.0,
                "leave_one_month_auc_min": float(np.min(lomo_scores)) if lomo_scores else 0.0,
                "beats_market_baseline_auc": bool(holdout_auc > baseline_auc),
                "survives_robustness": bool(best_execution.get("survives_robustness", False)),
            }
        )
        x_train, _ = _build_design_matrices(train_frame, test_frame, feature_columns)
        trained_model = _fit_estimator(spec, x_train, train_frame[target_spec.target_flag_column]) if spec.name != "market_baseline" else None
        if trained_model is not None:
            importance_rows.append(_extract_feature_importance(trained_model, x_train.columns.tolist(), spec.name, target_spec.name))

        segment_rows.append(segments)
        execution_rows.append(executions)
        curve_rows.append(curve)

    results = pd.DataFrame(result_rows).sort_values(
        ["survives_robustness", "best_execution_robustness", "auc"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    segment_report = pd.concat(segment_rows, ignore_index=True) if segment_rows else pd.DataFrame()
    execution_report = pd.concat(execution_rows, ignore_index=True) if execution_rows else pd.DataFrame()
    feature_importance_report = pd.concat(importance_rows, ignore_index=True) if importance_rows else pd.DataFrame()
    curve_report = pd.concat(curve_rows, ignore_index=True) if curve_rows else pd.DataFrame()

    if save_artifacts:
        save_dataframe(results, _targeted_report_path(RESULTS_PATH, target_spec.name))
        save_dataframe(segment_report, _targeted_report_path(SEGMENTS_PATH, target_spec.name))
        save_dataframe(execution_report, _targeted_report_path(EXECUTION_PATH, target_spec.name))
        save_dataframe(feature_importance_report, _targeted_report_path(FEATURE_IMPORTANCE_PATH, target_spec.name))
        save_dataframe(curve_report, _targeted_report_path(CURVE_PATH, target_spec.name))
        if not results.empty:
            json_dump(results.iloc[0].to_dict(), _targeted_artifact_path(BEST_MODEL_PATH, target_spec.name))

    print()
    print("CLV Prediction Research Summary")
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
        "segments": segment_report,
        "execution_tests": execution_report,
        "feature_importance": feature_importance_report,
        "market_agreement_curve": curve_report,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only CLV / market agreement model.")
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    parser.add_argument("--min-bets", type=int, default=50)
    parser.add_argument("--test-size", type=float, default=0.3)
    parser.add_argument("--walk-forward", type=int, default=6)
    parser.add_argument("--save-artifacts", action="store_true")
    parser.add_argument("--target", choices=["open_to_close", "sixty_to_close"], default="open_to_close")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_clv_prediction_research(
        args.matched_path,
        min_bets=args.min_bets,
        test_size=args.test_size,
        walk_forward_limit=args.walk_forward,
        save_artifacts=args.save_artifacts,
        target=args.target,
    )


if __name__ == "__main__":
    main()
