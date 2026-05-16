from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from app.betting.market_helpers import commission_adjusted_market_probability
from app.research.form_score_optimizer import apply_form_formula, prepare_form_features
from app.research.utils import (
    RESEARCH_ARTIFACTS_DIR,
    RESEARCH_DATA_DIR,
    RESEARCH_REPORTS_DIR,
    attach_common_labels,
    compute_max_drawdown,
    json_dump,
    save_dataframe,
)

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
ODDS_TIME_SERIES_PATH = RESEARCH_DATA_DIR / "odds_time_series.csv"

RESULTS_PATH = RESEARCH_REPORTS_DIR / "fundamental_market_residual_results.csv"
CALIBRATION_PATH = RESEARCH_REPORTS_DIR / "fundamental_market_residual_calibration.csv"
RANKING_PATH = RESEARCH_REPORTS_DIR / "fundamental_market_residual_ranking.csv"
EXECUTION_PATH = RESEARCH_REPORTS_DIR / "fundamental_market_residual_execution.csv"
BEST_MODEL_PATH = RESEARCH_ARTIFACTS_DIR / "best_fundamental_market_residual_model.json"

COMMISSION_RATE = 0.08
FLAT_STAKE = 100.0
MIN_RUNNERS_PER_RACE = 3
MIN_EXECUTION_BETS = 50
PREFERRED_EXECUTION_BETS = 100
RACE_KEYS = ["race_date", "track_norm", "race_number"]


@dataclass(frozen=True)
class ModelSpec:
    label: str
    calibration: str


FUNDAMENTAL_SPECS = [
    ModelSpec(label="fundamental_sigmoid", calibration="sigmoid"),
    ModelSpec(label="fundamental_isotonic", calibration="isotonic"),
]
MARKET_SPECS = [
    ModelSpec(label="market_sigmoid", calibration="sigmoid"),
    ModelSpec(label="market_isotonic", calibration="isotonic"),
]


def _default_form_config() -> dict[str, float]:
    return {
        "finish_weight": 2.0,
        "margin_weight": 1.0,
        "distance_weight": 0.5,
        "class_weight": 0.25,
        "barrier_weight": 0.5,
        "trainer_weight": 0.0,
        "jockey_weight": 0.0,
    }


def _safe_numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


def _first_existing(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _choose_calibration_folds(y: pd.Series) -> int | None:
    positives = int(y.sum())
    negatives = int(len(y) - positives)
    minority = min(positives, negatives)
    if minority < 3:
        return None
    return min(5, minority)


def _can_use_isotonic(y: pd.Series) -> bool:
    positives = int(y.sum())
    negatives = int(len(y) - positives)
    return positives >= 20 and negatives >= 20


def _safe_log_loss(y_true: pd.Series, probabilities: pd.Series) -> float:
    truth = pd.to_numeric(y_true, errors="coerce").fillna(0).astype(int)
    probs = pd.to_numeric(probabilities, errors="coerce").fillna(0).clip(1e-6, 1 - 1e-6)
    if truth.empty:
        return 0.0
    return float(log_loss(truth, probs, labels=[0, 1]))


def _dcg(relevances: list[float]) -> float:
    return sum(rel / math.log2(index + 2) for index, rel in enumerate(relevances))


def _ndcg_for_race(race: pd.DataFrame, rank_column: str, k: int = 3) -> float:
    ranked = race.sort_values(rank_column).head(k)
    actual = ranked["won_flag"].astype(float).tolist()
    ideal = sorted(race["won_flag"].astype(float).tolist(), reverse=True)[:k]
    ideal_dcg = _dcg(ideal)
    if ideal_dcg <= 0:
        return 0.0
    return _dcg(actual) / ideal_dcg


def _normalise_race_probabilities(frame: pd.DataFrame, probability_column: str, output_column: str) -> pd.DataFrame:
    working = frame.copy()
    raw_prob = pd.to_numeric(working[probability_column], errors="coerce").fillna(0.0).clip(lower=1e-9)
    totals = raw_prob.groupby([working[key] for key in RACE_KEYS], dropna=False).transform("sum")
    working[output_column] = (raw_prob / totals.replace({0.0: np.nan})).fillna(0.0)
    return working


def _rank_from_column(frame: pd.DataFrame, score_column: str, rank_column: str, ascending: bool = False) -> pd.DataFrame:
    working = frame.copy()
    working[rank_column] = (
        pd.to_numeric(working[score_column], errors="coerce")
        .groupby([working[key] for key in RACE_KEYS], dropna=False)
        .rank(method="dense", ascending=ascending)
    )
    return working


def _bucket_probability(value: float) -> str:
    if pd.isna(value):
        return "unknown"
    buckets = [0.0, 0.02, 0.05, 0.10, 0.15, 0.20, 0.30, 0.40, 0.60, 1.0]
    for lower, upper in zip(buckets[:-1], buckets[1:]):
        if lower <= value < upper or (upper == 1.0 and value <= upper):
            return f"{lower:.2f}-{upper:.2f}"
    return "unknown"


def _form_missing_warning_rows(frame: pd.DataFrame) -> pd.DataFrame:
    feature_groups = {
        "speed_sectionals": ["speed_figure", "sectional_strength", "last_600_rating"],
        "pace_shape": ["pace_score", "tempo_score", "race_shape_score", "pace_pressure"],
        "trainer_jockey_interaction": ["trainer_jockey_combo", "trainer_jockey_win_rate"],
        "days_since_last_start": ["days_since_last_start"],
    }
    rows = []
    for family, columns in feature_groups.items():
        present = [column for column in columns if column in frame.columns]
        missing = [column for column in columns if column not in frame.columns]
        rows.append(
            {
                "feature_family": family,
                "available_columns": ",".join(present),
                "missing_columns": ",".join(missing),
                "missing_count": len(missing),
            }
        )
    return pd.DataFrame(rows)


def _load_frame(
    matched_path: Path = MATCHED_PATH,
    odds_time_series_path: Path = ODDS_TIME_SERIES_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not matched_path.exists():
        raise RuntimeError(f"Matched dataset not found: {matched_path}")

    raw = pd.read_csv(matched_path, low_memory=False)
    prepared = prepare_form_features(raw)
    scored = apply_form_formula(prepared, _default_form_config())
    scored = attach_common_labels(scored)

    scored["race_day"] = pd.to_datetime(scored["race_date"], errors="coerce")
    scored = scored[scored["race_day"].notna()].copy()
    scored["race_month"] = scored["race_day"].dt.to_period("M").astype(str)
    scored["won_flag"] = pd.to_numeric(scored["won_flag"], errors="coerce").fillna(0).astype(int)
    scored["finish_position"] = pd.to_numeric(scored["finish_position"], errors="coerce")
    scored["barrier"] = _safe_numeric(scored, "barrier")
    scored["weight"] = _safe_numeric(scored, "weight")
    scored["distance"] = _safe_numeric(scored, "distance")
    scored["starting_price"] = _safe_numeric(scored, "starting_price")
    scored["opening_price"] = _safe_numeric(scored, "opening_price")
    scored["price_60m"] = _safe_numeric(scored, "price_60m")
    scored["price_30m"] = _safe_numeric(scored, "price_30m")
    scored["price_10m"] = _safe_numeric(scored, "price_10m")
    scored["price_5m"] = _safe_numeric(scored, "price_5m")
    scored["closing_price"] = _safe_numeric(scored, "closing_price")
    scored["last_start_finish"] = _safe_numeric(scored, "last_start_finish")
    scored["best_last_3_finish"] = _safe_numeric(scored, "best_last_3_finish")
    scored["average_last_3_finish"] = _safe_numeric(scored, "average_last_3_finish")
    scored["average_last_3_margin"] = _safe_numeric(scored, "average_last_3_margin")
    scored["average_margin_last_3"] = _safe_numeric(scored, "average_margin_last_3")
    scored["margin"] = _safe_numeric(scored, "margin")
    scored["days_since_last_start"] = _safe_numeric(scored, "days_since_last_start")

    scored["trainer"] = scored.get("trainer", pd.Series("unknown", index=scored.index)).fillna("unknown").astype(str)
    scored["jockey"] = scored.get("jockey", pd.Series("unknown", index=scored.index)).fillna("unknown").astype(str)
    scored["class_name"] = scored.get("class_name", pd.Series("unknown", index=scored.index)).fillna("unknown").astype(str)
    scored["track_condition"] = scored.get("track_condition", pd.Series("unknown", index=scored.index)).fillna("unknown").astype(str)
    scored["trainer_jockey_combo"] = scored["trainer"] + " | " + scored["jockey"]

    if odds_time_series_path.exists():
        time_series = pd.read_csv(odds_time_series_path, low_memory=False)
        time_series = attach_common_labels(time_series)
        time_series["race_date"] = pd.to_datetime(time_series["race_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        scored["race_date_join"] = scored["race_day"].dt.strftime("%Y-%m-%d")
        merged = scored.merge(
            time_series[
                [
                    "race_date",
                    "track_norm",
                    "race_number",
                    "horse_name_norm",
                    "odds_60m",
                    "odds_30m",
                    "odds_10m",
                    "odds_5m",
                    "odds_3m",
                    "odds_1m",
                    "latest_odds",
                ]
            ],
            left_on=["race_date_join", "track_norm", "race_number", "horse_name_norm"],
            right_on=["race_date", "track_norm", "race_number", "horse_name_norm"],
            how="left",
            suffixes=("", "_ts"),
        )
        scored = merged
    else:
        scored["odds_3m"] = np.nan
        scored["odds_1m"] = np.nan
        scored["latest_odds"] = np.nan

    current_odds = (
        pd.to_numeric(scored.get("odds_1m"), errors="coerce")
        .combine_first(pd.to_numeric(scored.get("latest_odds"), errors="coerce"))
        .combine_first(pd.to_numeric(scored.get("closing_price"), errors="coerce"))
        .combine_first(pd.to_numeric(scored.get("starting_price"), errors="coerce"))
    )
    scored["current_odds"] = current_odds
    scored = scored[scored["current_odds"].notna() & (scored["current_odds"] > 1.0)].copy()
    scored["field_size"] = scored.groupby(RACE_KEYS, dropna=False)["horse_name"].transform("size")
    scored["market_rank"] = scored.groupby(RACE_KEYS, dropna=False)["current_odds"].rank(method="dense", ascending=True)
    scored["implied_probability"] = scored["current_odds"].map(
        lambda value: commission_adjusted_market_probability(float(value), COMMISSION_RATE)
    )
    scored["odds_bucket"] = pd.cut(
        scored["current_odds"],
        bins=[0.0, 3.0, 6.0, 10.0, 20.0, 1000.0],
        labels=["0-3", "3-6", "6-10", "10-20", "20+"],
        include_lowest=True,
    ).astype(str)
    scored["field_size_bucket"] = pd.cut(
        scored["field_size"],
        bins=[0, 7, 11, 99],
        labels=["small", "medium", "large"],
        include_lowest=True,
    ).astype(str)

    for column in ["odds_60m", "odds_30m", "odds_10m", "odds_5m", "odds_3m", "odds_1m", "latest_odds"]:
        scored[column] = pd.to_numeric(scored.get(column), errors="coerce")

    movement_inputs = {
        "open_to_current": "opening_price",
        "60_to_current": "odds_60m",
        "30_to_current": "odds_30m",
        "10_to_current": "odds_10m",
        "5_to_current": "odds_5m",
        "3_to_current": "odds_3m",
        "1_to_current": "odds_1m",
    }
    for output, source in movement_inputs.items():
        scored[output] = (
            pd.to_numeric(scored.get(source), errors="coerce") - scored["current_odds"]
        ).fillna(0.0)

    scored["late_movement"] = (
        scored[["open_to_current", "60_to_current", "30_to_current", "10_to_current", "5_to_current", "3_to_current"]]
        .apply(pd.to_numeric, errors="coerce")
        .mean(axis=1)
        .fillna(0.0)
    )
    scored["track_condition_suitability"] = scored["track_condition_match"].fillna(0.0)
    scored["speed_figure"] = _safe_numeric(scored, "speed_figure")
    scored["sectional_strength"] = _safe_numeric(scored, "sectional_strength")
    scored["pace_score"] = _safe_numeric(scored, "pace_score")
    scored["race_shape_score"] = _safe_numeric(scored, "race_shape_score")

    warnings_frame = _form_missing_warning_rows(scored)
    return scored.sort_values(["race_day", "track_norm", "race_number", "horse_name"]).reset_index(drop=True), warnings_frame


def _time_split(frame: pd.DataFrame, train_ratio: float = 0.7) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordered_dates = sorted(frame["race_day"].dt.date.unique().tolist())
    if len(ordered_dates) < 2:
        return frame.copy(), frame.copy()
    split_index = max(1, int(len(ordered_dates) * train_ratio))
    split_index = min(split_index, len(ordered_dates) - 1)
    train_dates = ordered_dates[:split_index]
    test_dates = ordered_dates[split_index:]
    train = frame[frame["race_day"].dt.date.isin(train_dates)].copy()
    test = frame[frame["race_day"].dt.date.isin(test_dates)].copy()
    return train, test


def _leave_one_month_out_folds(frame: pd.DataFrame) -> list[tuple[str, pd.DataFrame, pd.DataFrame]]:
    folds = []
    for month in sorted(frame["race_month"].unique().tolist()):
        train = frame[frame["race_month"] != month].copy()
        test = frame[frame["race_month"] == month].copy()
        if train.empty or test.empty:
            continue
        folds.append((f"lomo_{month}", train, test))
    return folds


def _build_preprocessor(numeric_features: list[str], categorical_features: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_features,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_features,
            ),
        ],
        remainder="drop",
    )


def _fit_model_variant(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    feature_columns: list[str],
    categorical_columns: list[str],
    spec: ModelSpec,
    probability_column: str,
    probability_norm_column: str,
    rank_column: str,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    x_train = train[feature_columns + categorical_columns].copy()
    y_train = train["won_flag"].astype(int)
    x_test = test[feature_columns + categorical_columns].copy()

    folds = _choose_calibration_folds(y_train)
    if folds is None:
        return None, {"model_name": spec.label, "warning": "not_enough_class_balance_for_calibration"}
    if spec.calibration == "isotonic" and not _can_use_isotonic(y_train):
        return None, {"model_name": spec.label, "warning": "not_enough_data_for_isotonic"}

    model = Pipeline(
        steps=[
            ("preprocessor", _build_preprocessor(feature_columns, categorical_columns)),
            (
                "model",
                CalibratedClassifierCV(
                    estimator=LogisticRegression(
                        max_iter=4000,
                        class_weight="balanced",
                        solver="saga",
                        random_state=42,
                    ),
                    method=spec.calibration,
                    cv=folds,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)
    probabilities = model.predict_proba(x_test)[:, 1]
    output = test.copy()
    output["model_name"] = spec.label
    output[probability_column] = probabilities
    output = _normalise_race_probabilities(output, probability_column, probability_norm_column)
    output = _rank_from_column(output, probability_norm_column, rank_column, ascending=False)
    metrics = {
        "model_name": spec.label,
        "brier_score": float(brier_score_loss(output["won_flag"], output[probability_column])),
        "log_loss": _safe_log_loss(output["won_flag"], output[probability_column]),
        "average_probability": float(pd.to_numeric(output[probability_column], errors="coerce").mean()),
        "actual_win_rate": float(output["won_flag"].mean()),
    }
    metrics["overconfidence_gap"] = metrics["average_probability"] - metrics["actual_win_rate"]
    return output, metrics


def _fit_raw_logistic_frame(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    feature_columns: list[str],
    categorical_columns: list[str],
    probability_column: str,
    probability_norm_column: str,
    rank_column: str,
) -> pd.DataFrame:
    model = Pipeline(
        steps=[
            ("preprocessor", _build_preprocessor(feature_columns, categorical_columns)),
            (
                "model",
                LogisticRegression(
                    max_iter=4000,
                    class_weight="balanced",
                    solver="saga",
                    random_state=42,
                ),
            ),
        ]
    )
    x_train = train[feature_columns + categorical_columns].copy()
    y_train = train["won_flag"].astype(int)
    x_test = test[feature_columns + categorical_columns].copy()
    model.fit(x_train, y_train)
    probabilities = model.predict_proba(x_test)[:, 1]
    output = test.copy()
    output[probability_column] = probabilities
    output = _normalise_race_probabilities(output, probability_column, probability_norm_column)
    output = _rank_from_column(output, probability_norm_column, rank_column, ascending=False)
    return output


def _ranking_metrics(frame: pd.DataFrame, rank_column: str) -> dict[str, float]:
    valid = frame.copy()
    valid["finish_position"] = pd.to_numeric(valid["finish_position"], errors="coerce")
    valid = valid[valid["finish_position"].notna()].copy()
    if valid.empty:
        return {
            "evaluated_races": 0.0,
            "top1_hit_rate": 0.0,
            "top3_hit_rate": 0.0,
            "ndcg_at_3": 0.0,
            "average_finish_top1": 0.0,
            "rank_correlation": 0.0,
            "market_top1_hit_rate": 0.0,
            "market_top3_hit_rate": 0.0,
            "market_ndcg_at_3": 0.0,
            "market_average_finish_top1": 0.0,
            "market_rank_correlation": 0.0,
            "top1_hit_delta_vs_market": 0.0,
            "top3_hit_delta_vs_market": 0.0,
            "ndcg_delta_vs_market": 0.0,
            "average_finish_delta_vs_market": 0.0,
            "rank_correlation_delta_vs_market": 0.0,
        }

    top1 = valid[valid[rank_column] == 1]
    market_top1 = valid[valid["market_rank"] == 1]
    top1_hit_rate = float(top1["won_flag"].mean()) if len(top1) else 0.0
    market_top1_hit_rate = float(market_top1["won_flag"].mean()) if len(market_top1) else 0.0
    top3_hits: list[float] = []
    market_top3_hits: list[float] = []
    ndcgs: list[float] = []
    market_ndcgs: list[float] = []
    correlations: list[float] = []
    market_correlations: list[float] = []
    evaluated_races = 0
    for _, race in valid.groupby(RACE_KEYS, dropna=False):
        if len(race) < MIN_RUNNERS_PER_RACE:
            continue
        evaluated_races += 1
        top3_hits.append(float(((race[rank_column] <= 3) & (race["won_flag"] == 1)).any()))
        market_top3_hits.append(float(((race["market_rank"] <= 3) & (race["won_flag"] == 1)).any()))
        ndcgs.append(_ndcg_for_race(race, rank_column))
        market_ndcgs.append(_ndcg_for_race(race, "market_rank"))
        corr = race[rank_column].corr(race["finish_position"], method="spearman")
        market_corr = race["market_rank"].corr(race["finish_position"], method="spearman")
        if pd.notna(corr):
            correlations.append(float(-corr))
        if pd.notna(market_corr):
            market_correlations.append(float(-market_corr))

    avg_finish_top1 = float(top1["finish_position"].mean()) if len(top1) else 0.0
    market_avg_finish = float(market_top1["finish_position"].mean()) if len(market_top1) else 0.0
    top3_hit_rate = float(np.mean(top3_hits)) if top3_hits else 0.0
    market_top3_hit_rate = float(np.mean(market_top3_hits)) if market_top3_hits else 0.0
    ndcg_at_3 = float(np.mean(ndcgs)) if ndcgs else 0.0
    market_ndcg_at_3 = float(np.mean(market_ndcgs)) if market_ndcgs else 0.0
    rank_correlation = float(np.mean(correlations)) if correlations else 0.0
    market_rank_correlation = float(np.mean(market_correlations)) if market_correlations else 0.0
    return {
        "evaluated_races": float(evaluated_races),
        "top1_hit_rate": top1_hit_rate,
        "top3_hit_rate": top3_hit_rate,
        "ndcg_at_3": ndcg_at_3,
        "average_finish_top1": avg_finish_top1,
        "rank_correlation": rank_correlation,
        "market_top1_hit_rate": market_top1_hit_rate,
        "market_top3_hit_rate": market_top3_hit_rate,
        "market_ndcg_at_3": market_ndcg_at_3,
        "market_average_finish_top1": market_avg_finish,
        "market_rank_correlation": market_rank_correlation,
        "top1_hit_delta_vs_market": top1_hit_rate - market_top1_hit_rate,
        "top3_hit_delta_vs_market": top3_hit_rate - market_top3_hit_rate,
        "ndcg_delta_vs_market": ndcg_at_3 - market_ndcg_at_3,
        "average_finish_delta_vs_market": market_avg_finish - avg_finish_top1,
        "rank_correlation_delta_vs_market": rank_correlation - market_rank_correlation,
    }


def _build_calibration_report(frame: pd.DataFrame, model_name: str, probability_column: str, report_section: str) -> pd.DataFrame:
    working = frame.copy()
    working["probability_bucket"] = working[probability_column].map(_bucket_probability)
    grouped = working.groupby("probability_bucket", dropna=False)
    rows = [
        {
            "report_section": report_section,
            "model_name": model_name,
            "probability_bucket": bucket,
            "count": len(bucket_frame),
            "mean_predicted_probability": float(bucket_frame[probability_column].mean()),
            "actual_win_rate": float(bucket_frame["won_flag"].mean()),
            "brier_score": float(brier_score_loss(bucket_frame["won_flag"], bucket_frame[probability_column]))
            if len(bucket_frame)
            else 0.0,
            "log_loss": _safe_log_loss(bucket_frame["won_flag"], bucket_frame[probability_column]),
        }
        for bucket, bucket_frame in grouped
    ]
    overall = {
        "report_section": "overall",
        "model_name": model_name,
        "probability_bucket": "all",
        "count": len(working),
        "mean_predicted_probability": float(working[probability_column].mean()),
        "actual_win_rate": float(working["won_flag"].mean()),
        "brier_score": float(brier_score_loss(working["won_flag"], working[probability_column])) if len(working) else 0.0,
        "log_loss": _safe_log_loss(working["won_flag"], working[probability_column]),
    }
    report = pd.DataFrame([overall, *rows])
    report["overconfidence_gap"] = report["mean_predicted_probability"] - report["actual_win_rate"]
    return report


def _execution_profit_loss(odds: pd.Series, won_flag: pd.Series) -> pd.Series:
    return np.where(
        pd.to_numeric(won_flag, errors="coerce").fillna(0).astype(int) == 1,
        (pd.to_numeric(odds, errors="coerce").fillna(0.0) - 1.0) * (1.0 - COMMISSION_RATE) * FLAT_STAKE,
        -FLAT_STAKE,
    )


def _remove_top_winner_roi(bets: pd.DataFrame, winners_to_remove: int) -> float:
    if bets.empty:
        return 0.0
    winners = bets[bets["profit_loss"] > 0].sort_values("profit_loss", ascending=False)
    if winners.empty:
        total_staked = bets["stake"].sum()
        return float(bets["profit_loss"].sum() / total_staked) if total_staked else 0.0
    remaining = bets.drop(index=winners.head(winners_to_remove).index)
    total_staked = remaining["stake"].sum()
    if total_staked <= 0:
        return 0.0
    return float(remaining["profit_loss"].sum() / total_staked)


def _execution_summary(bets: pd.DataFrame) -> dict[str, float | int]:
    if bets.empty:
        return {
            "bets": 0,
            "wins": 0,
            "roi": 0.0,
            "profit_loss": 0.0,
            "strike_rate": 0.0,
            "average_odds": 0.0,
            "max_drawdown": 0.0,
            "remove_best_winner_roi": 0.0,
            "remove_top2_winners_roi": 0.0,
            "track_concentration": 0.0,
            "month_concentration": 0.0,
        }
    working = bets.copy()
    working["bank_after_bet"] = 10000.0 + working["profit_loss"].cumsum()
    total_staked = float(working["stake"].sum())
    return {
        "bets": int(len(working)),
        "wins": int(working["won_flag"].sum()),
        "roi": float(working["profit_loss"].sum() / total_staked) if total_staked else 0.0,
        "profit_loss": float(working["profit_loss"].sum()),
        "strike_rate": float(working["won_flag"].mean()),
        "average_odds": float(working["current_odds"].mean()),
        "max_drawdown": compute_max_drawdown([10000.0] + working["bank_after_bet"].tolist()),
        "remove_best_winner_roi": _remove_top_winner_roi(working, 1),
        "remove_top2_winners_roi": _remove_top_winner_roi(working, 2),
        "track_concentration": float(working["track_norm"].value_counts(normalize=True, dropna=False).max()),
        "month_concentration": float(working["race_month"].value_counts(normalize=True, dropna=False).max()),
    }


def _apply_execution_rule(frame: pd.DataFrame, rule_name: str) -> pd.DataFrame:
    working = frame.copy()
    top_model = working.sort_values(RACE_KEYS + ["calibrated_model_rank"]).groupby(RACE_KEYS, dropna=False).head(1).copy()
    top_overlay = working.sort_values(RACE_KEYS + ["market_residual_rank"]).groupby(RACE_KEYS, dropna=False).head(1).copy()
    if rule_name == "A_edge_ge_0.02":
        selected = working[working["true_edge"] >= 0.02].copy()
    elif rule_name == "B_edge_ge_0.05":
        selected = working[working["true_edge"] >= 0.05].copy()
    elif rule_name == "C_rank_overlay_ge_2":
        selected = working[working["rank_overlay"] >= 2].copy()
    elif rule_name == "D_rank_overlay_ge_2_and_edge_ge_0.02":
        selected = working[(working["rank_overlay"] >= 2) & (working["true_edge"] >= 0.02)].copy()
    elif rule_name == "E_top1_model_if_odds_2_8":
        selected = top_model[top_model["current_odds"].between(2.0, 8.0, inclusive="both")].copy()
    elif rule_name == "F_top1_overlay_if_odds_3_12":
        selected = top_overlay[top_overlay["current_odds"].between(3.0, 12.0, inclusive="both")].copy()
    else:
        selected = pd.DataFrame(columns=working.columns)
    if selected.empty:
        return selected
    selected["stake"] = FLAT_STAKE
    selected["profit_loss"] = _execution_profit_loss(selected["current_odds"], selected["won_flag"])
    selected["rule_name"] = rule_name
    return selected


def _execution_report_rows(
    holdout_predictions: pd.DataFrame,
    lomo_predictions: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    for rule_name in [
        "A_edge_ge_0.02",
        "B_edge_ge_0.05",
        "C_rank_overlay_ge_2",
        "D_rank_overlay_ge_2_and_edge_ge_0.02",
        "E_top1_model_if_odds_2_8",
        "F_top1_overlay_if_odds_3_12",
    ]:
        holdout_bets = _apply_execution_rule(holdout_predictions, rule_name)
        lomo_bets = _apply_execution_rule(lomo_predictions, rule_name)
        holdout_summary = _execution_summary(holdout_bets)
        lomo_summary = _execution_summary(lomo_bets)
        lomo_monthly_roi = (
            lomo_bets.groupby("race_month", dropna=False)["profit_loss"].sum()
            / lomo_bets.groupby("race_month", dropna=False)["stake"].sum().replace({0.0: np.nan})
            if not lomo_bets.empty
            else pd.Series(dtype=float)
        )
        rows.append(
            {
                "rule_name": rule_name,
                "holdout_bets": int(holdout_summary["bets"]),
                "holdout_wins": int(holdout_summary["wins"]),
                "holdout_roi": float(holdout_summary["roi"]),
                "holdout_profit_loss": float(holdout_summary["profit_loss"]),
                "holdout_strike_rate": float(holdout_summary["strike_rate"]),
                "holdout_average_odds": float(holdout_summary["average_odds"]),
                "holdout_drawdown": float(holdout_summary["max_drawdown"]),
                "remove_best_winner_roi": float(holdout_summary["remove_best_winner_roi"]),
                "remove_top2_winners_roi": float(holdout_summary["remove_top2_winners_roi"]),
                "lomo_bets": int(lomo_summary["bets"]),
                "lomo_roi": float(lomo_summary["roi"]),
                "lomo_drawdown": float(lomo_summary["max_drawdown"]),
                "lomo_positive_month_share": float((lomo_monthly_roi.fillna(0.0) > 0).mean()) if len(lomo_monthly_roi) else 0.0,
                "lomo_min_month_roi": float(lomo_monthly_roi.min()) if len(lomo_monthly_roi) else 0.0,
                "track_concentration": float(holdout_summary["track_concentration"]),
                "month_concentration": float(holdout_summary["month_concentration"]),
                "meets_min_bets": bool(int(holdout_summary["bets"]) >= MIN_EXECUTION_BETS),
                "meets_preferred_bets": bool(int(holdout_summary["bets"]) >= PREFERRED_EXECUTION_BETS),
                "survives_robustness": bool(
                    int(holdout_summary["bets"]) >= MIN_EXECUTION_BETS
                    and float(holdout_summary["roi"]) > 0
                    and float(holdout_summary["remove_best_winner_roi"]) > 0
                    and float(holdout_summary["remove_top2_winners_roi"]) > 0
                    and float(lomo_summary["roi"]) > 0
                    and float(holdout_summary["track_concentration"]) <= 0.35
                    and float(holdout_summary["month_concentration"]) <= 0.35
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["survives_robustness", "holdout_roi", "holdout_bets"],
        ascending=[False, False, False],
    )


def _jsonable(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def _row_payload(row: pd.Series) -> dict[str, Any]:
    return {key: _jsonable(value) for key, value in row.to_dict().items()}


def _build_ranking_rows(frame: pd.DataFrame, validation_label: str) -> list[dict[str, Any]]:
    methods = {
        "market_baseline": "market_rank",
        "fundamental_model_rank": "fundamental_model_rank",
        "calibrated_model_rank": "calibrated_model_rank",
        "market_residual_model_rank": "market_residual_rank",
    }
    rows = []
    for method_name, rank_column in methods.items():
        metrics = _ranking_metrics(frame, rank_column)
        rows.append({"validation_label": validation_label, "method_name": method_name, **metrics})
    return rows


def _select_best_variant(metrics_rows: list[dict[str, Any]], prefix: str) -> str:
    filtered = [row for row in metrics_rows if row["model_name"].startswith(prefix)]
    if not filtered:
        raise RuntimeError(f"No fitted variants for prefix {prefix}")
    ordered = sorted(filtered, key=lambda row: (row["brier_score"], row["log_loss"]))
    return str(ordered[0]["model_name"])


def run_research(
    matched_path: Path = MATCHED_PATH,
    odds_time_series_path: Path = ODDS_TIME_SERIES_PATH,
    *,
    train_ratio: float = 0.7,
    skip_save: bool = False,
) -> dict[str, Any]:
    frame, missing_feature_warnings = _load_frame(matched_path, odds_time_series_path)
    train, test = _time_split(frame, train_ratio=train_ratio)

    fundamental_numeric = [
        "form_score",
        "last_start_finish",
        "best_last_3_finish",
        "average_last_3_finish",
        "days_since_last_start",
        "class_change",
        "distance_change",
        "track_condition_suitability",
        "barrier",
        "weight",
        "speed_figure",
        "sectional_strength",
        "pace_score",
        "race_shape_score",
    ]
    fundamental_categorical = [
        "jockey",
        "trainer",
        "trainer_jockey_combo",
        "class_name",
        "track_condition",
        "track_norm",
    ]
    market_numeric = [
        "implied_probability",
        "market_rank",
        "current_odds",
        "field_size",
        "late_movement",
        "open_to_current",
        "60_to_current",
        "30_to_current",
        "10_to_current",
        "5_to_current",
        "3_to_current",
    ]
    market_categorical = [
        "odds_bucket",
        "field_size_bucket",
    ]

    calibration_rows: list[pd.DataFrame] = []
    holdout_metric_rows: list[dict[str, Any]] = []
    fitted_frames: dict[str, pd.DataFrame] = {}
    holdout_raw_fundamental = _fit_raw_logistic_frame(
        train,
        test,
        feature_columns=fundamental_numeric,
        categorical_columns=fundamental_categorical,
        probability_column="fundamental_uncalibrated_raw_probability",
        probability_norm_column="fundamental_uncalibrated_probability",
        rank_column="fundamental_model_rank",
    )

    for spec in FUNDAMENTAL_SPECS:
        fitted, metrics = _fit_model_variant(
            train,
            test,
            feature_columns=fundamental_numeric,
            categorical_columns=fundamental_categorical,
            spec=spec,
            probability_column="fundamental_raw_probability",
            probability_norm_column="fundamental_probability",
            rank_column="calibrated_model_rank",
        )
        if fitted is not None:
            fitted_frames[spec.label] = fitted
            holdout_metric_rows.append(metrics)
            calibration_rows.append(_build_calibration_report(fitted, spec.label, "fundamental_raw_probability", "holdout"))

    for spec in MARKET_SPECS:
        fitted, metrics = _fit_model_variant(
            train,
            test,
            feature_columns=market_numeric,
            categorical_columns=market_categorical,
            spec=spec,
            probability_column="market_raw_probability",
            probability_norm_column="corrected_market_probability",
            rank_column="corrected_market_rank",
        )
        if fitted is not None:
            fitted_frames[spec.label] = fitted
            holdout_metric_rows.append(metrics)
            calibration_rows.append(_build_calibration_report(fitted, spec.label, "market_raw_probability", "holdout"))

    best_fundamental_name = _select_best_variant(holdout_metric_rows, "fundamental_")
    best_market_name = _select_best_variant(holdout_metric_rows, "market_")

    holdout = holdout_raw_fundamental.copy()
    holdout["calibrated_model_probability"] = fitted_frames[best_fundamental_name]["fundamental_probability"].values
    holdout["calibrated_model_rank"] = fitted_frames[best_fundamental_name]["calibrated_model_rank"].values
    holdout["corrected_market_probability"] = fitted_frames[best_market_name]["corrected_market_probability"].values
    holdout["true_edge"] = holdout["calibrated_model_probability"] - holdout["corrected_market_probability"]
    holdout = _rank_from_column(holdout, "true_edge", "market_residual_rank", ascending=False)
    holdout["rank_overlay"] = holdout["market_rank"] - holdout["calibrated_model_rank"]

    ranking_rows = _build_ranking_rows(holdout, "holdout_test")

    lomo_predictions = []
    for fold_name, fold_train, fold_test in _leave_one_month_out_folds(frame):
        fold_raw = _fit_raw_logistic_frame(
            fold_train,
            fold_test,
            feature_columns=fundamental_numeric,
            categorical_columns=fundamental_categorical,
            probability_column="fundamental_uncalibrated_raw_probability",
            probability_norm_column="fundamental_uncalibrated_probability",
            rank_column="fundamental_model_rank",
        )
        fold_fundamental, _ = _fit_model_variant(
            fold_train,
            fold_test,
            feature_columns=fundamental_numeric,
            categorical_columns=fundamental_categorical,
            spec=ModelSpec(best_fundamental_name, "isotonic" if "isotonic" in best_fundamental_name else "sigmoid"),
            probability_column="fundamental_raw_probability",
            probability_norm_column="fundamental_probability",
            rank_column="calibrated_model_rank",
        )
        fold_market, _ = _fit_model_variant(
            fold_train,
            fold_test,
            feature_columns=market_numeric,
            categorical_columns=market_categorical,
            spec=ModelSpec(best_market_name, "isotonic" if "isotonic" in best_market_name else "sigmoid"),
            probability_column="market_raw_probability",
            probability_norm_column="corrected_market_probability",
            rank_column="corrected_market_rank",
        )
        if fold_fundamental is None or fold_market is None:
            continue
        fold = fold_raw.copy()
        fold["calibrated_model_probability"] = fold_fundamental["fundamental_probability"].values
        fold["calibrated_model_rank"] = fold_fundamental["calibrated_model_rank"].values
        fold["corrected_market_probability"] = fold_market["corrected_market_probability"].values
        fold["true_edge"] = fold["calibrated_model_probability"] - fold["corrected_market_probability"]
        fold = _rank_from_column(fold, "true_edge", "market_residual_rank", ascending=False)
        fold["rank_overlay"] = fold["market_rank"] - fold["calibrated_model_rank"]
        fold["validation_label"] = fold_name
        lomo_predictions.append(fold)
        ranking_rows.extend(_build_ranking_rows(fold, fold_name))

    lomo_frame = pd.concat(lomo_predictions, ignore_index=True) if lomo_predictions else holdout.copy()
    execution_report = _execution_report_rows(holdout, lomo_frame)
    ranking_report = pd.DataFrame(ranking_rows)
    calibration_report = pd.concat(calibration_rows, ignore_index=True) if calibration_rows else pd.DataFrame()

    holdout_rank_summary = ranking_report[
        (ranking_report["validation_label"] == "holdout_test")
        & (ranking_report["method_name"] != "market_baseline")
    ].sort_values(["top1_hit_delta_vs_market", "ndcg_delta_vs_market"], ascending=[False, False])
    best_ranking_row = holdout_rank_summary.iloc[0] if not holdout_rank_summary.empty else pd.Series(dtype=object)
    best_execution_row = execution_report.iloc[0] if not execution_report.empty else pd.Series(dtype=object)
    best_fundamental_metrics = next(row for row in holdout_metric_rows if row["model_name"] == best_fundamental_name)
    best_market_metrics = next(row for row in holdout_metric_rows if row["model_name"] == best_market_name)

    results_report = pd.DataFrame(
        [
            {
                "selected_fundamental_model": best_fundamental_name,
                "selected_market_model": best_market_name,
                "fundamental_brier_score": best_fundamental_metrics["brier_score"],
                "fundamental_log_loss": best_fundamental_metrics["log_loss"],
                "fundamental_overconfidence_gap": best_fundamental_metrics["overconfidence_gap"],
                "market_brier_score": best_market_metrics["brier_score"],
                "market_log_loss": best_market_metrics["log_loss"],
                "market_overconfidence_gap": best_market_metrics["overconfidence_gap"],
                "best_ranking_method": best_ranking_row.get("method_name"),
                "best_top1_delta_vs_market": best_ranking_row.get("top1_hit_delta_vs_market", 0.0),
                "best_ndcg_delta_vs_market": best_ranking_row.get("ndcg_delta_vs_market", 0.0),
                "best_execution_rule": best_execution_row.get("rule_name"),
                "best_execution_holdout_roi": best_execution_row.get("holdout_roi", 0.0),
                "best_execution_survives_robustness": best_execution_row.get("survives_robustness", False),
                "missing_feature_families": ",".join(
                    missing_feature_warnings[missing_feature_warnings["missing_count"] > 0]["feature_family"].tolist()
                ),
            }
        ]
    )

    best_payload = {
        "fundamental_model": best_fundamental_name,
        "market_model": best_market_name,
        "best_ranking_method": _row_payload(best_ranking_row) if not best_ranking_row.empty else {},
        "best_execution_rule": _row_payload(best_execution_row) if not best_execution_row.empty else {},
        "missing_feature_warnings": [_row_payload(row) for _, row in missing_feature_warnings.iterrows()],
    }

    if not skip_save:
        save_dataframe(results_report, RESULTS_PATH)
        save_dataframe(calibration_report, CALIBRATION_PATH)
        save_dataframe(ranking_report, RANKING_PATH)
        save_dataframe(execution_report, EXECUTION_PATH)
        json_dump(best_payload, BEST_MODEL_PATH)

    model_beats_market = bool(
        not holdout_rank_summary.empty
        and (
            float(holdout_rank_summary.iloc[0]["top1_hit_delta_vs_market"]) > 0
            or float(holdout_rank_summary.iloc[0]["ndcg_delta_vs_market"]) > 0
        )
    )
    calibrated_reliable = bool(
        float(best_fundamental_metrics["brier_score"]) < 0.09
        and abs(float(best_fundamental_metrics["overconfidence_gap"])) < 0.03
    )
    robust_execution = bool(not execution_report.empty and bool(execution_report.iloc[0]["survives_robustness"]))
    better_features_required = bool(not model_beats_market or not robust_execution or (missing_feature_warnings["missing_count"] > 0).any())

    print("Fundamental Market Residual Research Conclusions")
    print(f"1. Model beats market ranking? {model_beats_market}")
    if not best_ranking_row.empty:
        print(
            f"   Best ranking method: {best_ranking_row['method_name']} | "
            f"top1 delta={float(best_ranking_row['top1_hit_delta_vs_market']):.4f} | "
            f"ndcg delta={float(best_ranking_row['ndcg_delta_vs_market']):.4f}"
        )
    print(f"2. Calibrated probabilities reliable? {calibrated_reliable}")
    print(
        f"   Fundamental calibration: brier={best_fundamental_metrics['brier_score']:.4f} "
        f"log_loss={best_fundamental_metrics['log_loss']:.4f} "
        f"overconfidence_gap={best_fundamental_metrics['overconfidence_gap']:.4f}"
    )
    print(f"3. Any execution strategy survives robustness? {robust_execution}")
    if not best_execution_row.empty:
        print(
            f"   Best execution: {best_execution_row['rule_name']} | "
            f"holdout_bets={int(best_execution_row['holdout_bets'])} | "
            f"holdout_roi={float(best_execution_row['holdout_roi']):.4f} | "
            f"lomo_roi={float(best_execution_row['lomo_roi']):.4f}"
        )
    print(f"4. Better features still required? {better_features_required}")
    missing_families = missing_feature_warnings[missing_feature_warnings["missing_count"] > 0]["feature_family"].tolist()
    print("   Missing/weak feature families: " + (", ".join(missing_families) if missing_families else "none flagged"))

    return {
        "results": results_report,
        "calibration": calibration_report,
        "ranking": ranking_report,
        "execution": execution_report,
        "best_payload": best_payload,
        "missing_feature_warnings": missing_feature_warnings,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only Bolton-Chapman style fundamental vs market residual model."
    )
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    parser.add_argument("--odds-time-series-path", type=Path, default=ODDS_TIME_SERIES_PATH)
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--skip-save", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_research(
        matched_path=args.matched_path,
        odds_time_series_path=args.odds_time_series_path,
        train_ratio=args.train_ratio,
        skip_save=args.skip_save,
    )


if __name__ == "__main__":
    main()
