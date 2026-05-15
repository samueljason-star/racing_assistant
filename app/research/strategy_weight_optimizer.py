from __future__ import annotations

import argparse
import math
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from app.betting.market_helpers import commission_adjusted_market_probability
from app.research.form_score_optimizer import apply_form_formula, prepare_form_features
from app.research.utils import (
    RESEARCH_ARTIFACTS_DIR,
    RESEARCH_DATA_DIR,
    RESEARCH_REPORTS_DIR,
    attach_common_labels,
    compute_max_drawdown,
    estimate_runner_probabilities,
    json_dump,
    save_dataframe,
)

warnings.filterwarnings(
    "ignore",
    message="An input array is constant; the correlation coefficient is not defined.",
)

try:
    from lightgbm import LGBMRanker
except ImportError:
    LGBMRanker = None

try:
    from xgboost import XGBRanker
except ImportError:
    XGBRanker = None

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"

ALL_RESULTS_PATH = RESEARCH_REPORTS_DIR / "all_strategy_results.csv"
TOP_ROBUST_PATH = RESEARCH_REPORTS_DIR / "top_robust_strategies.csv"
UNSTABLE_HIGH_ROI_PATH = RESEARCH_REPORTS_DIR / "unstable_high_roi_strategies.csv"
CALIBRATION_REPORT_PATH = RESEARCH_REPORTS_DIR / "calibration_report.csv"
FACTOR_IMPORTANCE_PATH = RESEARCH_REPORTS_DIR / "factor_importance.csv"
SEGMENT_DISCOVERY_PATH = RESEARCH_REPORTS_DIR / "segment_discovery.csv"
ROBUSTNESS_CHECKS_PATH = RESEARCH_REPORTS_DIR / "robustness_checks.csv"
RANKING_METRICS_PATH = RESEARCH_REPORTS_DIR / "ranking_metrics.csv"
RANKING_VS_MARKET_PATH = RESEARCH_REPORTS_DIR / "ranking_vs_market.csv"
RANKING_QUALITY_REPORT_PATH = RESEARCH_REPORTS_DIR / "ranking_quality_report.csv"
STABLE_RANKING_SEGMENTS_PATH = RESEARCH_REPORTS_DIR / "stable_ranking_segments.csv"
UNSTABLE_SEGMENTS_PATH = RESEARCH_REPORTS_DIR / "unstable_segments.csv"
EXECUTION_STRATEGY_RESULTS_PATH = RESEARCH_REPORTS_DIR / "execution_strategy_results.csv"
RANKING_EXECUTION_TESTS_PATH = RESEARCH_REPORTS_DIR / "ranking_execution_tests.csv"
MISSING_FEATURE_WARNINGS_PATH = RESEARCH_REPORTS_DIR / "missing_feature_warnings.csv"
CHECKPOINT_RESULTS_PATH = RESEARCH_REPORTS_DIR / "all_strategy_results.checkpoint.csv"
CHECKPOINT_STATE_PATH = RESEARCH_ARTIFACTS_DIR / "strategy_weight_optimizer_checkpoint.json"
LIKELY_DEAD_END_BRANCHES_PATH = RESEARCH_REPORTS_DIR / "likely_dead_end_branches.txt"
PROMISING_UNPROVEN_BRANCHES_PATH = RESEARCH_REPORTS_DIR / "promising_but_unproven_branches.txt"
STRONGEST_RESEARCH_DIRECTIONS_PATH = RESEARCH_REPORTS_DIR / "strongest_remaining_research_directions.txt"

BEST_CONSERVATIVE_PATH = RESEARCH_ARTIFACTS_DIR / "best_conservative_strategy.json"
BEST_BALANCED_PATH = RESEARCH_ARTIFACTS_DIR / "best_balanced_strategy.json"
BEST_AGGRESSIVE_PATH = RESEARCH_ARTIFACTS_DIR / "best_aggressive_strategy.json"
BEST_RANKING_MODEL_PATH = RESEARCH_ARTIFACTS_DIR / "best_ranking_model.json"
BEST_EXECUTION_CANDIDATE_PATH = RESEARCH_ARTIFACTS_DIR / "best_execution_candidate.json"
BEST_CALIBRATED_MODEL_PATH = RESEARCH_ARTIFACTS_DIR / "best_calibrated_model.json"

FORM_CONFIG_PATH = RESEARCH_ARTIFACTS_DIR / "best_form_score_config.json"

COMMISSION_RATE = 0.08
MIN_RUNNERS_PER_RACE = 3
MIN_BETS_FOR_STABILITY = 40
MIN_SEGMENT_RACES = 25
RACE_KEYS = ["race_date", "track_norm", "race_number"]
MODEL_FEATURE_COLUMNS = [
    "form_signal",
    "market_signal",
    "movement_score",
    "edge_signal",
    "context_signal",
    "market_rank_signal",
    "odds_signal",
    "field_size",
]
EXPECTED_OPTIONAL_FEATURES = {
    "pace_tempo": ["pace_score", "tempo_score", "run_style", "settling_position"],
    "sectionals": ["sectional_strength", "last_600_rating", "speed_figure"],
    "race_shape": ["race_shape_score", "pace_pressure", "early_speed_pressure"],
    "track_bias": ["track_bias", "lane_bias", "inside_bias"],
    "trainer_jockey_interaction": ["trainer_jockey_combo", "trainer_jockey_win_rate"],
    "horse_latent_strength": ["horse_rating", "horse_consistency", "horse_volatility"],
}

GROUP_WEIGHT_FIELDS = [
    "form_weight",
    "market_weight",
    "movement_weight",
    "edge_weight",
    "context_weight",
]

INTERACTION_WEIGHT_FIELDS = [
    "interaction_form_movement",
    "interaction_movement_market_rank",
    "interaction_form_odds",
    "interaction_edge_movement",
]

CONFIG_FIELD_NAMES = list(StrategyConfig.__dataclass_fields__.keys()) if "StrategyConfig" in globals() else []


@dataclass(frozen=True)
class StrategyConfig:
    config_id: str
    form_weight: float
    market_weight: float
    movement_weight: float
    edge_weight: float
    context_weight: float
    interaction_form_movement: float
    interaction_movement_market_rank: float
    interaction_form_odds: float
    interaction_edge_movement: float
    odds_min: float
    odds_max: float
    market_rank_min: int
    market_rank_max: int
    movement_score_min: float
    form_score_min: float
    edge_min: float
    min_combined_score: float
    max_bets_per_day: int
    top_n_per_race: int
    selection_mode: str


CONFIG_FIELD_NAMES = list(StrategyConfig.__dataclass_fields__.keys())


def _default_form_config() -> dict[str, float]:
    if FORM_CONFIG_PATH.exists():
        import json

        return json.loads(FORM_CONFIG_PATH.read_text(encoding="utf-8"))
    return {
        "finish_weight": 2.0,
        "margin_weight": 1.0,
        "distance_weight": 0.5,
        "class_weight": 0.25,
        "barrier_weight": 0.5,
        "trainer_weight": 0.0,
        "jockey_weight": 0.0,
    }


def _safe_numeric(series: pd.Series | None, fill: float | None = None) -> pd.Series:
    if series is None:
        output = pd.Series(dtype=float)
    else:
        output = pd.to_numeric(series, errors="coerce")
    if fill is not None:
        output = output.fillna(fill)
    return output


def _first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _zscore(series: pd.Series, invert: bool = False, clip: float = 4.0) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    mean = numeric.mean()
    std = numeric.std(ddof=0)
    if pd.isna(std) or std == 0:
        values = pd.Series(0.0, index=numeric.index)
    else:
        values = (numeric - mean) / std
    values = values.clip(-clip, clip).fillna(0.0)
    return -values if invert else values


def _scaled_probability_from_odds(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").map(
        lambda value: commission_adjusted_market_probability(value, COMMISSION_RATE)
    )


def _current_price_column(frame: pd.DataFrame) -> str:
    column = _first_existing_column(
        frame,
        ["price_1m", "price_3m", "price_5m", "price_10m", "closing_price", "starting_price"],
    )
    if column is None:
        raise RuntimeError("No current odds column found in matched runner dataset.")
    return column


def _safe_log_loss(y_true: pd.Series, y_pred: pd.Series) -> float:
    truth = pd.to_numeric(y_true, errors="coerce").fillna(0).clip(0, 1)
    probs = pd.to_numeric(y_pred, errors="coerce").fillna(0).clip(1e-6, 1 - 1e-6)
    if truth.empty:
        return 0.0
    return float(-(truth * np.log(probs) + (1 - truth) * np.log(1 - probs)).mean())


def _bucket_field_size(size: float | int | None) -> str:
    if size is None or (isinstance(size, float) and math.isnan(size)):
        return "unknown"
    size = int(size)
    if size <= 7:
        return "small"
    if size <= 11:
        return "medium"
    return "large"


def _bucket_movement(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "unknown"
    if value >= 0.75:
        return "strong_shorten"
    if value >= 0.2:
        return "shorten"
    if value <= -0.75:
        return "strong_drift"
    if value <= -0.2:
        return "drift"
    return "flat"


def _bucket_form(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "unknown"
    if value >= 0.7:
        return "elite_form"
    if value >= 0.55:
        return "strong_form"
    if value >= 0.4:
        return "average_form"
    return "weak_form"


def _bucket_odds(value: float | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "unknown"
    if value < 4:
        return "short"
    if value < 8:
        return "mid"
    if value < 16:
        return "value"
    return "long"


def _checkpoint_state(results: pd.DataFrame, processed_configs: list[str]) -> dict[str, Any]:
    return {
        "processed_config_ids": processed_configs,
        "result_rows": int(len(results)),
    }


def _load_checkpoint(resume: bool) -> tuple[pd.DataFrame, set[str]]:
    if not resume or not CHECKPOINT_RESULTS_PATH.exists():
        return pd.DataFrame(), set()
    checkpoint_results = pd.read_csv(CHECKPOINT_RESULTS_PATH)
    processed = set(checkpoint_results["config_id"].astype(str).tolist()) if "config_id" in checkpoint_results.columns else set()
    return checkpoint_results, processed


def _save_checkpoint(results: pd.DataFrame, processed_configs: list[str]) -> None:
    if results.empty:
        return
    save_dataframe(results, CHECKPOINT_RESULTS_PATH)
    json_dump(_checkpoint_state(results, processed_configs), CHECKPOINT_STATE_PATH)


def _safe_text_write(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def collect_missing_feature_warnings(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for family, columns in EXPECTED_OPTIONAL_FEATURES.items():
        available = [column for column in columns if column in frame.columns]
        missing = [column for column in columns if column not in frame.columns]
        rows.append(
            {
                "feature_family": family,
                "available_count": len(available),
                "missing_count": len(missing),
                "available_columns": ",".join(available),
                "missing_columns": ",".join(missing),
                "warning": "missing_feature_family" if missing else "",
                "recommendation": (
                    "Collect these features to improve ranking edge."
                    if missing
                    else "Feature family available."
                ),
            }
        )
    return pd.DataFrame(rows)


def load_research_frame(matched_path: Path = MATCHED_PATH) -> pd.DataFrame:
    if not matched_path.exists():
        raise RuntimeError(f"Matched runner dataset not found: {matched_path}")

    raw = pd.read_csv(matched_path, low_memory=False)
    if raw.empty:
        raise RuntimeError("Matched runner dataset is empty.")

    prepared = prepare_form_features(raw)
    scored = apply_form_formula(prepared, _default_form_config())
    scored["form_score"] = pd.to_numeric(scored.get("form_score"), errors="coerce")
    form_default = float(scored["form_score"].median()) if scored["form_score"].notna().any() else 0.5
    scored["form_score"] = scored["form_score"].fillna(form_default)
    scored = attach_common_labels(scored)

    scored["race_day"] = pd.to_datetime(scored["race_date"], errors="coerce")
    scored = scored[scored["race_day"].notna()].copy()
    scored["race_month"] = scored["race_day"].dt.to_period("M").astype(str)
    iso_week = scored["race_day"].dt.isocalendar()
    scored["race_week"] = iso_week.year.astype(str) + "-W" + iso_week.week.astype(str).str.zfill(2)

    numeric_columns = [
        "barrier",
        "weight",
        "distance",
        "finish_position",
        "margin",
        "starting_price",
        "prize_money",
        "last_start_finish",
        "average_last_3_finish",
        "best_last_3_finish",
        "average_last_3_margin",
        "average_margin_last_3",
        "last_start_margin",
        "opening_price",
        "price_60m",
        "price_30m",
        "price_10m",
        "price_5m",
        "price_3m",
        "price_1m",
        "closing_price",
        "best_back_price",
        "best_lay_price",
        "total_matched",
        "market_rank",
        "days_since_last_start",
        "trainer_stat",
        "jockey_stat",
        "class_change",
        "distance_change",
    ]
    for column in numeric_columns:
        if column in scored.columns:
            scored[column] = pd.to_numeric(scored[column], errors="coerce")

    current_column = _current_price_column(scored)
    scored["current_price"] = pd.to_numeric(scored[current_column], errors="coerce")
    scored["current_price_source"] = current_column
    scored["implied_probability"] = _scaled_probability_from_odds(scored["current_price"])

    movement_pairs = {
        "open_to_current": "opening_price",
        "60_to_current": "price_60m",
        "30_to_current": "price_30m",
        "10_to_current": "price_10m",
        "5_to_current": "price_5m",
        "3_to_current": "price_3m",
        "1_to_current": "price_1m",
    }
    for output_column, source_column in movement_pairs.items():
        if source_column in scored.columns:
            scored[output_column] = pd.to_numeric(scored[source_column], errors="coerce") - scored["current_price"]
        else:
            scored[output_column] = np.nan

    scored["movement_score"] = (
        _zscore(scored["open_to_current"])
        + _zscore(scored["60_to_current"])
        + _zscore(scored["30_to_current"])
        + _zscore(scored["10_to_current"])
        + _zscore(scored["5_to_current"])
        + _zscore(scored["3_to_current"])
        + _zscore(scored["1_to_current"])
    ) / 7.0

    rank_source = pd.to_numeric(scored["current_price"], errors="coerce")
    scored["market_rank_current"] = (
        rank_source.groupby([scored["race_date"], scored["track_norm"], scored["race_number"]], dropna=False)
        .rank(method="dense", ascending=True)
    )
    scored["market_rank_bucket"] = scored["market_rank_current"].map(
        lambda value: "1-3"
        if pd.notna(value) and value <= 3
        else "4-6"
        if pd.notna(value) and value <= 6
        else "7-8"
        if pd.notna(value) and value <= 8
        else "outside"
        if pd.notna(value)
        else "unknown"
    )
    opening_for_rank = pd.to_numeric(scored.get("opening_price"), errors="coerce")
    scored["opening_market_rank"] = (
        opening_for_rank.groupby([scored["race_date"], scored["track_norm"], scored["race_number"]], dropna=False)
        .rank(method="dense", ascending=True)
    )
    scored["rank_delta_open_to_current"] = scored["opening_market_rank"] - scored["market_rank_current"]
    scored["field_size"] = scored.groupby(["race_date", "track_norm", "race_number"], dropna=False)["horse_name"].transform("size")
    scored["field_size_bucket"] = scored["field_size"].map(_bucket_field_size)

    scored = estimate_runner_probabilities(scored, "form_score")
    scored.rename(columns={"estimated_probability": "form_probability_model"}, inplace=True)
    scored["model_edge"] = scored["form_probability_model"] - scored["implied_probability"].fillna(0.0)
    scored["probability_disagreement"] = (scored["form_probability_model"] - scored["implied_probability"]).abs()
    scored["movement_market_disagreement"] = (
        scored["movement_score"] - _zscore(scored["market_rank_current"], invert=True)
    )

    scored["won_flag"] = pd.to_numeric(scored.get("won_flag"), errors="coerce").fillna(0).astype(int)
    scored["finish_position"] = pd.to_numeric(scored.get("finish_position"), errors="coerce")
    scored["form_regime"] = scored["form_score"].map(_bucket_form)
    scored["movement_regime"] = scored["movement_score"].map(_bucket_movement)
    scored["odds_regime"] = scored["current_price"].map(_bucket_odds)

    scored["region"] = (
        scored.get("state")
        if "state" in scored.columns
        else scored.get("region")
        if "region" in scored.columns
        else pd.Series(["unknown"] * len(scored), index=scored.index)
    )
    scored["region"] = scored["region"].fillna("unknown").astype(str)

    scored["race_class_group"] = scored.get("class_name", pd.Series(["unknown"] * len(scored), index=scored.index))
    scored["race_class_group"] = scored["race_class_group"].fillna("unknown").astype(str)
    scored["metro_provincial"] = (
        scored.get("metro_provincial")
        if "metro_provincial" in scored.columns
        else pd.Series(["unknown"] * len(scored), index=scored.index)
    )
    scored["metro_provincial"] = scored["metro_provincial"].fillna("unknown").astype(str)

    scored["odds_signal"] = (
        _zscore(scored["current_price"], invert=True)
        + _zscore(scored["implied_probability"])
        + _zscore(scored["rank_delta_open_to_current"])
    ) / 3.0
    scored["market_rank_signal"] = (
        _zscore(scored["market_rank_current"], invert=True)
        + _zscore(scored["opening_market_rank"], invert=True)
        + _zscore(scored["rank_delta_open_to_current"])
    ) / 3.0
    scored["market_signal"] = (scored["market_rank_signal"] + scored["odds_signal"]) / 2.0

    scored["form_signal"] = (
        _zscore(scored["form_score"])
        + _zscore(scored["best_last_3_finish"], invert=True)
        + _zscore(scored["average_last_3_finish"], invert=True)
        + _zscore(scored["average_margin_last_3"], invert=True)
        + _zscore(scored["last_start_finish"], invert=True)
    ) / 5.0

    scored["edge_signal"] = (
        _zscore(scored["model_edge"])
        + _zscore(scored["probability_disagreement"])
        + _zscore(scored["movement_market_disagreement"])
    ) / 3.0

    scored["context_signal"] = (
        _zscore(scored["field_size"], invert=True)
        + _zscore(scored["track_condition_match"])
        + _zscore(scored["similar_distance_flag"])
        + _zscore(scored["class_change"].abs(), invert=True)
    ) / 4.0

    return scored.sort_values(["race_day", "track_norm", "race_number", "horse_name"]).reset_index(drop=True)


def split_train_test(frame: pd.DataFrame, train_ratio: float = 0.7) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordered_dates = sorted(frame["race_day"].dropna().unique())
    if not ordered_dates:
        return frame.copy(), frame.copy()
    cutoff_idx = max(int(len(ordered_dates) * train_ratio) - 1, 0)
    cutoff = ordered_dates[cutoff_idx]
    train_frame = frame[frame["race_day"] <= cutoff].copy()
    test_frame = frame[frame["race_day"] > cutoff].copy()
    if test_frame.empty:
        test_frame = train_frame.copy()
    return train_frame, test_frame


def build_walk_forward_folds(frame: pd.DataFrame, max_folds: int = 6) -> list[tuple[str, pd.DataFrame, pd.DataFrame]]:
    periods = sorted(frame["race_month"].dropna().unique())
    if len(periods) < 6:
        return []

    train_start_idx = max(4, int(len(periods) * 0.45))
    candidate_indices = list(range(train_start_idx, len(periods) - 1))
    if len(candidate_indices) > max_folds:
        selected = np.linspace(0, len(candidate_indices) - 1, num=max_folds, dtype=int)
        candidate_indices = [candidate_indices[idx] for idx in selected]

    folds: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []
    for idx in candidate_indices:
        train_months = periods[: idx + 1]
        test_month = periods[idx + 1]
        train_slice = frame[frame["race_month"].isin(train_months)].copy()
        test_slice = frame[frame["race_month"] == test_month].copy()
        if train_slice.empty or test_slice.empty:
            continue
        folds.append((f"wf_{test_month}", train_slice, test_slice))
    return folds


def build_leave_one_month_out_folds(frame: pd.DataFrame) -> list[tuple[str, pd.DataFrame, pd.DataFrame]]:
    periods = sorted(frame["race_month"].dropna().unique())
    folds: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []
    for month in periods:
        train_slice = frame[frame["race_month"] != month].copy()
        test_slice = frame[frame["race_month"] == month].copy()
        if train_slice.empty or test_slice.empty:
            continue
        folds.append((f"lomo_{month}", train_slice, test_slice))
    return folds


def _assign_rank_from_score(frame: pd.DataFrame, score_column: str, rank_column: str, probability_column: str | None = None) -> pd.DataFrame:
    working = frame.copy()
    grouped_keys = [working["race_date"], working["track_norm"], working["race_number"]]
    grouped_score = working.groupby(["race_date", "track_norm", "race_number"], dropna=False)[score_column]
    working[rank_column] = grouped_score.rank(method="dense", ascending=False)
    if probability_column:
        exp_score = np.exp(pd.to_numeric(working[score_column], errors="coerce").fillna(0.0).clip(-6, 6))
        totals = exp_score.groupby(grouped_keys, dropna=False).transform("sum")
        working[probability_column] = (exp_score / totals.replace({0.0: np.nan})).fillna(0.0)
    return working


def _build_calibrated_model_frame(train_frame: pd.DataFrame, test_frame: pd.DataFrame) -> pd.DataFrame:
    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "model",
                CalibratedClassifierCV(
                    estimator=LogisticRegression(max_iter=1000, class_weight="balanced"),
                    method="isotonic",
                    cv=3,
                ),
            ),
        ]
    )
    x_train = train_frame[MODEL_FEATURE_COLUMNS].copy()
    y_train = pd.to_numeric(train_frame["won_flag"], errors="coerce").fillna(0).astype(int)
    x_test = test_frame[MODEL_FEATURE_COLUMNS].copy()

    if len(y_train.unique()) < 2:
        output = test_frame.copy()
        output["calibrated_model_score"] = 0.0
        output["calibrated_model_probability"] = 0.0
        output["calibrated_model_rank"] = output["market_rank_current"]
        return output

    model.fit(x_train, y_train)
    probabilities = model.predict_proba(x_test)[:, 1]
    output = test_frame.copy()
    output["calibrated_model_score"] = probabilities
    output = _assign_rank_from_score(
        output,
        "calibrated_model_score",
        "calibrated_model_rank",
        "calibrated_model_probability",
    )
    return output


def _dcg(relevances: list[float]) -> float:
    return sum(rel / math.log2(index + 2) for index, rel in enumerate(relevances))


def _ndcg_for_race(race: pd.DataFrame, rank_column: str, k: int = 3) -> float:
    ranked = race.sort_values(rank_column).head(k)
    actual = ranked["won_flag"].astype(float).tolist()
    ideal = sorted(race["won_flag"].astype(float).tolist(), reverse=True)[:k]
    actual_dcg = _dcg(actual)
    ideal_dcg = _dcg(ideal)
    if ideal_dcg <= 0:
        return 0.0
    return actual_dcg / ideal_dcg


def calculate_ranking_metrics(frame: pd.DataFrame, rank_column: str) -> dict[str, float]:
    valid = frame.copy()
    valid["finish_position"] = pd.to_numeric(valid["finish_position"], errors="coerce")
    valid = valid[valid["finish_position"].notna()].copy()
    if valid.empty or rank_column not in valid.columns:
        return {
            "evaluated_races": 0.0,
            "top1_hit_rate": 0.0,
            "top3_hit_rate": 0.0,
            "avg_finish_top1": 0.0,
            "rank_correlation": 0.0,
            "ndcg_at_3": 0.0,
            "market_top1_hit_rate": 0.0,
            "market_top3_hit_rate": 0.0,
            "market_avg_finish_top1": 0.0,
            "market_rank_correlation": 0.0,
            "market_ndcg_at_3": 0.0,
            "map_at_3": 0.0,
            "market_map_at_3": 0.0,
            "mrr": 0.0,
            "market_mrr": 0.0,
            "top1_hit_delta_vs_market": 0.0,
            "top3_hit_delta_vs_market": 0.0,
            "avg_finish_delta_vs_market": 0.0,
            "rank_correlation_delta_vs_market": 0.0,
            "ndcg_delta_vs_market": 0.0,
            "map_delta_vs_market": 0.0,
            "mrr_delta_vs_market": 0.0,
        }

    top1 = valid[valid[rank_column] == 1].copy()
    market_top1 = valid[valid["market_rank_current"] == 1].copy()
    top1_hit_rate = float(top1["won_flag"].mean()) if len(top1) else 0.0
    market_top1_hit_rate = float(market_top1["won_flag"].mean()) if len(market_top1) else 0.0
    avg_finish_top1 = float(top1["finish_position"].mean()) if len(top1) else 0.0
    market_avg_finish = float(market_top1["finish_position"].mean()) if len(market_top1) else 0.0

    top3_hits = []
    market_top3_hits = []
    correlations = []
    market_correlations = []
    ndcgs = []
    market_ndcgs = []
    average_precisions = []
    market_average_precisions = []
    reciprocal_ranks = []
    market_reciprocal_ranks = []
    evaluated_races = 0

    for _, race in valid.groupby(RACE_KEYS, dropna=False):
        if len(race) < MIN_RUNNERS_PER_RACE:
            continue
        evaluated_races += 1
        top3_hits.append(float(((race[rank_column] <= 3) & (race["won_flag"] == 1)).any()))
        market_top3_hits.append(float(((race["market_rank_current"] <= 3) & (race["won_flag"] == 1)).any()))
        corr = race[rank_column].corr(race["finish_position"], method="spearman")
        market_corr = race["market_rank_current"].corr(race["finish_position"], method="spearman")
        if pd.notna(corr):
            correlations.append(float(-corr))
        if pd.notna(market_corr):
            market_correlations.append(float(-market_corr))
        ndcgs.append(_ndcg_for_race(race, rank_column, k=3))
        market_ndcgs.append(_ndcg_for_race(race, "market_rank_current", k=3))
        ranked = race.sort_values(rank_column)
        market_ranked = race.sort_values("market_rank_current")
        ranked_top3 = ranked.head(3)
        market_top3 = market_ranked.head(3)
        ap = 0.0
        market_ap = 0.0
        if ranked_top3["won_flag"].any():
            first_winner_rank = ranked_top3.index[ranked_top3["won_flag"] == 1][0]
            ap = 1.0 / float(ranked_top3.index.get_loc(first_winner_rank) + 1)
        if market_top3["won_flag"].any():
            market_winner_rank = market_top3.index[market_top3["won_flag"] == 1][0]
            market_ap = 1.0 / float(market_top3.index.get_loc(market_winner_rank) + 1)
        average_precisions.append(ap)
        market_average_precisions.append(market_ap)
        if ranked["won_flag"].any():
            reciprocal_ranks.append(1.0 / float(ranked.index.get_loc(ranked.index[ranked["won_flag"] == 1][0]) + 1))
        if market_ranked["won_flag"].any():
            market_reciprocal_ranks.append(
                1.0 / float(market_ranked.index.get_loc(market_ranked.index[market_ranked["won_flag"] == 1][0]) + 1)
            )

    top3_hit_rate = float(np.mean(top3_hits)) if top3_hits else 0.0
    market_top3_hit_rate = float(np.mean(market_top3_hits)) if market_top3_hits else 0.0
    rank_correlation = float(np.mean(correlations)) if correlations else 0.0
    market_rank_correlation = float(np.mean(market_correlations)) if market_correlations else 0.0
    ndcg_at_3 = float(np.mean(ndcgs)) if ndcgs else 0.0
    market_ndcg_at_3 = float(np.mean(market_ndcgs)) if market_ndcgs else 0.0
    map_at_3 = float(np.mean(average_precisions)) if average_precisions else 0.0
    market_map_at_3 = float(np.mean(market_average_precisions)) if market_average_precisions else 0.0
    mrr = float(np.mean(reciprocal_ranks)) if reciprocal_ranks else 0.0
    market_mrr = float(np.mean(market_reciprocal_ranks)) if market_reciprocal_ranks else 0.0

    return {
        "evaluated_races": float(evaluated_races),
        "top1_hit_rate": top1_hit_rate,
        "top3_hit_rate": top3_hit_rate,
        "avg_finish_top1": avg_finish_top1,
        "rank_correlation": rank_correlation,
        "ndcg_at_3": ndcg_at_3,
        "market_top1_hit_rate": market_top1_hit_rate,
        "market_top3_hit_rate": market_top3_hit_rate,
        "market_avg_finish_top1": market_avg_finish,
        "market_rank_correlation": market_rank_correlation,
        "market_ndcg_at_3": market_ndcg_at_3,
        "map_at_3": map_at_3,
        "market_map_at_3": market_map_at_3,
        "mrr": mrr,
        "market_mrr": market_mrr,
        "top1_hit_delta_vs_market": top1_hit_rate - market_top1_hit_rate,
        "top3_hit_delta_vs_market": top3_hit_rate - market_top3_hit_rate,
        "avg_finish_delta_vs_market": market_avg_finish - avg_finish_top1,
        "rank_correlation_delta_vs_market": rank_correlation - market_rank_correlation,
        "ndcg_delta_vs_market": ndcg_at_3 - market_ndcg_at_3,
        "map_delta_vs_market": map_at_3 - market_map_at_3,
        "mrr_delta_vs_market": mrr - market_mrr,
    }


def _monthly_ranking_stability(frame: pd.DataFrame, rank_column: str) -> dict[str, float]:
    rows = []
    for month, subset in frame.groupby("race_month", dropna=False):
        metrics = calculate_ranking_metrics(subset, rank_column)
        metrics["race_month"] = month
        rows.append(metrics)
    monthly = pd.DataFrame(rows)
    if monthly.empty:
        return {
            "positive_month_share": 0.0,
            "monthly_top1_delta_std": 0.0,
            "monthly_ndcg_delta_std": 0.0,
        }
    return {
        "positive_month_share": float((monthly["top1_hit_delta_vs_market"] > 0).mean()),
        "monthly_top1_delta_std": float(monthly["top1_hit_delta_vs_market"].std(ddof=0) or 0.0),
        "monthly_ndcg_delta_std": float(monthly["ndcg_delta_vs_market"].std(ddof=0) or 0.0),
    }


def _relevance_target(frame: pd.DataFrame) -> pd.Series:
    finish = pd.to_numeric(frame["finish_position"], errors="coerce")
    relevance = np.where(
        finish == 1,
        3.0,
        np.where(finish == 2, 2.0, np.where(finish == 3, 1.0, 0.0)),
    )
    return pd.Series(relevance, index=frame.index, dtype=float)


def _fit_optional_ranker_frame(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    *,
    method_name: str,
) -> tuple[pd.DataFrame | None, str]:
    train_sorted = train_frame.sort_values(RACE_KEYS).copy()
    test_sorted = test_frame.sort_values(RACE_KEYS).copy()
    group_sizes = train_sorted.groupby(RACE_KEYS, dropna=False).size().astype(int).tolist()
    if not group_sizes:
        return None, f"{method_name}: no training races available"

    x_train = train_sorted[MODEL_FEATURE_COLUMNS].copy()
    x_test = test_sorted[MODEL_FEATURE_COLUMNS].copy()
    y_rank = _relevance_target(train_sorted)

    if method_name == "lightgbm_ranker":
        if LGBMRanker is None:
            return None, "lightgbm_ranker: lightgbm not installed"
        model = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "ranker",
                    LGBMRanker(
                        objective="lambdarank",
                        n_estimators=120,
                        learning_rate=0.05,
                        num_leaves=31,
                        min_child_samples=20,
                        random_state=42,
                    ),
                ),
            ]
        )
        model.fit(x_train, y_rank, ranker__group=group_sizes)
        scores = model.predict(x_test)
    elif method_name == "xgboost_ranker":
        if XGBRanker is None:
            return None, "xgboost_ranker: xgboost not installed"
        model = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "ranker",
                    XGBRanker(
                        objective="rank:pairwise",
                        n_estimators=120,
                        learning_rate=0.05,
                        max_depth=5,
                        subsample=0.9,
                        colsample_bytree=0.9,
                        random_state=42,
                    ),
                ),
            ]
        )
        model.fit(x_train, y_rank, ranker__group=group_sizes)
        scores = model.predict(x_test)
    else:
        return None, f"{method_name}: unsupported optional ranker"

    output = test_sorted.copy()
    output[f"{method_name}_score"] = scores
    output = _assign_rank_from_score(output, f"{method_name}_score", f"{method_name}_rank")
    return output, ""


def _build_pairwise_ranker_frame(train_frame: pd.DataFrame, test_frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[np.ndarray] = []
    labels: list[int] = []
    for _, race in train_frame.groupby(RACE_KEYS, dropna=False):
        race = race.sort_values("finish_position").copy()
        race = race[race["finish_position"].notna()].head(8)
        if len(race) < MIN_RUNNERS_PER_RACE:
            continue
        winner = race.iloc[0]
        losers = race.iloc[1:5]
        for _, loser in losers.iterrows():
            winner_vec = winner[MODEL_FEATURE_COLUMNS].to_numpy(dtype=float)
            loser_vec = loser[MODEL_FEATURE_COLUMNS].to_numpy(dtype=float)
            rows.append(winner_vec - loser_vec)
            labels.append(1)
            rows.append(loser_vec - winner_vec)
            labels.append(0)
    if not rows or len(set(labels)) < 2:
        output = test_frame.copy()
        output["pairwise_logistic_score"] = 0.0
        output["pairwise_logistic_probability"] = 0.0
        output["pairwise_logistic_rank"] = output["market_rank_current"]
        return output

    pairwise_x = pd.DataFrame(np.vstack(rows), columns=MODEL_FEATURE_COLUMNS)
    pairwise_y = pd.Series(labels)
    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )
    model.fit(pairwise_x, pairwise_y)
    test_scores = model.decision_function(test_frame[MODEL_FEATURE_COLUMNS].copy())
    output = test_frame.copy()
    output["pairwise_logistic_score"] = test_scores
    output = _assign_rank_from_score(
        output,
        "pairwise_logistic_score",
        "pairwise_logistic_rank",
        "pairwise_logistic_probability",
    )
    return output


def _build_listwise_relevance_frame(train_frame: pd.DataFrame, test_frame: pd.DataFrame) -> pd.DataFrame:
    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("model", HistGradientBoostingRegressor(max_depth=5, learning_rate=0.05, max_iter=200, random_state=42)),
        ]
    )
    x_train = train_frame[MODEL_FEATURE_COLUMNS].copy()
    y_train = _relevance_target(train_frame)
    x_test = test_frame[MODEL_FEATURE_COLUMNS].copy()
    if y_train.nunique() < 2:
        output = test_frame.copy()
        output["listwise_relevance_score"] = 0.0
        output["listwise_relevance_probability"] = 0.0
        output["listwise_relevance_rank"] = output["market_rank_current"]
        return output
    model.fit(x_train, y_train)
    scores = model.predict(x_test)
    output = test_frame.copy()
    output["listwise_relevance_score"] = scores
    output = _assign_rank_from_score(
        output,
        "listwise_relevance_score",
        "listwise_relevance_rank",
        "listwise_relevance_probability",
    )
    return output


def _build_research_model_frames(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    combined_config: StrategyConfig,
) -> tuple[dict[str, tuple[pd.DataFrame, str, str | None]], list[str]]:
    warnings_list: list[str] = []
    combined_test = apply_weighted_score(test_frame, combined_config)
    calibrated_test = _build_calibrated_model_frame(train_frame, test_frame)
    pairwise_test = _build_pairwise_ranker_frame(train_frame, test_frame)
    listwise_test = _build_listwise_relevance_frame(train_frame, test_frame)

    market_test = test_frame.copy()
    market_test["market_baseline_rank"] = market_test["market_rank_current"]
    form_test = _assign_rank_from_score(test_frame, "form_signal", "form_only_rank")
    movement_test = _assign_rank_from_score(test_frame, "movement_score", "movement_only_rank")

    method_frames: dict[str, tuple[pd.DataFrame, str, str | None]] = {
        "market_baseline": (market_test, "market_baseline_rank", "implied_probability"),
        "form_only": (form_test, "form_only_rank", None),
        "movement_only": (movement_test, "movement_only_rank", None),
        "combined_weighted": (combined_test, "strategy_rank", "strategy_probability"),
        "calibrated_model": (calibrated_test, "calibrated_model_rank", "calibrated_model_probability"),
        "pairwise_logistic": (pairwise_test, "pairwise_logistic_rank", "pairwise_logistic_probability"),
        "listwise_relevance": (listwise_test, "listwise_relevance_rank", "listwise_relevance_probability"),
    }

    for method_name in ["lightgbm_ranker", "xgboost_ranker"]:
        frame_or_none, warning_message = _fit_optional_ranker_frame(
            train_frame,
            test_frame,
            method_name=method_name,
        )
        if frame_or_none is not None:
            method_frames[method_name] = (frame_or_none, f"{method_name}_rank", None)
        elif warning_message:
            warnings_list.append(warning_message)
    return method_frames, warnings_list


def generate_strategy_configs(max_configs: int, seed: int) -> list[StrategyConfig]:
    rng = np.random.default_rng(seed)
    weight_options = np.array([-0.75, -0.25, 0.0, 0.25, 0.75, 1.25, 1.75])
    interaction_options = np.array([-0.75, -0.25, 0.0, 0.25, 0.75])

    odds_min_options = np.array([2.0, 3.0, 4.0, 6.0])
    odds_max_options = np.array([8.0, 12.0, 20.0, 35.0, 60.0])
    market_rank_max_options = np.array([1, 2, 3, 5, 8])
    movement_min_options = np.array([-1.0, -0.5, 0.0, 0.35, 0.7])
    form_min_options = np.array([0.2, 0.35, 0.5, 0.6, 0.7])
    edge_min_options = np.array([-0.03, 0.0, 0.02, 0.05, 0.08])
    combined_min_options = np.array([-1.0, -0.35, 0.0, 0.35, 0.75, 1.1])
    max_bets_day_options = np.array([1, 2, 3, 5, 8])
    top_n_options = np.array([1, 2, 3])
    selection_modes = np.array(["top_only", "multi_runner"])

    baselines = [
        StrategyConfig(
            config_id="baseline_balanced",
            form_weight=1.0,
            market_weight=0.75,
            movement_weight=0.5,
            edge_weight=1.0,
            context_weight=0.5,
            interaction_form_movement=0.25,
            interaction_movement_market_rank=0.25,
            interaction_form_odds=0.25,
            interaction_edge_movement=0.25,
            odds_min=3.0,
            odds_max=20.0,
            market_rank_min=1,
            market_rank_max=3,
            movement_score_min=0.0,
            form_score_min=0.45,
            edge_min=0.02,
            min_combined_score=0.0,
            max_bets_per_day=3,
            top_n_per_race=1,
            selection_mode="top_only",
        ),
        StrategyConfig(
            config_id="baseline_form_market",
            form_weight=1.25,
            market_weight=1.0,
            movement_weight=0.0,
            edge_weight=0.75,
            context_weight=0.5,
            interaction_form_movement=0.0,
            interaction_movement_market_rank=0.0,
            interaction_form_odds=0.25,
            interaction_edge_movement=0.0,
            odds_min=2.0,
            odds_max=12.0,
            market_rank_min=1,
            market_rank_max=5,
            movement_score_min=-1.0,
            form_score_min=0.5,
            edge_min=0.0,
            min_combined_score=-0.35,
            max_bets_per_day=5,
            top_n_per_race=2,
            selection_mode="multi_runner",
        ),
    ]

    seen: set[tuple[Any, ...]] = set()
    configs: list[StrategyConfig] = []
    for config in baselines:
        seen.add(tuple(asdict(config).values()))
        configs.append(config)

    while len(configs) < max_configs:
        odds_min = float(rng.choice(odds_min_options))
        odds_max = float(rng.choice(odds_max_options))
        if odds_max <= odds_min:
            continue

        top_n = int(rng.choice(top_n_options))
        selection_mode = str(rng.choice(selection_modes))
        if selection_mode == "top_only":
            top_n = 1

        config = StrategyConfig(
            config_id=f"cfg_{len(configs):05d}",
            form_weight=float(rng.choice(weight_options)),
            market_weight=float(rng.choice(weight_options)),
            movement_weight=float(rng.choice(weight_options)),
            edge_weight=float(rng.choice(weight_options)),
            context_weight=float(rng.choice(weight_options)),
            interaction_form_movement=float(rng.choice(interaction_options)),
            interaction_movement_market_rank=float(rng.choice(interaction_options)),
            interaction_form_odds=float(rng.choice(interaction_options)),
            interaction_edge_movement=float(rng.choice(interaction_options)),
            odds_min=odds_min,
            odds_max=odds_max,
            market_rank_min=1,
            market_rank_max=int(rng.choice(market_rank_max_options)),
            movement_score_min=float(rng.choice(movement_min_options)),
            form_score_min=float(rng.choice(form_min_options)),
            edge_min=float(rng.choice(edge_min_options)),
            min_combined_score=float(rng.choice(combined_min_options)),
            max_bets_per_day=int(rng.choice(max_bets_day_options)),
            top_n_per_race=top_n,
            selection_mode=selection_mode,
        )
        key = tuple(asdict(config).values())
        if key in seen:
            continue
        seen.add(key)
        configs.append(config)

    return configs[:max_configs]


def apply_weighted_score(frame: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    working = frame.copy()
    working["combined_score"] = (
        config.form_weight * working["form_signal"]
        + config.market_weight * working["market_signal"]
        + config.movement_weight * working["movement_score"]
        + config.edge_weight * working["edge_signal"]
        + config.context_weight * working["context_signal"]
        + config.interaction_form_movement * (working["form_signal"] * working["movement_score"])
        + config.interaction_movement_market_rank * (working["movement_score"] * working["market_rank_signal"])
        + config.interaction_form_odds * (working["form_signal"] * working["odds_signal"])
        + config.interaction_edge_movement * (working["edge_signal"] * working["movement_score"])
    )

    grouped = working.groupby(["race_date", "track_norm", "race_number"], dropna=False)["combined_score"]
    exp_score = np.exp(working["combined_score"].clip(-6, 6))
    score_total = exp_score.groupby([working["race_date"], working["track_norm"], working["race_number"]], dropna=False).transform("sum")
    working["strategy_probability"] = (exp_score / score_total.replace({0.0: np.nan})).fillna(0.0)
    working["strategy_rank"] = grouped.rank(method="dense", ascending=False)
    return working


def select_strategy_bets(frame: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    working = frame.copy()
    current_odds = pd.to_numeric(working["current_price"], errors="coerce")

    filters = (
        (current_odds >= config.odds_min)
        & (current_odds <= config.odds_max)
        & (working["market_rank_current"] >= config.market_rank_min)
        & (working["market_rank_current"] <= config.market_rank_max)
        & (working["movement_score"] >= config.movement_score_min)
        & (working["form_score"] >= config.form_score_min)
        & (working["model_edge"] >= config.edge_min)
        & (working["combined_score"] >= config.min_combined_score)
        & working["current_price"].notna()
    )
    working = working[filters].copy()
    if working.empty:
        return working

    if config.selection_mode == "top_only":
        working = working[working["strategy_rank"] == 1].copy()
    else:
        working = working[working["strategy_rank"] <= config.top_n_per_race].copy()

    if working.empty:
        return working

    working = working.sort_values(["race_day", "combined_score"], ascending=[True, False]).copy()
    working["daily_rank"] = working.groupby("race_date").cumcount() + 1
    working = working[working["daily_rank"] <= config.max_bets_per_day].copy()
    if working.empty:
        return working

    working["stake"] = 1.0
    working["profit_loss"] = np.where(
        working["won_flag"] == 1,
        (working["current_price"] - 1.0) * (1.0 - COMMISSION_RATE),
        -1.0,
    )
    working["strategy_config_id"] = config.config_id
    return working


def _weekly_metrics(bets: pd.DataFrame) -> tuple[float, float, float]:
    if bets.empty:
        return 0.0, 0.0, 0.0
    weekly = (
        bets.groupby("race_week", dropna=False)
        .agg(profit_loss=("profit_loss", "sum"), stake=("stake", "sum"))
        .reset_index()
    )
    weekly["roi"] = weekly["profit_loss"] / weekly["stake"].replace({0.0: np.nan})
    weekly_roi = weekly["roi"].fillna(0.0)
    positive_rate = float((weekly_roi > 0).mean()) if len(weekly_roi) else 0.0
    sharpe_like = float(weekly_roi.mean() / weekly_roi.std(ddof=0)) if len(weekly_roi) > 1 and weekly_roi.std(ddof=0) > 0 else 0.0
    return positive_rate, float(weekly_roi.std(ddof=0)) if len(weekly_roi) else 0.0, sharpe_like


def _concentration_metrics(bets: pd.DataFrame) -> tuple[float, float]:
    if bets.empty:
        return 0.0, 0.0
    track_concentration = float(bets["track_norm"].value_counts(normalize=True, dropna=False).max())
    month_concentration = float(bets["race_month"].value_counts(normalize=True, dropna=False).max())
    return track_concentration, month_concentration


def _remove_top_winner_roi(bets: pd.DataFrame, winners_to_remove: int) -> float:
    if bets.empty:
        return 0.0
    winners = bets[bets["profit_loss"] > 0].sort_values("profit_loss", ascending=False)
    if winners.empty:
        return float(bets["profit_loss"].sum() / bets["stake"].sum()) if bets["stake"].sum() else 0.0
    drop_index = winners.head(winners_to_remove).index
    remaining = bets.drop(index=drop_index)
    total_staked = remaining["stake"].sum()
    if total_staked <= 0:
        return 0.0
    return float(remaining["profit_loss"].sum() / total_staked)


def summarise_bets(bets: pd.DataFrame) -> dict[str, float | int]:
    if bets.empty:
        return {
            "bets": 0,
            "wins": 0,
            "strike_rate": 0.0,
            "roi": 0.0,
            "profit_loss": 0.0,
            "average_odds": 0.0,
            "average_edge": 0.0,
            "max_drawdown": 0.0,
            "weekly_positive_rate": 0.0,
            "weekly_roi_std": 0.0,
            "sharpe_like": 0.0,
            "track_concentration": 0.0,
            "month_concentration": 0.0,
            "remove_best_winner_roi": 0.0,
            "remove_top2_winners_roi": 0.0,
        }

    bets = bets.copy()
    bets["bank_after_bet"] = 1000.0 + bets["profit_loss"].cumsum()
    total_staked = float(bets["stake"].sum())
    weekly_positive_rate, weekly_roi_std, sharpe_like = _weekly_metrics(bets)
    track_concentration, month_concentration = _concentration_metrics(bets)

    return {
        "bets": int(len(bets)),
        "wins": int(bets["won_flag"].sum()),
        "strike_rate": float(bets["won_flag"].mean()),
        "roi": float(bets["profit_loss"].sum() / total_staked) if total_staked else 0.0,
        "profit_loss": float(bets["profit_loss"].sum()),
        "average_odds": float(bets["current_price"].mean()),
        "average_edge": float(bets["model_edge"].mean()),
        "max_drawdown": compute_max_drawdown([1000.0] + bets["bank_after_bet"].tolist()),
        "weekly_positive_rate": weekly_positive_rate,
        "weekly_roi_std": weekly_roi_std,
        "sharpe_like": sharpe_like,
        "track_concentration": track_concentration,
        "month_concentration": month_concentration,
        "remove_best_winner_roi": _remove_top_winner_roi(bets, 1),
        "remove_top2_winners_roi": _remove_top_winner_roi(bets, 2),
    }


def ranking_metrics(frame: pd.DataFrame) -> dict[str, float]:
    return calculate_ranking_metrics(frame, "strategy_rank")


def score_strategy_result(
    train_summary: dict[str, float | int],
    test_summary: dict[str, float | int],
    walk_forward_test_rois: list[float],
    rank_summary: dict[str, float],
    ranking_stability: dict[str, float],
    walk_forward_rank_deltas: list[float],
    leave_one_month_rank_deltas: list[float],
) -> dict[str, float | bool]:
    bets = int(test_summary["bets"])
    test_roi = float(test_summary["roi"])
    train_roi = float(train_summary["roi"])
    test_drawdown = float(test_summary["max_drawdown"])
    weekly_positive_rate = float(test_summary["weekly_positive_rate"])
    weekly_roi_std = float(test_summary["weekly_roi_std"])
    avg_odds = float(test_summary["average_odds"])
    track_concentration = float(test_summary["track_concentration"])
    month_concentration = float(test_summary["month_concentration"])
    remove_best = float(test_summary["remove_best_winner_roi"])
    remove_top2 = float(test_summary["remove_top2_winners_roi"])
    walk_forward_mean = float(np.mean(walk_forward_test_rois)) if walk_forward_test_rois else 0.0
    walk_forward_min = float(np.min(walk_forward_test_rois)) if walk_forward_test_rois else 0.0
    walk_forward_rank_mean = float(np.mean(walk_forward_rank_deltas)) if walk_forward_rank_deltas else 0.0
    walk_forward_rank_min = float(np.min(walk_forward_rank_deltas)) if walk_forward_rank_deltas else 0.0
    leave_one_month_mean = float(np.mean(leave_one_month_rank_deltas)) if leave_one_month_rank_deltas else 0.0
    leave_one_month_min = float(np.min(leave_one_month_rank_deltas)) if leave_one_month_rank_deltas else 0.0
    train_test_gap = abs(train_roi - test_roi)

    penalty = 0.0
    warnings: list[str] = []

    if float(rank_summary["evaluated_races"]) < MIN_SEGMENT_RACES:
        penalty += 0.25
        warnings.append("tiny_race_sample")
    if bets < MIN_BETS_FOR_STABILITY:
        penalty += 0.18
        warnings.append("tiny_sample")
    if avg_odds > 20:
        penalty += min((avg_odds - 20) / 40, 0.25)
        warnings.append("high_average_odds")
    if ranking_stability["positive_month_share"] < 0.5:
        penalty += 0.18
        warnings.append("weak_monthly_ranking_consistency")
    if ranking_stability["monthly_top1_delta_std"] > 0.08:
        penalty += 0.08
        warnings.append("unstable_monthly_top1_delta")
    if ranking_stability["monthly_ndcg_delta_std"] > 0.08:
        penalty += 0.08
        warnings.append("unstable_monthly_ndcg_delta")
    if weekly_positive_rate < 0.45:
        penalty += 0.08
        warnings.append("weak_weekly_betting_consistency")
    if weekly_roi_std > 0.75:
        penalty += 0.06
        warnings.append("high_weekly_variance")
    if track_concentration > 0.4:
        penalty += 0.12
        warnings.append("track_concentration")
    if month_concentration > 0.35:
        penalty += 0.1
        warnings.append("month_concentration")
    if test_drawdown > 0.35:
        penalty += min((test_drawdown - 0.35) * 0.7, 0.2)
        warnings.append("drawdown_risk")
    if remove_best < test_roi * 0.35:
        penalty += 0.18
        warnings.append("one_winner_dependence")
    if remove_top2 < 0:
        penalty += 0.12
        warnings.append("top2_winner_dependence")
    if train_test_gap > 0.12:
        penalty += min(train_test_gap * 0.5, 0.1)
        warnings.append("train_test_gap")
    if walk_forward_min < -0.15:
        penalty += 0.08
        warnings.append("walk_forward_betting_instability")
    if walk_forward_rank_min < -0.03:
        penalty += 0.12
        warnings.append("walk_forward_ranking_instability")
    if leave_one_month_min < -0.03:
        penalty += 0.12
        warnings.append("leave_one_month_ranking_instability")

    robustness_score = (
        rank_summary["top1_hit_delta_vs_market"] * 0.26
        + rank_summary["top3_hit_delta_vs_market"] * 0.14
        + rank_summary["ndcg_delta_vs_market"] * 0.18
        + rank_summary["rank_correlation_delta_vs_market"] * 0.1
        + rank_summary["avg_finish_delta_vs_market"] * 0.08
        + walk_forward_rank_mean * 0.12
        + leave_one_month_mean * 0.08
        + ranking_stability["positive_month_share"] * 0.1
        + test_roi * 0.06
        + remove_best * 0.04
        + remove_top2 * 0.02
        - penalty
    )

    return {
        "robustness_score": robustness_score,
        "overfit_penalty": penalty,
        "walk_forward_mean_roi": walk_forward_mean,
        "walk_forward_min_roi": walk_forward_min,
        "walk_forward_rank_delta_mean": walk_forward_rank_mean,
        "walk_forward_rank_delta_min": walk_forward_rank_min,
        "leave_one_month_rank_delta_mean": leave_one_month_mean,
        "leave_one_month_rank_delta_min": leave_one_month_min,
        "positive_month_share": ranking_stability["positive_month_share"],
        "monthly_top1_delta_std": ranking_stability["monthly_top1_delta_std"],
        "monthly_ndcg_delta_std": ranking_stability["monthly_ndcg_delta_std"],
        "train_test_gap": train_test_gap,
        "survives_remove_best": bool(remove_best > -0.05),
        "survives_remove_top2": bool(remove_top2 > -0.05),
        "survives_oos": bool(
            rank_summary["top1_hit_delta_vs_market"] > 0
            and rank_summary["ndcg_delta_vs_market"] > 0
            and walk_forward_rank_min > -0.03
        ),
        "warning_flags": ",".join(warnings),
    }


def evaluate_strategy(
    frame: pd.DataFrame,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    walk_forward_folds: list[tuple[str, pd.DataFrame, pd.DataFrame]],
    leave_one_month_folds: list[tuple[str, pd.DataFrame, pd.DataFrame]],
    config: StrategyConfig,
) -> tuple[dict[str, Any], pd.DataFrame]:
    scored_full = apply_weighted_score(frame, config)
    scored_train = scored_full.loc[train_frame.index].copy()
    scored_test = scored_full.loc[test_frame.index].copy()

    train_bets = select_strategy_bets(scored_train, config)
    test_bets = select_strategy_bets(scored_test, config)

    train_summary = summarise_bets(train_bets)
    test_summary = summarise_bets(test_bets)
    rank_summary = ranking_metrics(scored_test)
    ranking_stability = _monthly_ranking_stability(scored_test, "strategy_rank")

    walk_forward_rows: list[dict[str, Any]] = []
    walk_forward_rois: list[float] = []
    walk_forward_rank_deltas: list[float] = []
    for fold_name, _, fold_test in walk_forward_folds:
        scored_fold = scored_full.loc[fold_test.index].copy()
        fold_bets = select_strategy_bets(scored_fold, config)
        fold_summary = summarise_bets(fold_bets)
        fold_rank_summary = ranking_metrics(scored_fold)
        walk_forward_rois.append(float(fold_summary["roi"]))
        walk_forward_rank_deltas.append(float(fold_rank_summary["top1_hit_delta_vs_market"]))
        walk_forward_rows.append(
            {
                "config_id": config.config_id,
                "fold_name": fold_name,
                "fold_bets": int(fold_summary["bets"]),
                "fold_roi": float(fold_summary["roi"]),
                "fold_drawdown": float(fold_summary["max_drawdown"]),
                "fold_top1_delta_vs_market": float(fold_rank_summary["top1_hit_delta_vs_market"]),
                "fold_ndcg_delta_vs_market": float(fold_rank_summary["ndcg_delta_vs_market"]),
            }
        )

    leave_one_month_rank_deltas: list[float] = []
    for fold_name, _, fold_test in leave_one_month_folds:
        scored_fold = scored_full.loc[fold_test.index].copy()
        fold_rank_summary = ranking_metrics(scored_fold)
        leave_one_month_rank_deltas.append(float(fold_rank_summary["top1_hit_delta_vs_market"]))
        walk_forward_rows.append(
            {
                "config_id": config.config_id,
                "fold_name": fold_name,
                "fold_bets": 0,
                "fold_roi": 0.0,
                "fold_drawdown": 0.0,
                "fold_top1_delta_vs_market": float(fold_rank_summary["top1_hit_delta_vs_market"]),
                "fold_ndcg_delta_vs_market": float(fold_rank_summary["ndcg_delta_vs_market"]),
            }
        )

    score_summary = score_strategy_result(
        train_summary,
        test_summary,
        walk_forward_rois,
        rank_summary,
        ranking_stability,
        walk_forward_rank_deltas,
        leave_one_month_rank_deltas,
    )
    result = {
        **asdict(config),
        "train_bets": int(train_summary["bets"]),
        "train_roi": float(train_summary["roi"]),
        "train_drawdown": float(train_summary["max_drawdown"]),
        "test_bets": int(test_summary["bets"]),
        "test_wins": int(test_summary["wins"]),
        "test_roi": float(test_summary["roi"]),
        "test_profit_loss": float(test_summary["profit_loss"]),
        "test_strike_rate": float(test_summary["strike_rate"]),
        "test_average_odds": float(test_summary["average_odds"]),
        "test_average_edge": float(test_summary["average_edge"]),
        "test_drawdown": float(test_summary["max_drawdown"]),
        "weekly_positive_rate": float(test_summary["weekly_positive_rate"]),
        "weekly_roi_std": float(test_summary["weekly_roi_std"]),
        "sharpe_like": float(test_summary["sharpe_like"]),
        "track_concentration": float(test_summary["track_concentration"]),
        "month_concentration": float(test_summary["month_concentration"]),
        "remove_best_winner_roi": float(test_summary["remove_best_winner_roi"]),
        "remove_top2_winners_roi": float(test_summary["remove_top2_winners_roi"]),
        "top1_hit_rate": rank_summary["top1_hit_rate"],
        "top3_hit_rate": rank_summary["top3_hit_rate"],
        "avg_finish_top1": rank_summary["avg_finish_top1"],
        "rank_correlation": rank_summary["rank_correlation"],
        "ndcg_at_3": rank_summary["ndcg_at_3"],
        "market_top1_hit_rate": rank_summary["market_top1_hit_rate"],
        "market_top3_hit_rate": rank_summary["market_top3_hit_rate"],
        "market_rank_correlation": rank_summary["market_rank_correlation"],
        "market_ndcg_at_3": rank_summary["market_ndcg_at_3"],
        "top1_hit_delta_vs_market": rank_summary["top1_hit_delta_vs_market"],
        "top3_hit_delta_vs_market": rank_summary["top3_hit_delta_vs_market"],
        "avg_finish_delta_vs_market": rank_summary["avg_finish_delta_vs_market"],
        "rank_correlation_delta_vs_market": rank_summary["rank_correlation_delta_vs_market"],
        "ndcg_delta_vs_market": rank_summary["ndcg_delta_vs_market"],
        "evaluated_races": rank_summary["evaluated_races"],
        **score_summary,
    }
    return result, pd.DataFrame(walk_forward_rows)


def build_calibration_report(frame: pd.DataFrame, strategy_label: str) -> pd.DataFrame:
    working = frame.copy()
    working["bucket"] = pd.qcut(
        working["strategy_probability"].rank(method="first"),
        q=min(10, max(2, len(working) // 100)),
        duplicates="drop",
    )
    grouped = working.groupby("bucket", dropna=False)
    report = grouped.agg(
        count=("won_flag", "size"),
        mean_predicted_probability=("strategy_probability", "mean"),
        actual_win_rate=("won_flag", "mean"),
    ).reset_index()
    report["bucket"] = report["bucket"].astype(str)
    report["gap"] = report["mean_predicted_probability"] - report["actual_win_rate"]
    report["abs_gap"] = report["gap"].abs()
    report["brier_score"] = ((working["strategy_probability"] - working["won_flag"]) ** 2).mean()
    report["log_loss"] = _safe_log_loss(working["won_flag"], working["strategy_probability"])
    report["strategy_label"] = strategy_label
    return report


def build_factor_importance(top_results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for factor in GROUP_WEIGHT_FIELDS + INTERACTION_WEIGHT_FIELDS:
        series = pd.to_numeric(top_results[factor], errors="coerce").fillna(0.0)
        rows.append(
            {
                "factor": factor,
                "avg_weight": float(series.mean()),
                "avg_abs_weight": float(series.abs().mean()),
                "positive_weight_share": float((series > 0).mean()),
                "negative_weight_share": float((series < 0).mean()),
                "non_zero_share": float((series != 0).mean()),
            }
        )
    return pd.DataFrame(rows).sort_values("avg_abs_weight", ascending=False)


def build_segment_discovery_report(selected_bets: pd.DataFrame) -> pd.DataFrame:
    required = {"won_flag", "profit_loss", "current_price"}
    if selected_bets.empty or not required.issubset(selected_bets.columns):
        return pd.DataFrame(
            columns=[
                "segment_value",
                "bets",
                "wins",
                "profit_loss",
                "average_odds",
                "roi",
                "win_rate",
                "segment_type",
            ]
        )
    rows: list[dict[str, Any]] = []
    segment_columns = [
        "region",
        "metro_provincial",
        "race_class_group",
        "field_size_bucket",
        "odds_regime",
        "movement_regime",
        "form_regime",
    ]
    for column in segment_columns:
        grouped = selected_bets.groupby(column, dropna=False)
        summary = grouped.agg(
            bets=("won_flag", "size"),
            wins=("won_flag", "sum"),
            profit_loss=("profit_loss", "sum"),
            average_odds=("current_price", "mean"),
        ).reset_index()
        summary["roi"] = summary["profit_loss"] / summary["bets"].replace({0: np.nan})
        summary["win_rate"] = summary["wins"] / summary["bets"].replace({0: np.nan})
        summary["segment_type"] = column
        summary.rename(columns={column: "segment_value"}, inplace=True)
        rows.extend(summary.to_dict(orient="records"))
    report = pd.DataFrame(rows)
    if report.empty:
        return report
    return report.sort_values(["roi", "bets"], ascending=[False, False])


def build_ranking_vs_market_report(scored_test: pd.DataFrame) -> pd.DataFrame:
    overall = pd.DataFrame(
        [
            {
                "segment_type": "overall",
                "segment_value": "all",
                "model_top1_hit_rate": float(scored_test[scored_test["strategy_rank"] == 1]["won_flag"].mean()),
                "market_top1_hit_rate": float(scored_test[scored_test["market_rank_current"] == 1]["won_flag"].mean()),
            }
        ]
    )
    overall["delta_vs_market"] = overall["model_top1_hit_rate"] - overall["market_top1_hit_rate"]

    rows = overall.to_dict(orient="records")
    for segment_column in ["field_size_bucket", "odds_regime", "movement_regime"]:
        for value, subset in scored_test.groupby(segment_column, dropna=False):
            rows.append(
                {
                    "segment_type": segment_column,
                    "segment_value": value,
                    "model_top1_hit_rate": float(subset[subset["strategy_rank"] == 1]["won_flag"].mean()),
                    "market_top1_hit_rate": float(subset[subset["market_rank_current"] == 1]["won_flag"].mean()),
                    "delta_vs_market": float(
                        subset[subset["strategy_rank"] == 1]["won_flag"].mean()
                        - subset[subset["market_rank_current"] == 1]["won_flag"].mean()
                    ),
                }
            )
    return pd.DataFrame(rows)


def _monthly_execution_metrics(bets: pd.DataFrame) -> tuple[float, float]:
    if bets.empty:
        return 0.0, 0.0
    monthly = (
        bets.groupby("race_month", dropna=False)
        .agg(profit_loss=("profit_loss", "sum"), stake=("stake", "sum"))
        .reset_index()
    )
    monthly["roi"] = monthly["profit_loss"] / monthly["stake"].replace({0.0: np.nan})
    monthly_roi = monthly["roi"].fillna(0.0)
    return float((monthly_roi > 0).mean()), float(monthly_roi.std(ddof=0)) if len(monthly_roi) else 0.0


def _prepare_execution_bets(selection: pd.DataFrame) -> pd.DataFrame:
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


def _top_ranked_per_race(frame: pd.DataFrame, rank_column: str) -> pd.DataFrame:
    ordered = frame.sort_values([*RACE_KEYS, rank_column, "current_price"], ascending=[True, True, True, True, True])
    return ordered.groupby(RACE_KEYS, dropna=False).head(1).copy()


def _execution_rule_rows(
    method_frame: pd.DataFrame,
    *,
    method_name: str,
    rank_column: str,
    probability_column: str | None,
) -> list[dict[str, Any]]:
    top_ranked = _top_ranked_per_race(method_frame, rank_column)
    movement_median = float(method_frame["movement_score"].median()) if method_frame["movement_score"].notna().any() else 0.0
    form_median = float(method_frame["form_score"].median()) if method_frame["form_score"].notna().any() else 0.0
    execution_rules = {
        "A_top_1_model_ranked_per_race": lambda df: pd.Series(True, index=df.index),
        "B_top_1_model_ranked_if_odds_2_to_8": lambda df: df["current_price"].between(2.0, 8.0, inclusive="both"),
        "C_top_1_model_ranked_if_odds_3_to_10": lambda df: df["current_price"].between(3.0, 10.0, inclusive="both"),
        "D_top_1_model_ranked_if_market_rank_1_to_5": lambda df: df["market_rank_current"].between(1, 5, inclusive="both"),
        "E_top_1_model_ranked_if_model_improves_market_by_2_places": lambda df: (df["market_rank_current"] - df[rank_column]) >= 2,
        "F_top_1_model_ranked_if_movement_and_form_above_median": lambda df: (
            (df["movement_score"] >= movement_median) & (df["form_score"] >= form_median)
        ),
        "G_top_1_model_ranked_if_calibrated_prob_gt_market_prob_and_odds_3_to_8": lambda df: (
            (df[probability_column] > df["implied_probability"]) & df["current_price"].between(3.0, 8.0, inclusive="both")
        )
        if probability_column and probability_column in df.columns
        else pd.Series(False, index=df.index),
        "H_top_1_model_ranked_small_fields_only": lambda df: df["field_size_bucket"] == "small",
        "I_top_1_model_ranked_medium_fields_only": lambda df: df["field_size_bucket"] == "medium",
        "J_top_1_model_ranked_large_fields_only": lambda df: df["field_size_bucket"] == "large",
    }

    rows: list[dict[str, Any]] = []
    for rule_name, selector in execution_rules.items():
        selected = top_ranked[selector(top_ranked)].copy()
        bets = _prepare_execution_bets(selected)
        summary = summarise_bets(bets)
        monthly_positive_rate, monthly_roi_std = _monthly_execution_metrics(bets)
        rows.append(
            {
                "method_name": method_name,
                "execution_rule": rule_name,
                "bets": int(summary["bets"]),
                "wins": int(summary["wins"]),
                "losses": int(summary["bets"]) - int(summary["wins"]),
                "strike_rate": float(summary["strike_rate"]),
                "roi": float(summary["roi"]),
                "profit_loss": float(summary["profit_loss"]),
                "drawdown": float(summary["max_drawdown"]),
                "average_odds": float(summary["average_odds"]),
                "average_model_rank": float(selected[rank_column].mean()) if not selected.empty else 0.0,
                "average_market_rank": float(selected["market_rank_current"].mean()) if not selected.empty else 0.0,
                "remove_best_winner_roi": float(summary["remove_best_winner_roi"]),
                "remove_top2_winners_roi": float(summary["remove_top2_winners_roi"]),
                "weekly_positive_rate": float(summary["weekly_positive_rate"]),
                "weekly_roi_std": float(summary["weekly_roi_std"]),
                "monthly_positive_rate": monthly_positive_rate,
                "monthly_roi_std": monthly_roi_std,
                "survives_robustness": bool(
                    int(summary["bets"]) >= MIN_BETS_FOR_STABILITY
                    and float(summary["remove_top2_winners_roi"]) > -0.05
                    and monthly_positive_rate >= 0.45
                    and float(summary["max_drawdown"]) <= 0.4
                ),
            }
        )
    return rows


def build_execution_test_reports(
    method_frames: dict[str, tuple[pd.DataFrame, str, str | None]],
    ranking_vs_market: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    holdout_overall = ranking_vs_market[
        (ranking_vs_market["validation_label"] == "holdout_test")
        & (ranking_vs_market["segment_type"] == "overall")
    ].copy()
    ranking_lookup = holdout_overall.set_index("method_name") if not holdout_overall.empty else pd.DataFrame()

    for method_name, (method_frame, rank_column, probability_column) in method_frames.items():
        if method_name == "market_baseline":
            continue
        method_rows = _execution_rule_rows(
            method_frame,
            method_name=method_name,
            rank_column=rank_column,
            probability_column=probability_column,
        )
        for row in method_rows:
            if not ranking_lookup.empty and method_name in ranking_lookup.index:
                lookup = ranking_lookup.loc[method_name]
                row["top1_hit_delta_vs_market"] = float(lookup["top1_hit_delta_vs_market"])
                row["ndcg_delta_vs_market"] = float(lookup["ndcg_delta_vs_market"])
                row["ranking_gap_penalty"] = max(0.0, -float(lookup["top1_hit_delta_vs_market"])) + max(
                    0.0, -float(lookup["ndcg_delta_vs_market"])
                )
            else:
                row["top1_hit_delta_vs_market"] = 0.0
                row["ndcg_delta_vs_market"] = 0.0
                row["ranking_gap_penalty"] = 0.0
            row["execution_stability_score"] = (
                row["roi"] * 0.2
                + row["strike_rate"] * 0.15
                + row["weekly_positive_rate"] * 0.15
                + row["monthly_positive_rate"] * 0.15
                + row["remove_best_winner_roi"] * 0.15
                + row["remove_top2_winners_roi"] * 0.1
                + row["top1_hit_delta_vs_market"] * 0.1
                + row["ndcg_delta_vs_market"] * 0.1
                - row["drawdown"] * 0.2
                - row["ranking_gap_penalty"] * 0.25
            )
        rows.extend(method_rows)

    ranking_execution_tests = pd.DataFrame(rows)
    if ranking_execution_tests.empty:
        return ranking_execution_tests, ranking_execution_tests
    execution_strategy_results = ranking_execution_tests.sort_values(
        ["survives_robustness", "execution_stability_score", "bets", "roi"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    return ranking_execution_tests, execution_strategy_results


def _recommendation_texts(
    ranking_vs_market: pd.DataFrame,
    execution_strategy_results: pd.DataFrame,
    missing_feature_warnings: pd.DataFrame,
    model_warnings: list[str],
) -> tuple[list[str], list[str], list[str]]:
    likely_dead_end = ["Likely dead-end branches:"]
    promising = ["Promising but unproven branches:"]
    strongest = ["Strongest remaining research directions:"]

    holdout_overall = ranking_vs_market[
        (ranking_vs_market["validation_label"] == "holdout_test")
        & (ranking_vs_market["segment_type"] == "overall")
        & (ranking_vs_market["method_name"] != "market_baseline")
    ].copy()
    if not holdout_overall.empty:
        for row in holdout_overall.sort_values("top1_hit_delta_vs_market").head(3).itertuples(index=False):
            likely_dead_end.append(
                f"- {row.method_name}: top1 delta {row.top1_hit_delta_vs_market:.4f}, ndcg delta {row.ndcg_delta_vs_market:.4f}"
            )
        for row in holdout_overall.sort_values(
            ["top1_hit_delta_vs_market", "ndcg_delta_vs_market"], ascending=[False, False]
        ).head(3).itertuples(index=False):
            promising.append(
                f"- {row.method_name}: closest ranking challenger with top1 delta {row.top1_hit_delta_vs_market:.4f} and ndcg delta {row.ndcg_delta_vs_market:.4f}"
            )

    if not execution_strategy_results.empty:
        for row in execution_strategy_results.head(3).itertuples(index=False):
            strongest.append(
                f"- {row.method_name} / {row.execution_rule}: bets={int(row.bets)} roi={row.roi:.4f} robustness={row.survives_robustness}"
            )
    missing_families = missing_feature_warnings[missing_feature_warnings["missing_count"] > 0]["feature_family"].tolist()
    if missing_families:
        strongest.append(f"- Highest-value missing feature families: {', '.join(missing_families)}")
    if model_warnings:
        promising.append("- Optional external rankers unavailable in this environment: " + "; ".join(sorted(set(model_warnings))))
    return likely_dead_end, promising, strongest


def _evaluate_method_segments(
    frame: pd.DataFrame,
    method_name: str,
    rank_column: str,
    validation_label: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    overall = calculate_ranking_metrics(frame, rank_column)
    rows.append(
        {
            "method_name": method_name,
            "validation_label": validation_label,
            "segment_type": "overall",
            "segment_value": "all",
            **overall,
        }
    )
    for segment_column in [
        "odds_regime",
        "market_rank_bucket",
        "field_size_bucket",
        "region",
        "race_class_group",
    ]:
        for value, subset in frame.groupby(segment_column, dropna=False):
            metrics = calculate_ranking_metrics(subset, rank_column)
            rows.append(
                {
                    "method_name": method_name,
                    "validation_label": validation_label,
                    "segment_type": segment_column,
                    "segment_value": value,
                    **metrics,
                }
            )
    return pd.DataFrame(rows)


def build_baseline_ranking_reports(
    frame: pd.DataFrame,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    combined_config: StrategyConfig,
    walk_forward_folds: list[tuple[str, pd.DataFrame, pd.DataFrame]],
    leave_one_month_folds: list[tuple[str, pd.DataFrame, pd.DataFrame]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str], dict[str, tuple[pd.DataFrame, str, str | None]]]:
    method_frames, model_warnings = _build_research_model_frames(train_frame, test_frame, combined_config)
    calibrated_test = method_frames["calibrated_model"][0]

    ranking_quality_rows: list[pd.DataFrame] = []
    for method_name, (method_frame, rank_column, _) in method_frames.items():
        ranking_quality_rows.append(_evaluate_method_segments(method_frame, method_name, rank_column, "holdout_test"))

    for fold_name, fold_train, fold_test in walk_forward_folds:
        fold_method_frames, fold_warnings = _build_research_model_frames(fold_train, fold_test, combined_config)
        model_warnings.extend(fold_warnings)
        for method_name, (method_frame, rank_column, _) in fold_method_frames.items():
            ranking_quality_rows.append(_evaluate_method_segments(method_frame, method_name, rank_column, fold_name))

    for fold_name, fold_train, fold_test in leave_one_month_folds:
        fold_method_frames, fold_warnings = _build_research_model_frames(fold_train, fold_test, combined_config)
        model_warnings.extend(fold_warnings)
        for method_name, (method_frame, rank_column, _) in fold_method_frames.items():
            ranking_quality_rows.append(_evaluate_method_segments(method_frame, method_name, rank_column, fold_name))

    ranking_quality_report = pd.concat(ranking_quality_rows, ignore_index=True)
    ranking_vs_market = ranking_quality_report[
        [
            "method_name",
            "validation_label",
            "segment_type",
            "segment_value",
            "evaluated_races",
            "top1_hit_rate",
            "market_top1_hit_rate",
            "top1_hit_delta_vs_market",
            "top3_hit_rate",
            "market_top3_hit_rate",
            "top3_hit_delta_vs_market",
            "avg_finish_top1",
            "market_avg_finish_top1",
            "avg_finish_delta_vs_market",
            "rank_correlation",
            "market_rank_correlation",
            "rank_correlation_delta_vs_market",
            "ndcg_at_3",
            "market_ndcg_at_3",
            "ndcg_delta_vs_market",
            "map_at_3",
            "market_map_at_3",
            "map_delta_vs_market",
            "mrr",
            "market_mrr",
            "mrr_delta_vs_market",
        ]
    ].copy()

    holdout_segments = ranking_vs_market[
        (ranking_vs_market["validation_label"] == "holdout_test")
        & (ranking_vs_market["segment_type"] != "overall")
    ].copy()
    stable_ranking_segments = holdout_segments[
        (holdout_segments["evaluated_races"] >= MIN_SEGMENT_RACES)
        & (holdout_segments["top1_hit_delta_vs_market"] > 0)
        & (holdout_segments["ndcg_delta_vs_market"] > 0)
    ].sort_values(
        ["top1_hit_delta_vs_market", "ndcg_delta_vs_market", "evaluated_races"],
        ascending=[False, False, False],
    )
    unstable_segments = holdout_segments[
        (holdout_segments["evaluated_races"] >= MIN_SEGMENT_RACES)
        & (
            (holdout_segments["top1_hit_delta_vs_market"] < 0)
            | (holdout_segments["ndcg_delta_vs_market"] < 0)
        )
    ].sort_values(
        ["top1_hit_delta_vs_market", "ndcg_delta_vs_market", "evaluated_races"],
        ascending=[True, True, False],
    )
    return (
        ranking_quality_report,
        ranking_vs_market,
        stable_ranking_segments,
        unstable_segments,
        calibrated_test,
        sorted(set(model_warnings)),
        method_frames,
    )


def build_robustness_report(top_results: pd.DataFrame, all_walk_forward: pd.DataFrame) -> pd.DataFrame:
    if top_results.empty:
        return pd.DataFrame()
    merged = top_results[["config_id", "test_roi", "remove_best_winner_roi", "remove_top2_winners_roi", "survives_oos"]].copy()
    if all_walk_forward.empty:
        merged["walk_forward_worst_roi"] = 0.0
        merged["walk_forward_mean_roi"] = 0.0
        return merged
    fold_summary = (
        all_walk_forward.groupby("config_id", dropna=False)
        .agg(
            walk_forward_mean_roi=("fold_roi", "mean"),
            walk_forward_worst_roi=("fold_roi", "min"),
            walk_forward_best_roi=("fold_roi", "max"),
            walk_forward_bet_count=("fold_bets", "sum"),
        )
        .reset_index()
    )
    return merged.merge(fold_summary, on="config_id", how="left")


def choose_profile_strategies(results: pd.DataFrame) -> dict[str, pd.Series]:
    ordered = results.sort_values("robustness_score", ascending=False).copy()
    viable = ordered[ordered["test_bets"] > 0].copy()
    if viable.empty:
        viable = ordered.copy()

    conservative = viable[
        (viable["test_bets"] >= 80)
        & (viable["test_average_odds"] <= 14)
        & (viable["test_drawdown"] <= 0.28)
        & (viable["weekly_positive_rate"] >= 0.48)
    ]
    balanced = viable[
        (viable["test_bets"] >= 50)
        & (viable["test_drawdown"] <= 0.38)
        & (viable["survives_oos"])
    ]
    aggressive = viable[
        (viable["test_bets"] >= 30)
        & (viable["test_average_odds"] <= 30)
    ].sort_values(["test_roi", "robustness_score"], ascending=[False, False])

    picks = {
        "conservative": conservative.iloc[0] if not conservative.empty else viable.iloc[0],
        "balanced": balanced.iloc[0] if not balanced.empty else viable.iloc[0],
        "aggressive": aggressive.iloc[0] if not aggressive.empty else viable.iloc[0],
    }
    return picks


def _to_jsonable(value: Any) -> Any:
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


def _series_json_payload(row: pd.Series) -> dict[str, Any]:
    return {key: _to_jsonable(value) for key, value in row.to_dict().items()}


def _config_json_payload(row: pd.Series) -> dict[str, Any]:

    payload = {field: _to_jsonable(row[field]) for field in CONFIG_FIELD_NAMES if field in row.index}
    payload["summary"] = {
        "test_roi": _to_jsonable(row["test_roi"]),
        "robustness_score": _to_jsonable(row["robustness_score"]),
        "test_bets": _to_jsonable(row["test_bets"]),
        "weekly_positive_rate": _to_jsonable(row["weekly_positive_rate"]),
        "top1_hit_delta_vs_market": _to_jsonable(row["top1_hit_delta_vs_market"]),
        "warning_flags": _to_jsonable(row["warning_flags"]),
    }
    return payload


def _describe_factor_direction(factor_report: pd.DataFrame) -> tuple[list[str], list[str]]:
    helpful = factor_report[
        (factor_report["avg_abs_weight"] > 0.25) & (factor_report["avg_weight"] > 0.05)
    ]["factor"].tolist()
    harmful = factor_report[
        (factor_report["avg_abs_weight"] > 0.2) & (factor_report["avg_weight"] < -0.05)
    ]["factor"].tolist()
    return helpful, harmful


def run_optimizer(
    matched_path: Path = MATCHED_PATH,
    *,
    max_configs: int = 300,
    random_seed: int = 42,
    top_k_robustness: int = 25,
    train_ratio: float = 0.7,
    skip_save: bool = False,
    resume: bool = False,
    chunk_size: int = 25,
    checkpoint_every: int = 25,
    low_memory_mode: bool = False,
) -> dict[str, Any]:
    frame = load_research_frame(matched_path)
    missing_feature_warnings = collect_missing_feature_warnings(frame)
    train_frame, test_frame = split_train_test(frame, train_ratio=train_ratio)
    walk_forward_folds = build_walk_forward_folds(frame)
    leave_one_month_folds = build_leave_one_month_out_folds(frame)
    configs = generate_strategy_configs(max_configs=max_configs, seed=random_seed)
    checkpoint_results, processed_config_ids = _load_checkpoint(resume=resume)

    results: list[dict[str, Any]] = checkpoint_results.to_dict(orient="records") if not checkpoint_results.empty else []
    walk_forward_rows: list[pd.DataFrame] = []
    pending_configs = [config for config in configs if config.config_id not in processed_config_ids]

    for index, config in enumerate(pending_configs, start=1):
        result, fold_rows = evaluate_strategy(
            frame,
            train_frame,
            test_frame,
            walk_forward_folds,
            leave_one_month_folds,
            config,
        )
        results.append(result)
        processed_config_ids.add(config.config_id)
        if not fold_rows.empty and not low_memory_mode:
            walk_forward_rows.append(fold_rows)
        should_checkpoint = bool(checkpoint_every) and (
            index % checkpoint_every == 0 or index == len(pending_configs)
        )
        if should_checkpoint and not skip_save:
            checkpoint_frame = pd.DataFrame(results)
            _save_checkpoint(checkpoint_frame, sorted(processed_config_ids))
        if index % max(chunk_size, 1) == 0 or index == len(pending_configs):
            print(
                f"Evaluated {index}/{len(pending_configs)} new configs "
                f"({len(results)}/{len(configs)} total) | "
                f"current best robustness={max(r['robustness_score'] for r in results):.4f}"
            )

    results_frame = pd.DataFrame(results).sort_values(
        ["robustness_score", "test_roi", "test_bets"],
        ascending=[False, False, False],
    )
    walk_forward_frame = pd.concat(walk_forward_rows, ignore_index=True) if walk_forward_rows else pd.DataFrame()

    top_robust = results_frame[results_frame["test_bets"] > 0].head(top_k_robustness).copy()
    if top_robust.empty:
        top_robust = results_frame.head(top_k_robustness).copy()
    unstable_high_roi = results_frame[
        (results_frame["test_roi"] > 0.12)
        & ((results_frame["overfit_penalty"] >= 0.25) | (~results_frame["survives_remove_top2"]))
    ].copy()

    profile_picks = choose_profile_strategies(top_robust if not top_robust.empty else results_frame)

    best_balanced_config = StrategyConfig(**{field: profile_picks["balanced"][field] for field in asdict(configs[0]).keys()})
    balanced_scored_test = apply_weighted_score(test_frame, best_balanced_config)
    balanced_bets = select_strategy_bets(balanced_scored_test, best_balanced_config)
    (
        ranking_quality_report,
        ranking_vs_market,
        stable_ranking_segments,
        unstable_segments,
        calibrated_test,
        model_warnings,
        method_frames,
    ) = build_baseline_ranking_reports(
        frame,
        train_frame,
        test_frame,
        best_balanced_config,
        walk_forward_folds,
        leave_one_month_folds,
    )
    calibration_report = pd.concat(
        [
            build_calibration_report(balanced_scored_test, "combined_weighted"),
            build_calibration_report(
                calibrated_test.rename(
                    columns={"calibrated_model_probability": "strategy_probability"}
                ),
                "calibrated_model",
            ),
        ],
        ignore_index=True,
    )
    optional_probability_methods = []
    for method_name, (method_frame, _, probability_column) in method_frames.items():
        if method_name in {"market_baseline", "combined_weighted", "calibrated_model"}:
            continue
        if probability_column and probability_column in method_frame.columns:
            optional_probability_methods.append(
                build_calibration_report(
                    method_frame.rename(columns={probability_column: "strategy_probability"}),
                    method_name,
                )
            )
    if optional_probability_methods:
        calibration_report = pd.concat([calibration_report, *optional_probability_methods], ignore_index=True)
    factor_importance = build_factor_importance(top_robust if not top_robust.empty else results_frame.head(10))
    segment_discovery = build_segment_discovery_report(balanced_bets)
    robustness_report = build_robustness_report(top_robust, walk_forward_frame)
    ranking_metrics_report = ranking_vs_market[
        (ranking_vs_market["validation_label"] == "holdout_test")
        & (ranking_vs_market["segment_type"] == "overall")
    ].copy()
    execution_tests, execution_strategy_results = build_execution_test_reports(method_frames, ranking_vs_market)
    ranking_method_lookup = (
        ranking_metrics_report.set_index("method_name").sort_values(
            ["top1_hit_delta_vs_market", "ndcg_delta_vs_market"],
            ascending=[False, False],
        )
        if not ranking_metrics_report.empty
        else pd.DataFrame()
    )
    best_ranking_method = (
        ranking_method_lookup.head(1).reset_index().iloc[0]
        if not ranking_method_lookup.empty
        else pd.Series(dtype=object)
    )
    best_calibrated_method = (
        ranking_metrics_report[ranking_metrics_report["method_name"] == "calibrated_model"].iloc[0]
        if not ranking_metrics_report[ranking_metrics_report["method_name"] == "calibrated_model"].empty
        else pd.Series(dtype=object)
    )
    best_execution_candidate = (
        execution_strategy_results.iloc[0] if not execution_strategy_results.empty else pd.Series(dtype=object)
    )
    likely_dead_end_lines, promising_lines, strongest_lines = _recommendation_texts(
        ranking_vs_market,
        execution_strategy_results,
        missing_feature_warnings,
        model_warnings,
    )

    if not skip_save:
        save_dataframe(results_frame, ALL_RESULTS_PATH)
        save_dataframe(top_robust, TOP_ROBUST_PATH)
        save_dataframe(unstable_high_roi, UNSTABLE_HIGH_ROI_PATH)
        save_dataframe(calibration_report, CALIBRATION_REPORT_PATH)
        save_dataframe(factor_importance, FACTOR_IMPORTANCE_PATH)
        save_dataframe(segment_discovery, SEGMENT_DISCOVERY_PATH)
        save_dataframe(robustness_report, ROBUSTNESS_CHECKS_PATH)
        save_dataframe(ranking_metrics_report, RANKING_METRICS_PATH)
        save_dataframe(ranking_vs_market, RANKING_VS_MARKET_PATH)
        save_dataframe(ranking_quality_report, RANKING_QUALITY_REPORT_PATH)
        save_dataframe(stable_ranking_segments, STABLE_RANKING_SEGMENTS_PATH)
        save_dataframe(unstable_segments, UNSTABLE_SEGMENTS_PATH)
        save_dataframe(execution_strategy_results, EXECUTION_STRATEGY_RESULTS_PATH)
        save_dataframe(execution_tests, RANKING_EXECUTION_TESTS_PATH)
        save_dataframe(missing_feature_warnings, MISSING_FEATURE_WARNINGS_PATH)
        json_dump(_config_json_payload(profile_picks["conservative"]), BEST_CONSERVATIVE_PATH)
        json_dump(_config_json_payload(profile_picks["balanced"]), BEST_BALANCED_PATH)
        json_dump(_config_json_payload(profile_picks["aggressive"]), BEST_AGGRESSIVE_PATH)
        if not best_ranking_method.empty:
            json_dump(_series_json_payload(best_ranking_method), BEST_RANKING_MODEL_PATH)
        if not best_calibrated_method.empty:
            json_dump(_series_json_payload(best_calibrated_method), BEST_CALIBRATED_MODEL_PATH)
        if not best_execution_candidate.empty:
            json_dump(_series_json_payload(best_execution_candidate), BEST_EXECUTION_CANDIDATE_PATH)
        _safe_text_write(LIKELY_DEAD_END_BRANCHES_PATH, likely_dead_end_lines)
        _safe_text_write(PROMISING_UNPROVEN_BRANCHES_PATH, promising_lines)
        _safe_text_write(STRONGEST_RESEARCH_DIRECTIONS_PATH, strongest_lines)

    helpful_factors, harmful_factors = _describe_factor_direction(factor_importance)
    best_stable = profile_picks["balanced"]
    best_unstable = unstable_high_roi.sort_values("test_roi", ascending=False).iloc[0] if not unstable_high_roi.empty else results_frame.iloc[0]

    movement_only_candidates = results_frame[
        (results_frame["movement_weight"] > 0.5)
        & (results_frame["form_weight"] <= 0.25)
        & (results_frame["edge_weight"] <= 0.25)
    ]
    movement_plus_form_candidates = results_frame[
        (results_frame["movement_weight"] > 0.25)
        & (results_frame["form_weight"] > 0.25)
    ]
    edge_contribution = results_frame["edge_weight"].corr(results_frame["robustness_score"]) if len(results_frame) > 1 else 0.0
    market_rank_contribution = results_frame["market_weight"].corr(results_frame["robustness_score"]) if len(results_frame) > 1 else 0.0
    edge_contribution = float(edge_contribution) if pd.notna(edge_contribution) else 0.0
    market_rank_contribution = float(market_rank_contribution) if pd.notna(market_rank_contribution) else 0.0
    top_segments = stable_ranking_segments.head(10) if not stable_ranking_segments.empty else pd.DataFrame()
    calibration_gap = float(calibration_report["abs_gap"].mean()) if not calibration_report.empty else 0.0
    calibration_warning = calibration_gap > 0.07

    print()
    print("Research Conclusions")
    print(
        f"1. Best stable strategy: {best_stable['config_id']} | "
        f"test_roi={best_stable['test_roi']:.4f} bets={int(best_stable['test_bets'])} "
        f"robustness={best_stable['robustness_score']:.4f}"
    )
    print(
        f"2. Best unstable/high-risk strategy: {best_unstable['config_id']} | "
        f"test_roi={best_unstable['test_roi']:.4f} bets={int(best_unstable['test_bets'])} "
        f"warnings={best_unstable['warning_flags']}"
    )
    print(f"3. Factors that mattered most: {', '.join(helpful_factors) if helpful_factors else 'no dominant factor family'}")
    print(f"4. Factors that consistently hurt: {', '.join(harmful_factors) if harmful_factors else 'no persistently harmful factor family detected'}")
    print(
        "5. Movement alone works? "
        + (
            f"no robust evidence (best movement-only robustness={movement_only_candidates['robustness_score'].max():.4f})"
            if not movement_only_candidates.empty
            else "no movement-only candidate cleared the robustness screen"
        )
    )
    print(
        "6. Movement + form works? "
        + (
            f"best combined robustness={movement_plus_form_candidates['robustness_score'].max():.4f}"
            if not movement_plus_form_candidates.empty
            else "no strong movement+form candidate found"
        )
    )
    print(f"7. Edge contributes positively? {'yes' if edge_contribution > 0 else 'not consistently'} (corr={edge_contribution:.4f})")
    print(f"8. Market rank matters? {'yes' if market_rank_contribution > 0 else 'not consistently'} (corr={market_rank_contribution:.4f})")
    print(
        f"9. Ranking signal exists even if probabilities are poor? "
        f"top1 delta vs market={best_stable['top1_hit_delta_vs_market']:.4f}, "
        f"rank_corr={best_stable['rank_correlation']:.4f}"
    )
    print(
        "10. Regions/race types behaving differently: "
        + (
            "; ".join(
                f"{row.segment_type}={row.segment_value} top1_delta={row.top1_hit_delta_vs_market:.3f} "
                f"ndcg_delta={row.ndcg_delta_vs_market:.3f} races={int(row.evaluated_races)}"
                for row in top_segments.itertuples(index=False)
            )
            if not top_segments.empty
            else "no segment cleared the reporting threshold yet"
        )
    )
    print(
        f"11. Survives OOS / leave-one-week-out proxy / remove-top-winner? "
        f"{bool(best_stable['survives_oos'])} / "
        f"{float(best_stable['walk_forward_min_roi']) > -0.05} / "
        f"{bool(best_stable['survives_remove_best']) and bool(best_stable['survives_remove_top2'])}"
    )
    if calibration_warning:
        print(
            f"WARNING: Model probabilities still look poorly calibrated (mean abs bucket gap={calibration_gap:.4f})."
        )
    if model_warnings:
        print("Optional ranker availability warnings: " + "; ".join(sorted(set(model_warnings))))
    if not execution_strategy_results.empty:
        top_execution = execution_strategy_results.iloc[0]
        print(
            f"12. Best execution test: {top_execution['method_name']} / {top_execution['execution_rule']} | "
            f"bets={int(top_execution['bets'])} roi={top_execution['roi']:.4f} "
            f"robust={bool(top_execution['survives_robustness'])}"
        )
    print("Files created:" if not skip_save else "Files this run would create (skip-save enabled):")
    for path in [
        ALL_RESULTS_PATH,
        TOP_ROBUST_PATH,
        RANKING_METRICS_PATH,
        CALIBRATION_REPORT_PATH,
        RANKING_VS_MARKET_PATH,
        RANKING_QUALITY_REPORT_PATH,
        EXECUTION_STRATEGY_RESULTS_PATH,
        RANKING_EXECUTION_TESTS_PATH,
        STABLE_RANKING_SEGMENTS_PATH,
        UNSTABLE_SEGMENTS_PATH,
        MISSING_FEATURE_WARNINGS_PATH,
        LIKELY_DEAD_END_BRANCHES_PATH,
        PROMISING_UNPROVEN_BRANCHES_PATH,
        STRONGEST_RESEARCH_DIRECTIONS_PATH,
        BEST_RANKING_MODEL_PATH,
        BEST_EXECUTION_CANDIDATE_PATH,
        BEST_CALIBRATED_MODEL_PATH,
    ]:
        print(f"- {path}")
    print(
        "Exact rerun command: "
        "python -m app.research.strategy_weight_optimizer --max-configs "
        f"{max_configs} --chunk-size {chunk_size} --checkpoint-every {checkpoint_every}"
    )
    estimated_minutes = max(20, int(max_configs * 0.8))
    print(f"Estimated runtime: roughly {estimated_minutes // 60}h {estimated_minutes % 60}m at current config count.")
    print(
        "Inspect first: ranking_vs_market.csv, ranking_metrics.csv, execution_strategy_results.csv, "
        "stable_ranking_segments.csv, unstable_segments.csv, calibration_report.csv"
    )
    print(
        "Interpretation guide: positive top1/ndcg deltas imply better ranking than market; "
        "stable segments need enough races plus positive deltas; execution candidates still need "
        "bets, drawdown, and winner-removal robustness to count as real."
    )

    return {
        "results": results_frame,
        "top_robust": top_robust,
        "unstable_high_roi": unstable_high_roi,
        "calibration_report": calibration_report,
        "factor_importance": factor_importance,
        "segment_discovery": segment_discovery,
        "robustness_checks": robustness_report,
        "ranking_metrics": ranking_metrics_report,
        "ranking_vs_market": ranking_vs_market,
        "ranking_quality_report": ranking_quality_report,
        "stable_ranking_segments": stable_ranking_segments,
        "unstable_segments": unstable_segments,
        "profile_picks": profile_picks,
        "execution_strategy_results": execution_strategy_results,
        "ranking_execution_tests": execution_tests,
        "missing_feature_warnings": missing_feature_warnings,
        "best_ranking_method": best_ranking_method,
        "best_execution_candidate": best_execution_candidate,
        "best_calibrated_method": best_calibrated_method,
        "model_warnings": model_warnings,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only multi-factor strategy optimizer and ranking framework."
    )
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    parser.add_argument("--max-configs", type=int, default=300)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--top-k-robustness", type=int, default=25)
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--chunk-size", type=int, default=25)
    parser.add_argument("--checkpoint-every", type=int, default=25)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--low-memory-mode", action="store_true")
    parser.add_argument(
        "--skip-save",
        action="store_true",
        help="Run the optimizer without overwriting report or artifact files.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_optimizer(
        matched_path=args.matched_path,
        max_configs=args.max_configs,
        random_seed=args.random_seed,
        top_k_robustness=args.top_k_robustness,
        train_ratio=args.train_ratio,
        skip_save=args.skip_save,
        resume=args.resume,
        chunk_size=args.chunk_size,
        checkpoint_every=args.checkpoint_every,
        low_memory_mode=args.low_memory_mode,
    )


if __name__ == "__main__":
    main()
