from __future__ import annotations

import argparse
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

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

BEST_CONSERVATIVE_PATH = RESEARCH_ARTIFACTS_DIR / "best_conservative_strategy.json"
BEST_BALANCED_PATH = RESEARCH_ARTIFACTS_DIR / "best_balanced_strategy.json"
BEST_AGGRESSIVE_PATH = RESEARCH_ARTIFACTS_DIR / "best_aggressive_strategy.json"

FORM_CONFIG_PATH = RESEARCH_ARTIFACTS_DIR / "best_form_score_config.json"

COMMISSION_RATE = 0.08
MIN_RUNNERS_PER_RACE = 3
MIN_BETS_FOR_STABILITY = 40

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
    valid = frame.copy()
    valid["finish_position"] = pd.to_numeric(valid["finish_position"], errors="coerce")
    valid = valid[valid["finish_position"].notna()].copy()
    if valid.empty:
        return {
            "top1_hit_rate": 0.0,
            "top3_hit_rate": 0.0,
            "avg_finish_top1": 0.0,
            "rank_correlation": 0.0,
            "market_top1_hit_rate": 0.0,
            "top1_hit_delta_vs_market": 0.0,
            "avg_finish_delta_vs_market": 0.0,
        }

    race_keys = ["race_date", "track_norm", "race_number"]
    top1 = valid[valid["strategy_rank"] == 1].copy()
    market_top1 = valid[valid["market_rank_current"] == 1].copy()

    top1_hit_rate = float(top1["won_flag"].mean()) if len(top1) else 0.0
    top3_hit_rate = float(
        valid.assign(top3_hit=(valid["strategy_rank"] <= 3) & (valid["won_flag"] == 1))
        .groupby(race_keys, dropna=False)["top3_hit"]
        .max()
        .mean()
    )
    avg_finish_top1 = float(top1["finish_position"].mean()) if len(top1) else 0.0
    market_top1_hit_rate = float(market_top1["won_flag"].mean()) if len(market_top1) else 0.0
    avg_finish_market = float(market_top1["finish_position"].mean()) if len(market_top1) else 0.0

    correlations: list[float] = []
    for _, race in valid.groupby(race_keys, dropna=False):
        if len(race) < MIN_RUNNERS_PER_RACE:
            continue
        corr = race["strategy_rank"].corr(race["finish_position"], method="spearman")
        if pd.notna(corr):
            correlations.append(float(-corr))

    return {
        "top1_hit_rate": top1_hit_rate,
        "top3_hit_rate": top3_hit_rate,
        "avg_finish_top1": avg_finish_top1,
        "rank_correlation": float(np.mean(correlations)) if correlations else 0.0,
        "market_top1_hit_rate": market_top1_hit_rate,
        "top1_hit_delta_vs_market": top1_hit_rate - market_top1_hit_rate,
        "avg_finish_delta_vs_market": avg_finish_market - avg_finish_top1,
    }


def score_strategy_result(
    train_summary: dict[str, float | int],
    test_summary: dict[str, float | int],
    walk_forward_test_rois: list[float],
    rank_summary: dict[str, float],
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
    train_test_gap = abs(train_roi - test_roi)

    penalty = 0.0
    warnings: list[str] = []

    if bets < MIN_BETS_FOR_STABILITY:
        penalty += 0.35
        warnings.append("tiny_sample")
    if avg_odds > 20:
        penalty += min((avg_odds - 20) / 40, 0.25)
        warnings.append("high_average_odds")
    if weekly_positive_rate < 0.45:
        penalty += 0.15
        warnings.append("weak_weekly_consistency")
    if weekly_roi_std > 0.75:
        penalty += 0.1
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
        penalty += min(train_test_gap, 0.2)
        warnings.append("train_test_gap")
    if walk_forward_min < -0.15:
        penalty += 0.08
        warnings.append("walk_forward_instability")

    robustness_score = (
        test_roi * 0.32
        + walk_forward_mean * 0.22
        + remove_best * 0.1
        + remove_top2 * 0.08
        + weekly_positive_rate * 0.1
        + float(test_summary["sharpe_like"]) * 0.08
        + rank_summary["top1_hit_delta_vs_market"] * 0.18
        + rank_summary["rank_correlation"] * 0.1
        - penalty
    )

    return {
        "robustness_score": robustness_score,
        "overfit_penalty": penalty,
        "walk_forward_mean_roi": walk_forward_mean,
        "walk_forward_min_roi": walk_forward_min,
        "train_test_gap": train_test_gap,
        "survives_remove_best": bool(remove_best > 0),
        "survives_remove_top2": bool(remove_top2 > 0),
        "survives_oos": bool(test_roi > 0 and train_test_gap < 0.12),
        "warning_flags": ",".join(warnings),
    }


def evaluate_strategy(
    frame: pd.DataFrame,
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    walk_forward_folds: list[tuple[str, pd.DataFrame, pd.DataFrame]],
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

    walk_forward_rows: list[dict[str, Any]] = []
    walk_forward_rois: list[float] = []
    for fold_name, _, fold_test in walk_forward_folds:
        scored_fold = scored_full.loc[fold_test.index].copy()
        fold_bets = select_strategy_bets(scored_fold, config)
        fold_summary = summarise_bets(fold_bets)
        walk_forward_rois.append(float(fold_summary["roi"]))
        walk_forward_rows.append(
            {
                "config_id": config.config_id,
                "fold_name": fold_name,
                "fold_bets": int(fold_summary["bets"]),
                "fold_roi": float(fold_summary["roi"]),
                "fold_drawdown": float(fold_summary["max_drawdown"]),
            }
        )

    score_summary = score_strategy_result(train_summary, test_summary, walk_forward_rois, rank_summary)
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
        "market_top1_hit_rate": rank_summary["market_top1_hit_rate"],
        "top1_hit_delta_vs_market": rank_summary["top1_hit_delta_vs_market"],
        "avg_finish_delta_vs_market": rank_summary["avg_finish_delta_vs_market"],
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
    race_keys = ["race_date", "track_norm", "race_number"]
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


def _config_json_payload(row: pd.Series) -> dict[str, Any]:
    payload = {field: row[field] for field in CONFIG_FIELD_NAMES if field in row.index}
    payload["summary"] = {
        "test_roi": float(row["test_roi"]),
        "robustness_score": float(row["robustness_score"]),
        "test_bets": int(row["test_bets"]),
        "weekly_positive_rate": float(row["weekly_positive_rate"]),
        "top1_hit_delta_vs_market": float(row["top1_hit_delta_vs_market"]),
        "warning_flags": row["warning_flags"],
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
) -> dict[str, Any]:
    frame = load_research_frame(matched_path)
    train_frame, test_frame = split_train_test(frame, train_ratio=train_ratio)
    walk_forward_folds = build_walk_forward_folds(frame)
    configs = generate_strategy_configs(max_configs=max_configs, seed=random_seed)

    results: list[dict[str, Any]] = []
    walk_forward_rows: list[pd.DataFrame] = []
    for index, config in enumerate(configs, start=1):
        result, fold_rows = evaluate_strategy(frame, train_frame, test_frame, walk_forward_folds, config)
        results.append(result)
        if not fold_rows.empty:
            walk_forward_rows.append(fold_rows)
        if index % 25 == 0 or index == len(configs):
            print(
                f"Evaluated {index}/{len(configs)} configs | "
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
    calibration_report = build_calibration_report(balanced_scored_test, "best_balanced")
    factor_importance = build_factor_importance(top_robust if not top_robust.empty else results_frame.head(10))
    segment_discovery = build_segment_discovery_report(balanced_bets)
    robustness_report = build_robustness_report(top_robust, walk_forward_frame)
    ranking_vs_market = build_ranking_vs_market_report(balanced_scored_test)
    ranking_metrics_report = pd.DataFrame(
        [
            {
                "strategy_label": "best_balanced",
                **ranking_metrics(balanced_scored_test),
                "test_bets": int(profile_picks["balanced"]["test_bets"]),
                "test_roi": float(profile_picks["balanced"]["test_roi"]),
            }
        ]
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
        json_dump(_config_json_payload(profile_picks["conservative"]), BEST_CONSERVATIVE_PATH)
        json_dump(_config_json_payload(profile_picks["balanced"]), BEST_BALANCED_PATH)
        json_dump(_config_json_payload(profile_picks["aggressive"]), BEST_AGGRESSIVE_PATH)

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
    top_segments = segment_discovery[segment_discovery["bets"] >= 15].head(10) if not segment_discovery.empty else pd.DataFrame()
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
                f"{row.segment_type}={row.segment_value} roi={row.roi:.3f} bets={int(row.bets)}"
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
        "profile_picks": profile_picks,
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
    )


if __name__ == "__main__":
    main()
