from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error

from app.research.labs.common import (
    _safe_auc,
    _safe_divide,
    _safe_numeric_series,
    _safe_qbucket,
    _string_series,
    evaluate_models_on_split,
    ex_ante_feature_columns,
)
from app.research.market_residual_model import _split_train_test

MARKET_STATE_COLUMNS = {
    "anchor_price",
    "anchor_market_prob_norm",
    "market_rank_current",
    "market_signal",
    "odds_signal",
    "current_price",
    "price_60m",
    "movement_open_to_60",
    "sixty_market_rank",
    "sixty_market_signal",
    "sixty_odds_signal",
    "market_rank_bucket",
    "odds_bucket",
}


@dataclass(frozen=True)
class ExecutionScenario:
    name: str
    move_capture: float
    spread_multiplier: float
    fill_fraction: float
    stale_penalty: float


DEFAULT_SCENARIOS = [
    ExecutionScenario("ideal_fill", move_capture=0.00, spread_multiplier=0.00, fill_fraction=1.00, stale_penalty=0.00),
    ExecutionScenario("mild_delay", move_capture=0.25, spread_multiplier=0.25, fill_fraction=1.00, stale_penalty=0.00),
    ExecutionScenario("moderate_delay", move_capture=0.50, spread_multiplier=0.50, fill_fraction=0.90, stale_penalty=0.01),
    ExecutionScenario("heavy_delay", move_capture=0.75, spread_multiplier=0.75, fill_fraction=0.75, stale_penalty=0.02),
    ExecutionScenario("partial_fill", move_capture=0.50, spread_multiplier=0.50, fill_fraction=0.55, stale_penalty=0.01),
    ExecutionScenario("adverse_fill", move_capture=1.00, spread_multiplier=1.00, fill_fraction=0.40, stale_penalty=0.03),
]


def scenario_grid() -> list[ExecutionScenario]:
    return list(DEFAULT_SCENARIOS) + [
        ExecutionScenario("partial_fill_75", move_capture=0.50, spread_multiplier=0.50, fill_fraction=0.75, stale_penalty=0.01),
        ExecutionScenario("partial_fill_50", move_capture=0.50, spread_multiplier=0.50, fill_fraction=0.50, stale_penalty=0.01),
        ExecutionScenario("partial_fill_25", move_capture=0.50, spread_multiplier=0.50, fill_fraction=0.25, stale_penalty=0.01),
        ExecutionScenario("worse_price_by_1_tick", move_capture=0.50, spread_multiplier=0.60, fill_fraction=0.90, stale_penalty=0.02),
        ExecutionScenario("worse_price_by_2_ticks", move_capture=0.50, spread_multiplier=0.70, fill_fraction=0.90, stale_penalty=0.04),
        ExecutionScenario("worse_price_by_5_ticks", move_capture=0.50, spread_multiplier=1.00, fill_fraction=0.90, stale_penalty=0.10),
    ]


def realistic_non_market_features(frame: pd.DataFrame) -> tuple[list[str], pd.DataFrame]:
    features, audit = ex_ante_feature_columns(frame, include_price_60=True, include_open_movement=True)
    return [feature for feature in features if feature not in MARKET_STATE_COLUMNS], audit


def selection_masks(predictions: pd.DataFrame, *, include_post_value: bool = False) -> dict[str, pd.Series]:
    if predictions.empty:
        return {}
    quantile_80 = float(predictions["predicted_shorten_probability"].quantile(0.80))
    quantile_90 = float(predictions["predicted_shorten_probability"].quantile(0.90))
    per_race_rank = predictions.groupby(["race_date", "track_norm", "race_number"], observed=False)["predicted_shorten_probability"].rank(
        method="first", ascending=False
    )
    masks = {
        "top_decile": predictions["predicted_shorten_probability"] >= quantile_90,
        "top_quintile": predictions["predicted_shorten_probability"] >= quantile_80,
        "per_race_top_1": per_race_rank.eq(1),
    }
    if include_post_value and "post_shortening_edge" in predictions.columns:
        post_value_rank = predictions.groupby(["race_date", "track_norm", "race_number"], observed=False)["post_shortening_edge"].rank(
            method="first", ascending=False
        )
        masks["post_value_top_1"] = post_value_rank.eq(1)
    return masks


def ex_ante_candidate_masks(predictions: pd.DataFrame) -> dict[str, pd.Series]:
    if predictions.empty:
        return {}
    top_decile = float(predictions["predicted_shorten_probability"].quantile(0.90))
    top_quintile = float(predictions["predicted_shorten_probability"].quantile(0.80))
    controlled_floor = float(predictions["predicted_shorten_probability"].quantile(0.60))
    moderate_ceiling = float(predictions["predicted_shorten_probability"].quantile(0.75))
    compression_positive = _safe_numeric_series(predictions, "odds_compression_index").gt(0)
    strong_density = _safe_numeric_series(predictions, "favourite_density").gt(_safe_numeric_series(predictions, "favourite_density").median())
    per_race_top = predictions.groupby(["race_date", "track_norm", "race_number"], observed=False)["predicted_shorten_probability"].rank(method="first", ascending=False).eq(1)
    return {
        "top_decile_shortening": predictions["predicted_shorten_probability"].ge(top_decile),
        "top_quintile_shortening": predictions["predicted_shorten_probability"].ge(top_quintile),
        "per_race_top_1_shortening": per_race_top,
        "shortening_plus_compression": predictions["predicted_shorten_probability"].ge(top_quintile) & compression_positive,
        "compression_conditioned_value": compression_positive & strong_density,
        "controlled_steam_proxy": predictions["predicted_shorten_probability"].between(controlled_floor, moderate_ceiling, inclusive="both") & compression_positive,
        "small_steam_proxy": predictions["predicted_shorten_probability"].between(0.15, 0.25, inclusive="both"),
        "medium_steam_proxy": predictions["predicted_shorten_probability"].between(0.25, 0.40, inclusive="both"),
    }


def fit_candidate_thresholds(predictions: pd.DataFrame) -> dict[str, float]:
    if predictions.empty:
        return {
            "top_decile": 1.0,
            "top_quintile": 1.0,
            "controlled_floor": 1.0,
            "moderate_ceiling": 1.0,
            "compression_median": 0.0,
            "density_median": 0.0,
            "form_consistency_median": 0.0,
            "pace_pressure_median": 0.0,
            "adverse_fill_proxy_median": 0.0,
        }
    return {
        "top_decile": float(predictions["predicted_shorten_probability"].quantile(0.90)),
        "top_quintile": float(predictions["predicted_shorten_probability"].quantile(0.80)),
        "controlled_floor": float(predictions["predicted_shorten_probability"].quantile(0.60)),
        "moderate_ceiling": float(predictions["predicted_shorten_probability"].quantile(0.75)),
        "compression_median": float(_safe_numeric_series(predictions, "odds_compression_index").median()),
        "density_median": float(_safe_numeric_series(predictions, "favourite_density").median()),
        "form_consistency_median": float(_safe_numeric_series(predictions, "form_consistency_proxy").median()),
        "pace_pressure_median": float(_safe_numeric_series(predictions, "pace_pressure_proxy").median()),
        "adverse_fill_proxy_median": float(_safe_numeric_series(predictions, "adverse_fill_probability_proxy").median()),
    }


def frozen_candidate_masks(predictions: pd.DataFrame, thresholds: dict[str, float]) -> dict[str, pd.Series]:
    if predictions.empty:
        return {}
    compression_positive = _safe_numeric_series(predictions, "odds_compression_index").gt(thresholds.get("compression_median", 0.0))
    strong_density = _safe_numeric_series(predictions, "favourite_density").gt(thresholds.get("density_median", 0.0))
    per_race_top = predictions.groupby(["race_date", "track_norm", "race_number"], observed=False)["predicted_shorten_probability"].rank(method="first", ascending=False).eq(1)
    return {
        "top_decile_shortening": predictions["predicted_shorten_probability"].ge(thresholds.get("top_decile", 1.0)),
        "top_quintile_shortening": predictions["predicted_shorten_probability"].ge(thresholds.get("top_quintile", 1.0)),
        "per_race_top_1_shortening": per_race_top,
        "shortening_plus_compression": predictions["predicted_shorten_probability"].ge(thresholds.get("top_quintile", 1.0)) & compression_positive,
        "compression_conditioned_value": compression_positive & strong_density,
        "controlled_steam_proxy": predictions["predicted_shorten_probability"].between(
            thresholds.get("controlled_floor", 1.0),
            thresholds.get("moderate_ceiling", 1.0),
            inclusive="both",
        ) & compression_positive,
        "small_steam_proxy": predictions["predicted_shorten_probability"].between(0.15, 0.25, inclusive="both"),
        "medium_steam_proxy": predictions["predicted_shorten_probability"].between(0.25, 0.40, inclusive="both"),
        "adverse_fill_filtered": (
            predictions["predicted_shorten_probability"].ge(thresholds.get("top_quintile", 1.0))
            & compression_positive
            & _safe_numeric_series(predictions, "adverse_fill_probability_proxy").lt(thresholds.get("adverse_fill_proxy_median", 0.0))
        ),
    }


def predicted_shortening_report(frame: pd.DataFrame, target_spec, *, model_names: set[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    features, _ = realistic_non_market_features(frame)
    train_frame, test_frame = _split_train_test(frame, 0.25)
    return evaluate_models_on_split(train_frame, test_frame, target_spec, features, model_names=model_names or {"gradient_boosting"})


def add_value_columns(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    close_price = _safe_numeric_series(working, "close_price", fill=np.nan)
    price_60 = _safe_numeric_series(working, "price_60m", fill=np.nan)
    best_back = _safe_numeric_series(working, "best_back_price", fill=np.nan)
    best_lay = _safe_numeric_series(working, "best_lay_price", fill=np.nan)
    spread = (best_lay - best_back).replace([np.inf, -np.inf], np.nan).clip(lower=0.0).fillna(0.0)
    working["spread_proxy"] = spread
    working["close_implied_prob"] = (1.0 / close_price.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    working["anchor_implied_prob"] = (1.0 / price_60.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    working["opening_implied_prob"] = (
        1.0 / _safe_numeric_series(working, "opening_price", fill=np.nan).replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    working["realized_win_flag"] = _safe_numeric_series(working, "won_flag").astype(float)
    working["post_shortening_edge"] = working["realized_win_flag"] - working["close_implied_prob"]
    working["closing_value_gap"] = working["anchor_implied_prob"] - working["close_implied_prob"]
    working["shortening_magnitude"] = (price_60 - close_price).fillna(0.0)
    working["price_acceleration"] = _safe_numeric_series(working, "movement_60_to_close") - _safe_numeric_series(working, "movement_open_to_60")
    working["steam_persistence"] = np.sign(_safe_numeric_series(working, "movement_open_to_60")) * np.sign(_safe_numeric_series(working, "movement_60_to_close"))
    working["shortening_exhaustion"] = np.where(
        _safe_numeric_series(working, "movement_open_to_60") > 0,
        _safe_numeric_series(working, "movement_60_to_close"),
        0.0,
    )
    working["compression_persistence"] = _safe_numeric_series(working, "odds_compression_index") + _safe_numeric_series(working, "pre60_compression")
    return working


def add_compression_features(frame: pd.DataFrame) -> pd.DataFrame:
    working = add_value_columns(frame)
    compression = _safe_numeric_series(working, "odds_compression_index")
    working["compression_regime"] = pd.cut(
        compression,
        bins=[-np.inf, -0.5, 0.5, np.inf],
        labels=["low_compression", "medium_compression", "high_compression"],
        include_lowest=True,
    ).astype(str)
    working["rank_band"] = pd.cut(
        _safe_numeric_series(working, "market_rank_current"),
        bins=[0, 2, 5],
        labels=["rank_1_to_2", "rank_3_to_5"],
        include_lowest=True,
    ).astype(str)
    working["odds_band"] = pd.cut(
        _safe_numeric_series(working, "anchor_price"),
        bins=[0, 4, 6, 10],
        labels=["odds_2_to_4", "odds_4_to_6", "odds_6_to_10"],
        include_lowest=True,
    ).astype(str)
    return working


def add_steam_features(frame: pd.DataFrame) -> pd.DataFrame:
    working = add_compression_features(frame)
    price_60 = _safe_numeric_series(working, "price_60m", fill=np.nan)
    close_price = _safe_numeric_series(working, "close_price", fill=np.nan)
    working["steam_percent"] = ((price_60 - close_price) / price_60.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    working["steam_size"] = (price_60 - close_price).fillna(0.0)
    working["steam_velocity"] = _safe_numeric_series(working, "movement_60_to_close") / price_60.replace(0, np.nan)
    working["steam_velocity"] = working["steam_velocity"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    working["steam_exhaustion_score"] = working["steam_percent"] - _safe_numeric_series(working, "post_shortening_edge")
    working["controlled_steam_flag"] = ((working["steam_percent"] > 0.0) & (working["steam_percent"] <= 0.12)).astype(int)
    working["excessive_steam_flag"] = (working["steam_percent"] > 0.20).astype(int)
    working["steam_bucket"] = pd.cut(
        working["steam_percent"],
        bins=[-np.inf, 0.0, 0.03, 0.08, 0.15, 0.25, np.inf],
        labels=["no_steam", "tiny_steam", "small_steam", "medium_steam", "large_steam", "extreme_steam"],
        include_lowest=True,
    ).astype(str)
    return working


def scenario_fill_price(frame: pd.DataFrame, scenario: ExecutionScenario) -> pd.Series:
    price_60 = _safe_numeric_series(frame, "price_60m", fill=np.nan)
    close_price = _safe_numeric_series(frame, "close_price", fill=np.nan)
    spread = _safe_numeric_series(frame, "spread_proxy")
    # move_capture=0 means perfect 60-second fill; 1 means close-like worst fill.
    fill = price_60 + scenario.move_capture * (close_price - price_60)
    fill = fill - (spread * scenario.spread_multiplier) - scenario.stale_penalty
    fill = fill.clip(lower=1.01)
    return fill.replace([np.inf, -np.inf], np.nan)


def executed_frame(frame: pd.DataFrame, target_spec, scenario: ExecutionScenario) -> pd.DataFrame:
    working = add_value_columns(frame)
    fill_price = scenario_fill_price(working, scenario)
    stake = _safe_numeric_series(working, "stake", fill=1.0) * scenario.fill_fraction
    won = _safe_numeric_series(working, "won_flag")
    working["expected_fill_price"] = fill_price
    working["executed_stake"] = stake
    working["executed_clv"] = (fill_price / _safe_numeric_series(working, "close_price", fill=np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0) - 1.0
    working["executable_edge"] = won - (1.0 / fill_price.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    working["executed_profit"] = np.where(won > 0, stake * (fill_price - 1.0), -stake)
    working["execution_fragility"] = (_safe_numeric_series(working, target_spec.target_clv_column) - working["executed_clv"]).clip(lower=0.0)
    working["scenario_name"] = scenario.name
    return working


def drawdown(profit: pd.Series) -> float:
    cumulative = pd.to_numeric(profit, errors="coerce").fillna(0.0).cumsum()
    if cumulative.empty:
        return 0.0
    peak = cumulative.cummax()
    return float((peak - cumulative).max())


def bankroll_path_metrics(frame: pd.DataFrame, *, initial_bankroll: float = 100.0) -> dict[str, float]:
    if frame.empty:
        return {
            "bankroll_end": initial_bankroll,
            "bankroll_return": 0.0,
            "max_drawdown": 0.0,
            "volatility": 0.0,
            "risk_of_ruin_proxy": 0.0,
        }
    profit = pd.to_numeric(frame["executed_profit"], errors="coerce").fillna(0.0)
    cumulative = initial_bankroll + profit.cumsum()
    returns = profit / max(initial_bankroll, 1.0)
    peak = cumulative.cummax()
    max_drawdown = float((peak - cumulative).max()) if not cumulative.empty else 0.0
    return {
        "bankroll_end": float(cumulative.iloc[-1]) if not cumulative.empty else initial_bankroll,
        "bankroll_return": float((cumulative.iloc[-1] - initial_bankroll) / max(initial_bankroll, 1.0)) if not cumulative.empty else 0.0,
        "max_drawdown": max_drawdown,
        "volatility": float(returns.std(ddof=0)) if len(returns) > 1 else 0.0,
        "risk_of_ruin_proxy": float((cumulative <= (initial_bankroll * 0.7)).mean()) if not cumulative.empty else 0.0,
    }


def add_execution_risk_features(frame: pd.DataFrame) -> pd.DataFrame:
    working = add_steam_features(frame)
    working["price_velocity"] = _safe_numeric_series(working, "movement_60_to_close").abs()
    working["steam_acceleration"] = (_safe_numeric_series(working, "movement_60_to_close") - _safe_numeric_series(working, "movement_open_to_60")).abs()
    working["volatility_before_jump"] = _safe_numeric_series(working, "pre60_volatility") + _safe_numeric_series(working, "price_velocity")
    working["compression_instability"] = _safe_numeric_series(working, "market_dispersion").abs() + _safe_numeric_series(working, "field_compression_proxy").abs()
    working["late_market_chaos"] = (
        _safe_numeric_series(working, "price_velocity").abs()
        + _safe_numeric_series(working, "spread_proxy").abs()
        + _safe_numeric_series(working, "steam_acceleration").abs()
    ) / 3.0
    working["odds_whipsaw"] = (_safe_numeric_series(working, "movement_open_to_60") * _safe_numeric_series(working, "movement_60_to_close")).lt(0).astype(int)
    working["market_rank_instability"] = (
        _safe_numeric_series(working, "sixty_market_rank") - _safe_numeric_series(working, "market_rank_current")
    ).abs()
    working["field_liquidity_proxy"] = (
        np.log1p(_safe_numeric_series(working, "total_matched"))
        - _safe_numeric_series(working, "field_size") / 10.0
    )
    working["adverse_fill_probability_proxy"] = (
        _safe_numeric_series(working, "late_market_chaos")
        + _safe_numeric_series(working, "compression_instability")
        + _safe_numeric_series(working, "odds_whipsaw")
        + _safe_numeric_series(working, "market_rank_instability")
        - _safe_numeric_series(working, "field_liquidity_proxy")
    ) / 5.0
    return working


def adverse_fill_flags(frame: pd.DataFrame, *, severe_threshold: float = 0.20, catastrophic_threshold: float = 0.35) -> pd.DataFrame:
    working = frame.copy()
    working["fill_price_gap"] = (_safe_numeric_series(working, "expected_fill_price") - _safe_numeric_series(working, "price_60m")).abs()
    working["fill_gap_ratio"] = working["fill_price_gap"] / np.clip(_safe_numeric_series(working, "price_60m", fill=1.0), 1.0, None)
    working["adverse_fill_flag"] = working["fill_gap_ratio"].ge(severe_threshold).astype(int)
    working["catastrophic_fill_flag"] = working["fill_gap_ratio"].ge(catastrophic_threshold).astype(int)
    working["fill_quality_label"] = np.select(
        [
            working["fill_gap_ratio"] < 0.05,
            working["fill_gap_ratio"] < severe_threshold,
            working["fill_gap_ratio"] < catastrophic_threshold,
        ],
        ["good_fill", "acceptable_fill", "adverse_fill"],
        default="catastrophic_fill",
    )
    return working


def execution_summary(frame: pd.DataFrame, target_spec, *, label: str, scenario_name: str) -> dict[str, float | int | str | bool]:
    if frame.empty:
        return {
            "selection_rule": label,
            "scenario": scenario_name,
            "selections": 0,
        }
    return {
        "selection_rule": label,
        "scenario": scenario_name,
        "selections": int(len(frame)),
        "actual_shorten_rate": float(frame[target_spec.target_flag_column].mean()),
        "average_clv": float(frame[target_spec.target_clv_column].mean()),
        "executed_clv": float(frame["executed_clv"].mean()),
        "executable_edge": float(frame["executable_edge"].mean()),
        "post_shortening_edge": float(frame["post_shortening_edge"].mean()),
        "roi": _safe_divide(float(frame["executed_profit"].sum()), float(frame["executed_stake"].sum())),
        "drawdown": drawdown(frame["executed_profit"]),
        "fill_quality": float((frame["expected_fill_price"] / _safe_numeric_series(frame, "price_60m", fill=np.nan)).replace([np.inf, -np.inf], np.nan).mean()),
        "average_fill_price": float(frame["expected_fill_price"].mean()),
        "average_odds": float(frame["anchor_price"].mean()),
        "average_market_rank": float(frame["market_rank_current"].mean()),
        "clv_hit_rate": float(frame["executed_clv"].gt(0).mean()),
        "value_hit_rate": float(frame["executable_edge"].gt(0).mean()),
        "execution_fragility": float(frame["execution_fragility"].mean()),
        "low_sample_size": bool(len(frame) < 80),
        "slippage_sensitivity": bool(float(frame["execution_fragility"].mean()) > 0.25),
        "concentration_risk": bool(_string_series(frame, "track_norm").value_counts(normalize=True, dropna=False).max() > 0.45),
    }


def bucket_by_probability(predictions: pd.DataFrame) -> pd.DataFrame:
    working = predictions.copy()
    working["predicted_bucket"] = _safe_qbucket(
        working["predicted_shorten_probability"],
        ["q1", "q2", "q3", "q4", "q5"],
    )
    return working


def regression_models(train_x: pd.DataFrame, train_y: pd.Series) -> dict[str, object]:
    return {
        "ridge": Ridge(alpha=1.0, random_state=None),
        "random_forest_regressor": RandomForestRegressor(n_estimators=200, random_state=42, min_samples_leaf=5, n_jobs=-1),
        "gradient_boosting_regressor": GradientBoostingRegressor(random_state=42),
    }


def fit_regression_suite(frame: pd.DataFrame, feature_columns: Iterable[str], target_column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    working = frame.copy()
    columns = [column for column in feature_columns if column in working.columns and working[column].notna().sum() >= 20]
    if target_column not in working.columns or not columns:
        return pd.DataFrame(), pd.DataFrame()
    train_frame, test_frame = _split_train_test(working, 0.25)
    x_train = pd.get_dummies(train_frame[columns].copy(), dummy_na=True)
    x_test = pd.get_dummies(test_frame[columns].copy(), dummy_na=True)
    all_cols = sorted(set(x_train.columns) | set(x_test.columns))
    x_train = x_train.reindex(columns=all_cols, fill_value=0).fillna(0)
    x_test = x_test.reindex(columns=all_cols, fill_value=0).fillna(0)
    y_train = pd.to_numeric(train_frame[target_column], errors="coerce").fillna(0.0)
    y_test = pd.to_numeric(test_frame[target_column], errors="coerce").fillna(0.0)
    result_rows = []
    prediction_rows = []
    for model_name, model in regression_models(x_train, y_train).items():
        model.fit(x_train, y_train)
        pred = pd.Series(model.predict(x_test), index=test_frame.index)
        rmse = float(np.sqrt(mean_squared_error(y_test, pred)))
        mae = float(mean_absolute_error(y_test, pred))
        result_rows.append(
            {
                "model_name": model_name,
                "target_column": target_column,
                "mae": mae,
                "rmse": rmse,
                "pearson_corr": float(pd.Series(y_test).corr(pred)) if len(pred) > 1 else 0.0,
                "spearman_corr": float(pd.Series(y_test).corr(pred, method="spearman")) if len(pred) > 1 else 0.0,
                "selections": int(len(pred)),
            }
        )
        prediction_rows.append(
            pd.DataFrame(
                {
                    "model_name": model_name,
                    "actual_value": y_test.to_numpy(),
                    "predicted_value": pred.to_numpy(),
                }
            )
        )
    return pd.DataFrame(result_rows).sort_values(["rmse", "mae"], ascending=[True, True]).reset_index(drop=True), pd.concat(prediction_rows, ignore_index=True)


def value_bucket_report(frame: pd.DataFrame, bucket_column: str) -> pd.DataFrame:
    rows = []
    for bucket, subset in frame.groupby(bucket_column, observed=False):
        clv_source = "executed_clv"
        if clv_source not in subset.columns:
            clv_source = "clv_60_to_close" if "clv_60_to_close" in subset.columns else "lab_open_to_60_clv" if "lab_open_to_60_clv" in subset.columns else ""
        executed_clv = float(subset[clv_source].mean()) if clv_source else 0.0
        executable_edge = float(subset["executable_edge"].mean()) if "executable_edge" in subset.columns else 0.0
        executed_profit = float(subset["executed_profit"].sum()) if "executed_profit" in subset.columns else float(subset["profit_loss"].sum()) if "profit_loss" in subset.columns else 0.0
        executed_stake = float(subset["executed_stake"].sum()) if "executed_stake" in subset.columns else float(subset["stake"].sum()) if "stake" in subset.columns else 0.0
        rows.append(
            {
                bucket_column: str(bucket),
                "selections": int(len(subset)),
                "executed_clv": executed_clv,
                "executable_edge": executable_edge,
                "post_shortening_edge": float(subset["post_shortening_edge"].mean()) if "post_shortening_edge" in subset.columns else 0.0,
                "roi": _safe_divide(executed_profit, executed_stake),
                "win_rate": float(_safe_numeric_series(subset, "won_flag").mean()),
                "close_implied_prob": float(subset["close_implied_prob"].mean()) if "close_implied_prob" in subset.columns else 0.0,
            }
        )
    return pd.DataFrame(rows)


def outlier_stress(frame: pd.DataFrame, *, profit_column: str, value_column: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    working = frame.sort_values(profit_column, ascending=False).reset_index(drop=True)
    scenarios = {
        "baseline": working,
        "remove_best_profit": working.iloc[1:].copy(),
        "remove_top2_profit": working.iloc[min(2, len(working)) :].copy(),
        "remove_top5_profit": working.iloc[min(5, len(working)) :].copy(),
    }
    value_sorted = frame.sort_values(value_column, ascending=False).reset_index(drop=True)
    scenarios["remove_best_value"] = value_sorted.iloc[1:].copy()
    scenarios["remove_top5_value"] = value_sorted.iloc[min(5, len(value_sorted)) :].copy()
    rows = []
    for scenario_name, subset in scenarios.items():
        if subset.empty:
            continue
        rows.append(
            {
                "scenario": scenario_name,
                "selections": int(len(subset)),
                "roi": _safe_divide(float(subset[profit_column].sum()), float(_safe_numeric_series(subset, "executed_stake", fill=1.0).sum())),
                "value_mean": float(_safe_numeric_series(subset, value_column).mean()),
                "clv_mean": float(_safe_numeric_series(subset, "executed_clv" if "executed_clv" in subset.columns else "clv_60_to_close").mean()),
            }
        )
    return pd.DataFrame(rows)


def candidate_live_readiness(row: pd.Series) -> float:
    score = 0.0
    score += min(20.0, float(row.get("selections", 0)) / 20.0)
    score += 15.0 if float(row.get("roi", 0.0)) > 0 else 0.0
    score += 15.0 if float(row.get("executed_clv", 0.0)) > 0 else 0.0
    score += 10.0 if float(row.get("post_shortening_edge", 0.0)) > 0 else 0.0
    score += 10.0 if not bool(row.get("low_sample_size", True)) else 0.0
    score += 10.0 if not bool(row.get("slippage_sensitivity", True)) else 0.0
    score += 10.0 if not bool(row.get("concentration_risk", True)) else 0.0
    score += 10.0 if float(row.get("average_odds", 99.0)) <= 10.0 else 0.0
    return round(min(100.0, score), 2)


def hardened_live_readiness(row: pd.Series) -> tuple[float, dict[str, float]]:
    base = candidate_live_readiness(row)
    penalties = {
        "temporal_weakness": 20.0 if float(row.get("temporal_consistency_score", 0.0)) < 0.25 else 0.0,
        "month_concentration": min(20.0, float(row.get("month_concentration", 0.0)) * 20.0),
        "slippage_sensitivity": 12.0 if bool(row.get("slippage_sensitivity", False)) else 0.0,
        "adverse_fill_sensitivity": 12.0 if bool(row.get("adverse_fill_sensitivity", False)) else 0.0,
        "overbet_risk": 12.0 if float(row.get("post_shortening_edge", 0.0)) < 0 else 0.0,
        "weak_fill_model": 10.0 if float(row.get("fill_model_confidence", 0.0)) < 0.60 else 0.0,
        "tiny_sample": 12.0 if bool(row.get("low_sample_size", False)) else 0.0,
        "large_steam_dependence": 10.0 if bool(row.get("large_steam_dependence", False)) else 0.0,
    }
    score = max(0.0, base - sum(penalties.values()))
    return round(score, 2), penalties
