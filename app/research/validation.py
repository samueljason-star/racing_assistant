from __future__ import annotations

import argparse
from dataclasses import dataclass
from itertools import product
from pathlib import Path

import pandas as pd
from sklearn.metrics import brier_score_loss, roc_auc_score

from app.betting.market_helpers import commission_adjusted_market_probability
from app.research.testing_model import build_candidate_models, build_model_frame, split_train_validation
from app.research.utils import (
    RESEARCH_ARTIFACTS_DIR,
    RESEARCH_DATA_DIR,
    RESEARCH_REPORTS_DIR,
    compute_max_drawdown,
    json_dump,
    odds_bucket_label,
    save_dataframe,
)

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
VALIDATION_RESULTS_PATH = RESEARCH_REPORTS_DIR / "validation_strategy_results.csv"
VALIDATION_BREAKDOWN_MONTHLY_PATH = RESEARCH_REPORTS_DIR / "validation_monthly_breakdown.csv"
VALIDATION_BREAKDOWN_TRACK_PATH = RESEARCH_REPORTS_DIR / "validation_track_breakdown.csv"
VALIDATION_BREAKDOWN_ODDS_PATH = RESEARCH_REPORTS_DIR / "validation_odds_bucket_breakdown.csv"
VALIDATION_CLV_PATH = RESEARCH_REPORTS_DIR / "validation_clv_diagnostics.csv"
VALIDATION_RECOMMENDATIONS_PATH = RESEARCH_REPORTS_DIR / "validation_recommendations.csv"
CONSERVATIVE_CONFIG_PATH = RESEARCH_ARTIFACTS_DIR / "conservative_recommended_config.json"
BALANCED_CONFIG_PATH = RESEARCH_ARTIFACTS_DIR / "balanced_recommended_config.json"
AGGRESSIVE_CONFIG_PATH = RESEARCH_ARTIFACTS_DIR / "aggressive_recommended_config.json"
LIVE_CANDIDATE_PATH = RESEARCH_ARTIFACTS_DIR / "model_edge_v3_candidate.json"
LEGACY_OUTPUT_PATH = RESEARCH_ARTIFACTS_DIR / "best_strategy_config.json"

CONSERVATIVE_TIER = {"name": "conservative", "min_bets": 100, "max_odds_cap": 20.0, "max_drawdown": 0.20}
BALANCED_TIER = {"name": "balanced", "min_bets": 50, "max_odds_cap": 30.0, "max_drawdown": 0.30}
AGGRESSIVE_TIER = {"name": "aggressive", "min_bets": 25, "max_odds_cap": 50.0, "max_drawdown": 0.45}
RECOMMENDATION_TIERS = (CONSERVATIVE_TIER, BALANCED_TIER, AGGRESSIVE_TIER)
MIN_FLAT_ROI = 0.10
LATE_MARKET_MINUTES = 75


@dataclass(frozen=True)
class ModeConfig:
    name: str
    feature_columns: tuple[str, ...]
    odds_column: str
    market_probability_column: str
    edge_column: str
    rank_column: str
    notes: str


MODEL_MODES = {
    "morning_model": ModeConfig(
        name="morning_model",
        feature_columns=(
            "form_score",
            "morning_edge",
            "morning_market_probability_adj",
            "opening_price",
            "opening_market_rank",
            "barrier",
            "distance",
            "average_last_3_finish",
            "best_last_3_finish",
            "last_start_finish",
            "track_condition_match",
            "similar_distance_flag",
        ),
        odds_column="opening_price",
        market_probability_column="morning_market_probability_adj",
        edge_column="morning_edge",
        rank_column="opening_market_rank",
        notes="Morning-safe: excludes 10-minute and closing-derived features.",
    ),
    "late_market_model": ModeConfig(
        name="late_market_model",
        feature_columns=(
            "form_score",
            "late_edge",
            "late_market_probability_adj",
            "price_60m",
            "price_30m",
            "price_10m",
            "open_to_10_change",
            "60_to_10_change",
            "price_10m_market_rank",
            "barrier",
            "distance",
            "average_last_3_finish",
            "best_last_3_finish",
            "last_start_finish",
            "track_condition_match",
            "similar_distance_flag",
        ),
        odds_column="price_10m",
        market_probability_column="late_market_probability_adj",
        edge_column="late_edge",
        rank_column="price_10m_market_rank",
        notes="Late-market-safe: uses pre-jump prices and movement without any closing-derived inputs.",
    ),
}


def _safe_probability_from_odds(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").map(
        lambda value: commission_adjusted_market_probability(value, 0.08)
    )


def _safe_rank(frame: pd.DataFrame, odds_column: str, output_column: str) -> None:
    frame[output_column] = (
        pd.to_numeric(frame.get(odds_column), errors="coerce")
        .groupby([frame["race_date"], frame["track_norm"], frame["race_number"]], dropna=False)
        .rank(method="dense", ascending=True)
    )


def build_validation_frame(matched_path: Path = MATCHED_PATH) -> pd.DataFrame:
    frame = build_model_frame(matched_path)
    frame["opening_price"] = pd.to_numeric(frame.get("opening_price"), errors="coerce")
    frame["price_60m"] = pd.to_numeric(frame.get("price_60m"), errors="coerce")
    frame["price_30m"] = pd.to_numeric(frame.get("price_30m"), errors="coerce")
    frame["price_10m"] = pd.to_numeric(frame.get("price_10m"), errors="coerce")
    frame["closing_price"] = pd.to_numeric(frame.get("closing_price"), errors="coerce")

    frame["open_to_10_change"] = frame["opening_price"] - frame["price_10m"]
    frame["60_to_10_change"] = frame["price_60m"] - frame["price_10m"]

    frame["morning_market_probability_adj"] = _safe_probability_from_odds(frame["opening_price"])
    frame["late_market_probability_adj"] = _safe_probability_from_odds(frame["price_10m"])
    frame["morning_edge"] = frame["estimated_probability"] - frame["morning_market_probability_adj"].fillna(0.0)
    frame["late_edge"] = frame["estimated_probability"] - frame["late_market_probability_adj"].fillna(0.0)

    _safe_rank(frame, "opening_price", "opening_market_rank")
    _safe_rank(frame, "price_10m", "price_10m_market_rank")
    frame["month"] = frame["race_day"].dt.to_period("M").astype(str)
    return frame


def _profit_for_row(odds: float, stake: float, won_flag: int) -> float:
    return round(((odds - 1.0) * 0.92 * stake) if won_flag == 1 else -stake, 2)


def _simulate_staking(bets: pd.DataFrame, stake_mode: str) -> dict[str, float]:
    bank = 10000.0
    bank_history = [bank]
    profits: list[float] = []
    stakes: list[float] = []

    for _, row in bets.iterrows():
        stake = 100.0 if stake_mode == "flat" else round(bank * 0.01, 2)
        stakes.append(stake)
        profit_loss = _profit_for_row(float(row["odds_used"]), stake, int(row["won_flag"]))
        profits.append(profit_loss)
        bank = round(bank + profit_loss, 2)
        bank_history.append(bank)

    total_staked = round(sum(stakes), 2)
    total_profit = round(sum(profits), 2)
    return {
        "total_staked": total_staked,
        "profit_loss": total_profit,
        "roi": round(total_profit / total_staked, 4) if total_staked else 0.0,
        "drawdown": compute_max_drawdown(bank_history),
        "final_bank": bank,
        "profit_positive": bool(total_profit > 0),
    }


def _build_warning_list(row: dict[str, object]) -> list[str]:
    warnings: list[str] = []
    if int(row.get("bets", 0)) < 50:
        warnings.append("low_sample_size")
    if float(row.get("average_odds", 0.0)) > 25.0:
        warnings.append("high_average_odds")
    if float(row.get("average_edge", 0.0)) < 0.0:
        warnings.append("negative_average_edge")
    if not bool(row.get("clv_sanity_passed", False)):
        warnings.append("poor_clv_sanity")
    if float(row.get("track_concentration", 0.0)) > 0.45:
        warnings.append("track_concentration")
    if float(row.get("month_concentration", 0.0)) > 0.45:
        warnings.append("month_concentration")
    return warnings


def _simulate_validation_strategy(
    frame: pd.DataFrame,
    *,
    mode: ModeConfig,
    model_name: str,
    probability_threshold: float,
    min_edge: float,
    min_form_score: float,
    max_odds: float,
    max_bets_per_day: int,
) -> tuple[dict[str, object], pd.DataFrame]:
    working = frame.copy()
    odds_series = pd.to_numeric(working.get(mode.odds_column), errors="coerce")
    edge_series = pd.to_numeric(working.get(mode.edge_column), errors="coerce")

    working = working[
        (working["predicted_win_probability"] >= probability_threshold)
        & (edge_series >= min_edge)
        & (working["form_score"] >= min_form_score)
        & (odds_series >= 3.0)
        & (odds_series <= max_odds)
    ].copy()

    if working.empty:
        return {
            "mode": mode.name,
            "model_name": model_name,
            "probability_threshold": probability_threshold,
            "min_edge": min_edge,
            "min_form_score": min_form_score,
            "max_odds": max_odds,
            "max_bets_per_day": max_bets_per_day,
            "bets": 0,
            "wins": 0,
            "flat_roi": 0.0,
            "pct_roi": 0.0,
            "flat_drawdown": 0.0,
            "pct_drawdown": 0.0,
            "flat_profit_loss": 0.0,
            "pct_profit_loss": 0.0,
            "flat_profit_positive": False,
            "pct_profit_positive": False,
            "average_odds": 0.0,
            "average_edge": 0.0,
            "average_clv": 0.0,
            "clv_hit_rate": 0.0,
            "clv_sanity_passed": False,
            "track_concentration": 0.0,
            "month_concentration": 0.0,
            "validation_positive_months": 0,
            "validation_positive_tracks": 0,
            "warning_flags": "low_sample_size,poor_clv_sanity",
            "mode_notes": mode.notes,
        }, pd.DataFrame()

    working["odds_used"] = odds_series.loc[working.index]
    working["market_probability_used"] = pd.to_numeric(
        working.get(mode.market_probability_column), errors="coerce"
    ).loc[working.index]
    working["edge_used"] = edge_series.loc[working.index]
    working["mode"] = mode.name
    working["model_name"] = model_name
    working["race_rank"] = working.groupby(
        ["race_date", "track_norm", "race_number"], dropna=False
    )["predicted_win_probability"].rank(method="dense", ascending=False)
    working = working[working["race_rank"] == 1].copy()
    working = working.sort_values(["race_day", "predicted_win_probability"], ascending=[True, False])
    working["daily_rank"] = working.groupby("race_date").cumcount() + 1
    working = working[working["daily_rank"] <= max_bets_per_day].copy()

    if working.empty:
        return _simulate_validation_strategy(
            frame.iloc[0:0],
            mode=mode,
            model_name=model_name,
            probability_threshold=probability_threshold,
            min_edge=min_edge,
            min_form_score=min_form_score,
            max_odds=max_odds,
            max_bets_per_day=max_bets_per_day,
        )

    flat_metrics = _simulate_staking(working, "flat")
    pct_metrics = _simulate_staking(working, "pct")

    clv_values = pd.to_numeric(working.get("clv_percent"), errors="coerce").dropna()
    clv_sanity_passed = bool(
        len(clv_values) >= max(10, int(len(working) * 0.5))
        and clv_values.abs().median() <= 35.0
    )
    monthly_profit = (
        working.assign(flat_profit_loss=working.apply(lambda row: _profit_for_row(float(row["odds_used"]), 100.0, int(row["won_flag"])), axis=1))
        .groupby("month", dropna=False)["flat_profit_loss"]
        .sum()
    )
    track_profit = (
        working.assign(flat_profit_loss=working.apply(lambda row: _profit_for_row(float(row["odds_used"]), 100.0, int(row["won_flag"])), axis=1))
        .groupby("track", dropna=False)["flat_profit_loss"]
        .sum()
    )

    result = {
        "mode": mode.name,
        "model_name": model_name,
        "probability_threshold": probability_threshold,
        "min_edge": min_edge,
        "min_form_score": min_form_score,
        "max_odds": max_odds,
        "max_bets_per_day": max_bets_per_day,
        "bets": int(len(working)),
        "wins": int(working["won_flag"].sum()),
        "strike_rate": round(float(working["won_flag"].mean()), 4),
        "flat_roi": flat_metrics["roi"],
        "pct_roi": pct_metrics["roi"],
        "flat_drawdown": flat_metrics["drawdown"],
        "pct_drawdown": pct_metrics["drawdown"],
        "flat_profit_loss": flat_metrics["profit_loss"],
        "pct_profit_loss": pct_metrics["profit_loss"],
        "flat_profit_positive": flat_metrics["profit_positive"],
        "pct_profit_positive": pct_metrics["profit_positive"],
        "flat_final_bank": flat_metrics["final_bank"],
        "pct_final_bank": pct_metrics["final_bank"],
        "average_odds": round(float(working["odds_used"].mean()), 4),
        "average_edge": round(float(working["edge_used"].mean()), 4),
        "average_clv": round(float(clv_values.mean()), 4) if not clv_values.empty else 0.0,
        "median_clv": round(float(clv_values.median()), 4) if not clv_values.empty else 0.0,
        "clv_hit_rate": round(float((clv_values > 0).mean()), 4) if not clv_values.empty else 0.0,
        "clv_sample_size": int(len(clv_values)),
        "clv_sanity_passed": clv_sanity_passed,
        "track_concentration": round(float(working["track"].value_counts(normalize=True, dropna=False).iloc[0]), 4),
        "month_concentration": round(float(working["month"].value_counts(normalize=True, dropna=False).iloc[0]), 4),
        "validation_positive_months": int((monthly_profit > 0).sum()),
        "validation_month_count": int(len(monthly_profit)),
        "validation_positive_tracks": int((track_profit > 0).sum()),
        "validation_track_count": int(len(track_profit)),
        "mode_notes": mode.notes,
    }
    result["warning_flags"] = ",".join(_build_warning_list(result))
    return result, working


def _result_score(row: pd.Series, tier: dict[str, object]) -> float:
    score = float(row["flat_roi"]) * 1.20
    score += float(row["pct_roi"]) * 0.25
    score += min(float(row["bets"]) / float(tier["min_bets"]), 1.25) * 0.15
    score += 0.05 if bool(row["flat_profit_positive"]) else -0.20
    score += 0.04 if bool(row["clv_sanity_passed"]) and float(row["average_clv"]) > 0 else 0.0
    score -= float(row["flat_drawdown"]) * 0.60
    score -= max(float(row["average_odds"]) - float(tier["max_odds_cap"]), 0.0) / 100.0
    score -= max(float(row["track_concentration"]) - 0.35, 0.0) * 0.30
    score -= max(float(row["month_concentration"]) - 0.35, 0.0) * 0.30
    if float(row["average_edge"]) < 0:
        score -= 0.03
    if row["model_name"] == "logistic":
        score += 0.03
    if row["mode"] == "morning_model":
        score += 0.02
    return round(score, 6)


def _invalid_recommendation(tier: dict[str, object], reason: str) -> dict[str, object]:
    return {
        "tier": tier["name"],
        "mode": "none",
        "model_name": "none",
        "probability_threshold": 0.0,
        "min_edge": 0.0,
        "min_form_score": 0.0,
        "max_odds": float(tier["max_odds_cap"]),
        "max_bets_per_day": 0,
        "bets": 0,
        "wins": 0,
        "strike_rate": 0.0,
        "flat_roi": 0.0,
        "pct_roi": 0.0,
        "flat_drawdown": 0.0,
        "pct_drawdown": 0.0,
        "flat_profit_loss": 0.0,
        "pct_profit_loss": 0.0,
        "flat_profit_positive": False,
        "pct_profit_positive": False,
        "flat_final_bank": 10000.0,
        "pct_final_bank": 10000.0,
        "average_odds": 0.0,
        "average_edge": 0.0,
        "average_clv": 0.0,
        "median_clv": 0.0,
        "clv_hit_rate": 0.0,
        "clv_sample_size": 0,
        "clv_sanity_passed": False,
        "track_concentration": 0.0,
        "month_concentration": 0.0,
        "validation_positive_months": 0,
        "validation_month_count": 0,
        "validation_positive_tracks": 0,
        "validation_track_count": 0,
        "mode_notes": "No validated strategy met the tier requirements.",
        "warning_flags": "no_valid_strategy",
        "warnings": ["no_valid_strategy", reason],
        "auc": 0.0,
        "brier_score": 0.0,
        "tier_score": -999.0,
        "meets_tier_min_bets": False,
        "meets_flat_roi_floor": False,
        "recommendation_valid": False,
        "recommendation_reason": reason,
    }


def _choose_tier_recommendation(results: pd.DataFrame, tier: dict[str, object]) -> dict[str, object]:
    viable = results[results["bets"] > 0].copy()
    if viable.empty:
        return _invalid_recommendation(tier, "no_positive_bet_config_found")

    eligible = results[
        (results["max_odds"] <= float(tier["max_odds_cap"]))
        & (results["flat_roi"] >= MIN_FLAT_ROI)
        & (results["flat_profit_positive"] == True)
        & (results["bets"] >= int(tier["min_bets"]))
        & (results["flat_drawdown"] <= float(tier["max_drawdown"]))
        & (results["validation_positive_months"] > 0)
        & (results["validation_positive_tracks"] > 0)
    ].copy()

    fallback_used = False
    if eligible.empty:
        eligible = viable[
            (viable["mode"] == "morning_model")
            & (viable["max_odds"] <= float(tier["max_odds_cap"]))
            & (viable["flat_roi"] >= MIN_FLAT_ROI)
            & (viable["flat_profit_positive"] == True)
            & (viable["bets"] >= max(10, int(tier["min_bets"] // 2)))
        ].copy()
        fallback_used = True

    if eligible.empty:
        eligible = viable[
            (viable["max_odds"] <= float(tier["max_odds_cap"]))
            & (viable["flat_roi"] >= MIN_FLAT_ROI)
            & (viable["flat_profit_positive"] == True)
            & (viable["bets"] >= max(5, int(tier["min_bets"] // 3)))
        ].copy()
        fallback_used = True

    if eligible.empty:
        eligible = viable[
            (viable["mode"] == "morning_model")
            & (viable["bets"] > 0)
        ].copy()
        fallback_used = True

    if eligible.empty:
        eligible = viable.copy()
        fallback_used = True

    scored = eligible.copy()
    scored["tier_score"] = scored.apply(lambda row: _result_score(row, tier), axis=1)
    scored = scored.sort_values(
        ["tier_score", "flat_roi", "bets", "pct_roi"],
        ascending=[False, False, False, False],
    )
    chosen = scored.iloc[0].to_dict()
    warnings = [flag for flag in str(chosen.get("warning_flags", "")).split(",") if flag]
    if fallback_used:
        warnings.append("tier_fallback_used")
    chosen["tier"] = tier["name"]
    chosen["warnings"] = sorted(set(warnings))
    chosen["meets_tier_min_bets"] = int(chosen.get("bets", 0)) >= int(tier["min_bets"])
    chosen["meets_flat_roi_floor"] = float(chosen.get("flat_roi", 0.0)) >= MIN_FLAT_ROI
    chosen["recommendation_valid"] = int(chosen.get("bets", 0)) > 0
    chosen["recommendation_reason"] = "fallback_nonzero_strategy" if fallback_used else "tier_requirements_met"
    return chosen


def _strategy_breakdowns(bets: pd.DataFrame, tier_payloads: list[dict[str, object]]) -> dict[str, pd.DataFrame]:
    selected_keys = {
        (
            payload["tier"],
            payload["mode"],
            payload["model_name"],
            float(payload["probability_threshold"]),
            float(payload["min_edge"]),
            float(payload["min_form_score"]),
            float(payload["max_odds"]),
            int(payload["max_bets_per_day"]),
        )
        for payload in tier_payloads
    }

    tagged = []
    for _, row in bets.iterrows():
        key = (
            row["tier"],
            row["mode"],
            row["model_name"],
            float(row["probability_threshold"]),
            float(row["min_edge"]),
            float(row["min_form_score"]),
            float(row["max_odds"]),
            int(row["max_bets_per_day"]),
        )
        if key in selected_keys:
            tagged.append(row)

    if not tagged:
        empty = pd.DataFrame()
        return {"monthly": empty, "track": empty, "odds": empty, "clv": empty}

    selected = pd.DataFrame(tagged).copy()
    selected["flat_profit_loss"] = selected.apply(
        lambda row: _profit_for_row(float(row["odds_used"]), 100.0, int(row["won_flag"])), axis=1
    )
    selected["odds_bucket"] = selected["odds_used"].map(odds_bucket_label)

    monthly = (
        selected.groupby(["tier", "month"], dropna=False)
        .agg(
            bets=("won_flag", "size"),
            wins=("won_flag", "sum"),
            flat_profit_loss=("flat_profit_loss", "sum"),
            average_odds=("odds_used", "mean"),
            average_edge=("edge_used", "mean"),
            average_clv=("clv_percent", "mean"),
        )
        .reset_index()
    )
    monthly["flat_roi"] = monthly["flat_profit_loss"] / (monthly["bets"] * 100.0)

    track = (
        selected.groupby(["tier", "track"], dropna=False)
        .agg(
            bets=("won_flag", "size"),
            wins=("won_flag", "sum"),
            flat_profit_loss=("flat_profit_loss", "sum"),
            average_odds=("odds_used", "mean"),
            average_edge=("edge_used", "mean"),
            average_clv=("clv_percent", "mean"),
        )
        .reset_index()
    )
    track["flat_roi"] = track["flat_profit_loss"] / (track["bets"] * 100.0)

    odds = (
        selected.groupby(["tier", "odds_bucket"], dropna=False)
        .agg(
            bets=("won_flag", "size"),
            wins=("won_flag", "sum"),
            flat_profit_loss=("flat_profit_loss", "sum"),
            average_odds=("odds_used", "mean"),
            average_edge=("edge_used", "mean"),
            average_clv=("clv_percent", "mean"),
        )
        .reset_index()
    )
    odds["flat_roi"] = odds["flat_profit_loss"] / (odds["bets"] * 100.0)

    clv = (
        selected.groupby(["tier"], dropna=False)
        .agg(
            clv_sample_size=("clv_percent", lambda values: pd.to_numeric(values, errors="coerce").notna().sum()),
            average_clv=("clv_percent", "mean"),
            median_clv=("clv_percent", "median"),
            positive_clv_rate=("clv_percent", lambda values: (pd.to_numeric(values, errors="coerce").dropna() > 0).mean() if pd.to_numeric(values, errors="coerce").dropna().size else 0.0),
            average_odds=("odds_used", "mean"),
        )
        .reset_index()
    )
    clv["clv_sanity_passed"] = (
        (clv["clv_sample_size"] >= 10) & (clv["median_clv"].abs() <= 35.0)
    )

    return {"monthly": monthly, "track": track, "odds": odds, "clv": clv}


def _live_candidate_from_recommendations(recommendations: list[dict[str, object]]) -> dict[str, object]:
    valid_rows = [row for row in recommendations if row.get("recommendation_valid") and int(row.get("bets", 0)) > 0]
    if not valid_rows:
        return {
            "decision_version": "model_edge_v3",
            "disabled": True,
            "disabled_reason": "no_valid_recommendation",
            "live_mode": "disabled",
            "max_bets_per_day": 3,
            "max_odds": 20.0,
            "min_edge": 0.0,
            "min_form_score": 0.3,
            "stake_pct": 0.005,
            "require_form_confirmation": True,
            "require_recent_history": True,
            "no_history_auto_bets": True,
            "watchlist_only_above_cap": True,
            "no_late_features_for_morning_bets": True,
            "warnings": ["no_valid_recommendation"],
            "notes": "No validated non-zero strategy was found. model_edge_v3 should remain disabled for live paper selection.",
        }

    morning_logistic = [
        row for row in valid_rows
        if row["mode"] == "morning_model" and row["model_name"] == "logistic"
    ]
    morning_any = [row for row in valid_rows if row["mode"] == "morning_model"]
    logistic_rows = [row for row in valid_rows if row["model_name"] == "logistic"]
    chosen = (
        (morning_logistic[0] if morning_logistic else None)
        or (morning_any[0] if morning_any else None)
        or (logistic_rows[0] if logistic_rows else None)
        or valid_rows[0]
    )
    live_max_odds = 20.0 if float(chosen["max_odds"]) <= 20.0 else 30.0
    mode_name = "morning_model" if chosen["mode"] == "morning_model" else "late_market_model"
    return {
        "decision_version": "model_edge_v3",
        "disabled": False,
        "source_tier": chosen["tier"],
        "source_model_name": chosen["model_name"],
        "source_mode": chosen["mode"],
        "live_mode": mode_name,
        "max_odds": live_max_odds,
        "max_bets_per_day": 3,
        "stake_pct": 0.005,
        "min_edge": max(float(chosen["min_edge"]), 0.0),
        "min_form_score": max(float(chosen["min_form_score"]), 0.30),
        "require_form_confirmation": True,
        "require_recent_history": True,
        "no_history_auto_bets": True,
        "watchlist_only_above_cap": True,
        "no_late_features_for_morning_bets": True,
        "warnings": chosen.get("warnings", []),
        "notes": (
            "Validated candidate only. Morning proposals remain leak-safe and do not rely on closing or late-only fields."
        ),
    }


def run_validation_suite(matched_path: Path = MATCHED_PATH) -> dict[str, pd.DataFrame]:
    frame = build_validation_frame(matched_path)
    train_frame, validation_frame = split_train_validation(frame)

    result_rows: list[dict[str, object]] = []
    selected_bets: list[pd.DataFrame] = []

    for mode_name, mode in MODEL_MODES.items():
        x_train = train_frame[list(mode.feature_columns)].apply(pd.to_numeric, errors="coerce")
        y_train = train_frame["won_flag"]
        x_validation = validation_frame[list(mode.feature_columns)].apply(pd.to_numeric, errors="coerce")
        y_validation = validation_frame["won_flag"]

        for model_name, model in build_candidate_models().items():
            model.fit(x_train, y_train)
            validation_probabilities = model.predict_proba(x_validation)[:, 1]
            scored_validation = validation_frame.copy()
            scored_validation["predicted_win_probability"] = validation_probabilities
            model_auc = roc_auc_score(y_validation, validation_probabilities) if len(y_validation.unique()) > 1 else 0.0
            model_brier = brier_score_loss(y_validation, validation_probabilities)

            for probability_threshold, min_edge, min_form_score, max_odds, max_bets_per_day in product(
                (0.01, 0.03, 0.05, 0.08, 0.10, 0.12, 0.15),
                (-0.05, -0.02, -0.01, 0.00, 0.01, 0.02, 0.03),
                (0.10, 0.15, 0.30, 0.45, 0.60),
                (20.0, 30.0, 50.0, 100.0),
                (3, 5),
            ):
                summary, bets = _simulate_validation_strategy(
                    scored_validation,
                    mode=mode,
                    model_name=model_name,
                    probability_threshold=probability_threshold,
                    min_edge=min_edge,
                    min_form_score=min_form_score,
                    max_odds=max_odds,
                    max_bets_per_day=max_bets_per_day,
                )
                summary["auc"] = round(float(model_auc), 4)
                summary["brier_score"] = round(float(model_brier), 4)
                result_rows.append(summary)
                if not bets.empty:
                    bets = bets.copy()
                    bets["tier"] = ""
                    bets["probability_threshold"] = probability_threshold
                    bets["min_edge"] = min_edge
                    bets["min_form_score"] = min_form_score
                    bets["max_odds"] = max_odds
                    bets["max_bets_per_day"] = max_bets_per_day
                    selected_bets.append(bets)

    results_frame = pd.DataFrame(result_rows)
    if results_frame.empty:
        raise RuntimeError("Validation suite produced no strategy results.")

    recommendations = [_choose_tier_recommendation(results_frame, tier) for tier in RECOMMENDATION_TIERS]
    recommendations_frame = pd.DataFrame(recommendations)

    all_bets_frame = pd.concat(selected_bets, ignore_index=True) if selected_bets else pd.DataFrame()
    if not all_bets_frame.empty:
        key_columns = ["mode", "model_name", "probability_threshold", "min_edge", "min_form_score", "max_odds", "max_bets_per_day"]
        tier_lookup = {
            (
                payload["mode"],
                payload["model_name"],
                float(payload["probability_threshold"]),
                float(payload["min_edge"]),
                float(payload["min_form_score"]),
                float(payload["max_odds"]),
                int(payload["max_bets_per_day"]),
            ): payload["tier"]
            for payload in recommendations
        }
        all_bets_frame["tier"] = all_bets_frame.apply(
            lambda row: tier_lookup.get(
                (
                    row["mode"],
                    row["model_name"],
                    float(row["probability_threshold"]),
                    float(row["min_edge"]),
                    float(row["min_form_score"]),
                    float(row["max_odds"]),
                    int(row["max_bets_per_day"]),
                ),
                "",
            ),
            axis=1,
        )

    breakdowns = _strategy_breakdowns(all_bets_frame, recommendations)
    live_candidate = _live_candidate_from_recommendations(recommendations)

    save_dataframe(results_frame, VALIDATION_RESULTS_PATH)
    save_dataframe(recommendations_frame, VALIDATION_RECOMMENDATIONS_PATH)
    save_dataframe(breakdowns["monthly"], VALIDATION_BREAKDOWN_MONTHLY_PATH)
    save_dataframe(breakdowns["track"], VALIDATION_BREAKDOWN_TRACK_PATH)
    save_dataframe(breakdowns["odds"], VALIDATION_BREAKDOWN_ODDS_PATH)
    save_dataframe(breakdowns["clv"], VALIDATION_CLV_PATH)

    json_dump(recommendations[0], CONSERVATIVE_CONFIG_PATH)
    json_dump(recommendations[1], BALANCED_CONFIG_PATH)
    json_dump(recommendations[2], AGGRESSIVE_CONFIG_PATH)
    json_dump(live_candidate, LIVE_CANDIDATE_PATH)
    json_dump(
        {
            "recommended_safe_strategy": recommendations[0],
            "recommended_balanced_strategy": recommendations[1],
            "recommended_aggressive_strategy": recommendations[2],
            "model_edge_v3_candidate": live_candidate,
        },
        LEGACY_OUTPUT_PATH,
    )

    print("Validation Recommendations")
    print(recommendations_frame.to_string(index=False))
    return {
        "results": results_frame,
        "recommendations": recommendations_frame,
        "monthly_breakdown": breakdowns["monthly"],
        "track_breakdown": breakdowns["track"],
        "odds_breakdown": breakdowns["odds"],
        "clv_diagnostics": breakdowns["clv"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run validation-led research recommendations for model_edge_v3.")
    parser.add_argument(
        "--matched-path",
        type=Path,
        default=MATCHED_PATH,
        help="Path to matched_runner_data.csv for validation reruns.",
    )
    args = parser.parse_args()
    run_validation_suite(args.matched_path)


if __name__ == "__main__":
    main()
