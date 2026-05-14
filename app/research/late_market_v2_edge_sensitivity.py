from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.betting.market_helpers import closing_line_metrics, odds_bucket_label
from app.research.late_market_v2_backtest import (
    MATCHED_PATH,
    ODDS_TIME_SERIES_PATH,
    _load_frame,
    _score_runners,
)
from app.research.utils import RESEARCH_REPORTS_DIR, compute_max_drawdown, save_dataframe
from app.strategy.late_market_v2_bets import (
    DECISION_VERSION,
    MAX_DAILY_BETS,
    MAX_MARKET_RANK,
    MAX_RUNNER_ODDS,
    MIN_COMBINED_SCORE,
    MIN_FORM_SCORE,
    MIN_MOVEMENT_SCORE,
    MIN_RUNNER_ODDS,
    _combined_score,
)

SUMMARY_PATH = RESEARCH_REPORTS_DIR / "late_v2_edge_sensitivity.csv"
NEAR_MISS_PATH = RESEARCH_REPORTS_DIR / "late_v2_edge_near_miss_analysis.csv"
BY_ODDS_PATH = RESEARCH_REPORTS_DIR / "late_v2_edge_sensitivity_by_odds.csv"
BY_RANK_PATH = RESEARCH_REPORTS_DIR / "late_v2_edge_sensitivity_by_rank.csv"
ODDS_3_TO_5_PATH = RESEARCH_REPORTS_DIR / "late_v2_edge_near_miss_odds_3_to_5.csv"
ODDS_3_TO_5_SUMMARY_PATH = RESEARCH_REPORTS_DIR / "late_v2_edge_near_miss_odds_3_to_5_summary.csv"
ODDS_3_TO_5_FILTER_TEST_PATH = RESEARCH_REPORTS_DIR / "late_v2_edge_near_miss_odds_3_to_5_filter_test.csv"
FLAT_STAKE = 100.0
MIN_SAMPLE_BETS = 20

VARIANTS = {
    "current_edge_filter": {"min_edge": -0.01},
    "relaxed_edge_filter": {"min_edge": -0.03},
    "very_relaxed_edge_filter": {"min_edge": -0.05},
    "soft_edge_only": {"min_edge": None},
    "hard_stop_only": {"min_edge": -0.08},
}


def _movement_bucket(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    if value < 0.45:
        return "<0.45"
    if value < 0.60:
        return "0.45-0.60"
    if value < 0.75:
        return "0.60-0.75"
    return "0.75+"


def _form_bucket(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    if value < 0.30:
        return "<0.30"
    if value < 0.50:
        return "0.30-0.50"
    if value < 0.70:
        return "0.50-0.70"
    return "0.70+"


def _safe_mean(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    value = pd.to_numeric(frame[column], errors="coerce").mean()
    return round(float(value), 4) if pd.notna(value) else 0.0


def _add_clv_columns(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    metrics = [
        closing_line_metrics(
            float(odds_taken) if pd.notna(odds_taken) else None,
            float(closing_odds) if pd.notna(closing_odds) else None,
        )
        for odds_taken, closing_odds in zip(working["latest_odds"], working["closing_price"])
    ]
    working["clv_percent"] = [item["clv_percent"] for item in metrics]
    return working


def _build_variant_frame(scored: pd.DataFrame, variant_name: str, min_edge: float | None) -> pd.DataFrame:
    working = scored.copy()
    working["variant_name"] = variant_name
    working["effective_edge_threshold"] = min_edge
    working["combined_score"] = working.apply(
        lambda row: _combined_score(
            float(row["movement_score"] or 0.0),
            float(row["edge"]) if pd.notna(row["edge"]) else None,
            float(row["form_score"] or 0.0),
        ),
        axis=1,
    )
    working["passes_form_history"] = working["has_history"]
    working["passes_recent_form"] = working["recent_form_qualifies"]
    working["passes_form_score"] = working["form_score"] >= MIN_FORM_SCORE
    working["passes_odds_band"] = (working["latest_odds"] >= MIN_RUNNER_ODDS) & (working["latest_odds"] <= MAX_RUNNER_ODDS)
    working["passes_market_rank"] = working["market_rank"].notna() & (working["market_rank"] <= MAX_MARKET_RANK)
    working["passes_movement_history"] = working["movement_history_ok"]
    working["passes_recent_drift"] = working["recent_drift"] != True
    working["passes_movement_score"] = working["movement_score"] >= MIN_MOVEMENT_SCORE
    working["passes_edge"] = True if min_edge is None else (working["edge"].notna() & (working["edge"] >= min_edge))
    working["passes_combined_score"] = working["combined_score"] >= MIN_COMBINED_SCORE
    working["eligible"] = (
        working["latest_odds"].notna()
        & working["passes_form_history"]
        & working["passes_recent_form"]
        & working["passes_form_score"]
        & working["passes_odds_band"]
        & working["passes_market_rank"]
        & working["passes_movement_history"]
        & working["passes_recent_drift"]
        & working["passes_movement_score"]
        & working["passes_edge"]
        & working["passes_combined_score"]
    )
    working["odds_bucket"] = working["latest_odds"].map(odds_bucket_label)
    working["movement_score_bucket"] = working["movement_score"].map(_movement_bucket)
    working["form_score_bucket"] = working["form_score"].map(_form_bucket)
    return working


def _simulate_flat_stake(eligible: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float | int]]:
    race_best = (
        eligible.sort_values(
            ["race_date", "track_norm", "race_number", "combined_score", "movement_score"],
            ascending=[True, True, True, False, False],
        )
        .groupby(["race_date", "track_norm", "race_number"], as_index=False, dropna=False)
        .head(1)
        .copy()
    )

    selected = (
        race_best.sort_values(
            ["race_date", "combined_score", "movement_score"],
            ascending=[True, False, False],
        )
        .groupby("race_date", as_index=False, dropna=False)
        .head(MAX_DAILY_BETS)
        .copy()
    )

    if selected.empty:
        return selected, {
            "total_bets": 0,
            "wins": 0,
            "losses": 0,
            "strike_rate": 0.0,
            "flat_profit_loss": 0.0,
            "flat_roi": 0.0,
            "max_drawdown": 0.0,
            "average_odds": 0.0,
            "average_edge": 0.0,
            "average_movement_score": 0.0,
            "average_form_score": 0.0,
            "average_combined_score": 0.0,
            "average_clv": 0.0,
        }

    selected = selected.copy()
    selected["stake"] = FLAT_STAKE
    selected["profit_loss"] = selected.apply(
        lambda row: round(
            ((float(row["latest_odds"]) - 1.0) * 0.92 * FLAT_STAKE)
            if int(row["won_flag"]) == 1
            else -FLAT_STAKE,
            2,
        ),
        axis=1,
    )
    bank = 10000.0
    bank_history = [bank]
    for profit in selected["profit_loss"].tolist():
        bank = round(bank + float(profit), 2)
        bank_history.append(bank)

    total_bets = len(selected)
    wins = int(selected["won_flag"].sum())
    losses = total_bets - wins
    total_staked = FLAT_STAKE * total_bets
    profit_loss = round(float(selected["profit_loss"].sum()), 2)
    summary = {
        "total_bets": total_bets,
        "wins": wins,
        "losses": losses,
        "strike_rate": round(wins / total_bets, 4) if total_bets else 0.0,
        "flat_profit_loss": profit_loss,
        "flat_roi": round(profit_loss / total_staked, 4) if total_staked else 0.0,
        "max_drawdown": compute_max_drawdown(bank_history),
        "average_odds": _safe_mean(selected, "latest_odds"),
        "average_edge": _safe_mean(selected, "edge"),
        "average_movement_score": _safe_mean(selected, "movement_score"),
        "average_form_score": _safe_mean(selected, "form_score"),
        "average_combined_score": _safe_mean(selected, "combined_score"),
        "average_clv": _safe_mean(selected, "clv_percent"),
    }
    return selected, summary


def _build_group_breakdown(selected: pd.DataFrame, variant_name: str, label_column: str, report_type: str) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame(
            columns=[
                "variant_name",
                "report_type",
                "bucket",
                "total_bets",
                "wins",
                "losses",
                "strike_rate",
                "flat_profit_loss",
                "flat_roi",
                "average_odds",
                "average_edge",
                "average_movement_score",
                "average_form_score",
                "average_combined_score",
                "average_clv",
            ]
        )

    grouped = (
        selected.groupby(label_column, dropna=False)
        .agg(
            total_bets=("won_flag", "size"),
            wins=("won_flag", "sum"),
            flat_profit_loss=("profit_loss", "sum"),
            average_odds=("latest_odds", "mean"),
            average_edge=("edge", "mean"),
            average_movement_score=("movement_score", "mean"),
            average_form_score=("form_score", "mean"),
            average_combined_score=("combined_score", "mean"),
            average_clv=("clv_percent", "mean"),
        )
        .reset_index()
    )
    grouped["variant_name"] = variant_name
    grouped["report_type"] = report_type
    grouped["bucket"] = grouped[label_column].astype(str)
    grouped["losses"] = grouped["total_bets"] - grouped["wins"]
    grouped["strike_rate"] = (grouped["wins"] / grouped["total_bets"]).round(4)
    grouped["flat_roi"] = (grouped["flat_profit_loss"] / (grouped["total_bets"] * FLAT_STAKE)).round(4)
    return grouped[
        [
            "variant_name",
            "report_type",
            "bucket",
            "total_bets",
            "wins",
            "losses",
            "strike_rate",
            "flat_profit_loss",
            "flat_roi",
            "average_odds",
            "average_edge",
            "average_movement_score",
            "average_form_score",
            "average_combined_score",
            "average_clv",
        ]
    ]


def _build_current_edge_near_miss_analysis(current_variant: pd.DataFrame) -> pd.DataFrame:
    working = current_variant.copy()
    failure_masks = {
        "missing_latest_odds": working["latest_odds"].isna(),
        "missing_form_history": ~working["passes_form_history"],
        "poor_recent_form": ~working["passes_recent_form"],
        "form_score_too_low": ~working["passes_form_score"],
        "odds_band": ~working["passes_odds_band"],
        "market_rank_too_low": ~working["passes_market_rank"],
        "missing_late_movement": ~working["passes_movement_history"],
        "recent_drift": ~working["passes_recent_drift"],
        "movement_score_too_low": ~working["passes_movement_score"],
        "edge_too_negative": ~working["passes_edge"],
        "combined_score_too_low": ~working["passes_combined_score"],
    }
    for reason, mask in failure_masks.items():
        working[f"fails_{reason}"] = mask.astype(int)
    failure_columns = [f"fails_{reason}" for reason in failure_masks]
    working["failure_count"] = working[failure_columns].sum(axis=1)
    working["single_fail_reason"] = working.apply(
        lambda row: next((reason for reason in failure_masks if row[f"fails_{reason}"] == 1), None)
        if row["failure_count"] == 1
        else None,
        axis=1,
    )

    near_misses = working[
        (working["single_fail_reason"] == "edge_too_negative")
        & (~working["eligible"])
    ].copy()

    if near_misses.empty:
        return pd.DataFrame()

    near_misses["profit_loss"] = near_misses.apply(
        lambda row: round(
            ((float(row["latest_odds"]) - 1.0) * 0.92 * FLAT_STAKE)
            if int(row["won_flag"]) == 1
            else -FLAT_STAKE,
            2,
        ),
        axis=1,
    )

    summary_rows = [
        {
            "report_section": "summary",
            "bucket_type": "all",
            "bucket_value": "all",
            "runner_count": int(len(near_misses)),
            "wins": int(near_misses["won_flag"].sum()),
            "losses": int(len(near_misses) - int(near_misses["won_flag"].sum())),
            "win_rate": round(float(near_misses["won_flag"].mean()), 4),
            "flat_profit_loss": round(float(near_misses["profit_loss"].sum()), 2),
            "flat_roi": round(float(near_misses["profit_loss"].sum()) / (len(near_misses) * FLAT_STAKE), 4),
            "average_odds": _safe_mean(near_misses, "latest_odds"),
            "average_edge": _safe_mean(near_misses, "edge"),
            "average_movement_score": _safe_mean(near_misses, "movement_score"),
            "average_form_score": _safe_mean(near_misses, "form_score"),
            "average_combined_score": _safe_mean(near_misses, "combined_score"),
            "average_clv": _safe_mean(near_misses, "clv_percent"),
        }
    ]

    def build_distribution(label_column: str, section: str) -> list[dict[str, object]]:
        grouped = (
            near_misses.groupby(label_column, dropna=False)
            .agg(
                runner_count=("won_flag", "size"),
                wins=("won_flag", "sum"),
                flat_profit_loss=("profit_loss", "sum"),
                average_odds=("latest_odds", "mean"),
                average_edge=("edge", "mean"),
                average_movement_score=("movement_score", "mean"),
                average_form_score=("form_score", "mean"),
                average_combined_score=("combined_score", "mean"),
                average_clv=("clv_percent", "mean"),
            )
            .reset_index()
        )
        rows = []
        for _, row in grouped.iterrows():
            rows.append(
                {
                    "report_section": section,
                    "bucket_type": section,
                    "bucket_value": str(row[label_column]),
                    "runner_count": int(row["runner_count"]),
                    "wins": int(row["wins"]),
                    "losses": int(row["runner_count"] - row["wins"]),
                    "win_rate": round(float(row["wins"] / row["runner_count"]), 4) if row["runner_count"] else 0.0,
                    "flat_profit_loss": round(float(row["flat_profit_loss"]), 2),
                    "flat_roi": round(float(row["flat_profit_loss"]) / (float(row["runner_count"]) * FLAT_STAKE), 4)
                    if row["runner_count"]
                    else 0.0,
                    "average_odds": round(float(row["average_odds"]), 4) if pd.notna(row["average_odds"]) else 0.0,
                    "average_edge": round(float(row["average_edge"]), 4) if pd.notna(row["average_edge"]) else 0.0,
                    "average_movement_score": round(float(row["average_movement_score"]), 4) if pd.notna(row["average_movement_score"]) else 0.0,
                    "average_form_score": round(float(row["average_form_score"]), 4) if pd.notna(row["average_form_score"]) else 0.0,
                    "average_combined_score": round(float(row["average_combined_score"]), 4) if pd.notna(row["average_combined_score"]) else 0.0,
                    "average_clv": round(float(row["average_clv"]), 4) if pd.notna(row["average_clv"]) else 0.0,
                }
            )
        return rows

    near_misses["odds_bucket"] = near_misses["latest_odds"].map(odds_bucket_label)
    near_misses["market_rank_bucket"] = near_misses["market_rank"].fillna(-1).astype(int).astype(str)
    summary_rows.extend(build_distribution("odds_bucket", "odds_distribution"))
    summary_rows.extend(build_distribution("market_rank_bucket", "market_rank_distribution"))
    return pd.DataFrame(summary_rows)


def _build_odds_3_to_5_focus_report(current_variant: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    working = current_variant.copy()
    failure_masks = {
        "missing_latest_odds": working["latest_odds"].isna(),
        "missing_form_history": ~working["passes_form_history"],
        "poor_recent_form": ~working["passes_recent_form"],
        "form_score_too_low": ~working["passes_form_score"],
        "odds_band": ~working["passes_odds_band"],
        "market_rank_too_low": ~working["passes_market_rank"],
        "missing_late_movement": ~working["passes_movement_history"],
        "recent_drift": ~working["passes_recent_drift"],
        "movement_score_too_low": ~working["passes_movement_score"],
        "edge_too_negative": ~working["passes_edge"],
        "combined_score_too_low": ~working["passes_combined_score"],
    }
    for reason, mask in failure_masks.items():
        working[f"fails_{reason}"] = mask.astype(int)
    failure_columns = [f"fails_{reason}" for reason in failure_masks]
    working["failure_count"] = working[failure_columns].sum(axis=1)
    working["single_fail_reason"] = working.apply(
        lambda row: next((reason for reason in failure_masks if row[f"fails_{reason}"] == 1), None)
        if row["failure_count"] == 1
        else None,
        axis=1,
    )
    working["odds_bucket"] = working["latest_odds"].map(odds_bucket_label)
    working["movement_score_bucket"] = working["movement_score"].map(_movement_bucket)
    working["form_score_bucket"] = working["form_score"].map(_form_bucket)
    working["profit_loss"] = working.apply(
        lambda row: round(
            ((float(row["latest_odds"]) - 1.0) * 0.92 * FLAT_STAKE)
            if int(row["won_flag"]) == 1
            else -FLAT_STAKE,
            2,
        ),
        axis=1,
    )

    focus = working[
        (working["single_fail_reason"] == "edge_too_negative")
        & (working["odds_bucket"] == "3-5")
    ].copy()
    if focus.empty:
        return pd.DataFrame(), pd.DataFrame()

    detailed_columns = [
        "race_date",
        "track",
        "race_number",
        "horse_name",
        "latest_odds",
        "market_rank",
        "movement_score",
        "movement_score_bucket",
        "form_score",
        "form_score_bucket",
        "edge",
        "combined_score",
        "won_flag",
        "profit_loss",
        "price_60m",
        "price_30m",
        "price_10m",
        "price_5m",
        "price_3m_aligned",
        "price_1m_aligned",
        "qualification_reason",
    ]
    detail_frame = focus[detailed_columns].sort_values(
        ["combined_score", "movement_score", "form_score", "edge"],
        ascending=[False, False, False, False],
    )

    summary_rows = [
        {
            "segment": "overall",
            "bucket": "3-5_edge_single_fail",
            "runner_count": int(len(focus)),
            "wins": int(focus["won_flag"].sum()),
            "losses": int(len(focus) - int(focus["won_flag"].sum())),
            "win_rate": round(float(focus["won_flag"].mean()), 4),
            "flat_profit_loss": round(float(focus["profit_loss"].sum()), 2),
            "flat_roi": round(float(focus["profit_loss"].sum()) / (len(focus) * FLAT_STAKE), 4),
            "average_odds": _safe_mean(focus, "latest_odds"),
            "average_market_rank": _safe_mean(focus, "market_rank"),
            "average_movement_score": _safe_mean(focus, "movement_score"),
            "average_form_score": _safe_mean(focus, "form_score"),
            "average_edge": _safe_mean(focus, "edge"),
            "average_combined_score": _safe_mean(focus, "combined_score"),
            "average_clv": _safe_mean(focus, "clv_percent"),
        }
    ]

    for label, column in (
        ("market_rank", "market_rank"),
        ("movement_score_bucket", "movement_score_bucket"),
        ("form_score_bucket", "form_score_bucket"),
    ):
        grouped = (
            focus.groupby(column, dropna=False)
            .agg(
                runner_count=("won_flag", "size"),
                wins=("won_flag", "sum"),
                flat_profit_loss=("profit_loss", "sum"),
                average_odds=("latest_odds", "mean"),
                average_market_rank=("market_rank", "mean"),
                average_movement_score=("movement_score", "mean"),
                average_form_score=("form_score", "mean"),
                average_edge=("edge", "mean"),
                average_combined_score=("combined_score", "mean"),
                average_clv=("clv_percent", "mean"),
            )
            .reset_index()
        )
        for _, row in grouped.iterrows():
            summary_rows.append(
                {
                    "segment": label,
                    "bucket": str(row[column]),
                    "runner_count": int(row["runner_count"]),
                    "wins": int(row["wins"]),
                    "losses": int(row["runner_count"] - row["wins"]),
                    "win_rate": round(float(row["wins"] / row["runner_count"]), 4) if row["runner_count"] else 0.0,
                    "flat_profit_loss": round(float(row["flat_profit_loss"]), 2),
                    "flat_roi": round(float(row["flat_profit_loss"]) / (float(row["runner_count"]) * FLAT_STAKE), 4)
                    if row["runner_count"]
                    else 0.0,
                    "average_odds": round(float(row["average_odds"]), 4) if pd.notna(row["average_odds"]) else 0.0,
                    "average_market_rank": round(float(row["average_market_rank"]), 4) if pd.notna(row["average_market_rank"]) else 0.0,
                    "average_movement_score": round(float(row["average_movement_score"]), 4) if pd.notna(row["average_movement_score"]) else 0.0,
                    "average_form_score": round(float(row["average_form_score"]), 4) if pd.notna(row["average_form_score"]) else 0.0,
                    "average_edge": round(float(row["average_edge"]), 4) if pd.notna(row["average_edge"]) else 0.0,
                    "average_combined_score": round(float(row["average_combined_score"]), 4) if pd.notna(row["average_combined_score"]) else 0.0,
                    "average_clv": round(float(row["average_clv"]), 4) if pd.notna(row["average_clv"]) else 0.0,
                }
            )

    return detail_frame, pd.DataFrame(summary_rows)


def _build_odds_3_to_5_filter_test(current_variant: pd.DataFrame) -> pd.DataFrame:
    working = current_variant.copy()
    failure_masks = {
        "missing_latest_odds": working["latest_odds"].isna(),
        "missing_form_history": ~working["passes_form_history"],
        "poor_recent_form": ~working["passes_recent_form"],
        "form_score_too_low": ~working["passes_form_score"],
        "odds_band": ~working["passes_odds_band"],
        "market_rank_too_low": ~working["passes_market_rank"],
        "missing_late_movement": ~working["passes_movement_history"],
        "recent_drift": ~working["passes_recent_drift"],
        "movement_score_too_low": ~working["passes_movement_score"],
        "edge_too_negative": ~working["passes_edge"],
        "combined_score_too_low": ~working["passes_combined_score"],
    }
    for reason, mask in failure_masks.items():
        working[f"fails_{reason}"] = mask.astype(int)
    failure_columns = [f"fails_{reason}" for reason in failure_masks]
    working["failure_count"] = working[failure_columns].sum(axis=1)
    working["single_fail_reason"] = working.apply(
        lambda row: next((reason for reason in failure_masks if row[f"fails_{reason}"] == 1), None)
        if row["failure_count"] == 1
        else None,
        axis=1,
    )
    working["odds_bucket"] = working["latest_odds"].map(odds_bucket_label)
    working["profit_loss"] = working.apply(
        lambda row: round(
            ((float(row["latest_odds"]) - 1.0) * 0.92 * FLAT_STAKE)
            if int(row["won_flag"]) == 1
            else -FLAT_STAKE,
            2,
        ),
        axis=1,
    )

    focus = working[
        (working["single_fail_reason"] == "edge_too_negative")
        & (working["odds_bucket"] == "3-5")
    ].copy()
    if focus.empty:
        return pd.DataFrame()

    focus["movement_tier"] = focus["movement_score"].map(
        lambda value: "movement>=0.75" if pd.notna(value) and value >= 0.75 else "movement<0.75"
    )
    focus["rank_tier"] = focus["market_rank"].map(
        lambda value: "rank4-5"
        if pd.notna(value) and 4 <= float(value) <= 5
        else "rank6-8"
        if pd.notna(value) and 6 <= float(value) <= 8
        else "other_rank"
    )
    focus["segment"] = focus["movement_tier"] + " | " + focus["rank_tier"]

    def summarize(frame: pd.DataFrame, label: str) -> dict[str, object]:
        count = len(frame)
        wins = int(frame["won_flag"].sum()) if count else 0
        profit = round(float(frame["profit_loss"].sum()), 2) if count else 0.0
        return {
            "segment": label,
            "runner_count": count,
            "wins": wins,
            "losses": count - wins,
            "win_rate": round(wins / count, 4) if count else 0.0,
            "flat_profit_loss": profit,
            "flat_roi": round(profit / (count * FLAT_STAKE), 4) if count else 0.0,
            "average_odds": _safe_mean(frame, "latest_odds"),
            "average_market_rank": _safe_mean(frame, "market_rank"),
            "average_movement_score": _safe_mean(frame, "movement_score"),
            "average_form_score": _safe_mean(frame, "form_score"),
            "average_edge": _safe_mean(frame, "edge"),
            "average_combined_score": _safe_mean(frame, "combined_score"),
            "average_clv": _safe_mean(frame, "clv_percent"),
        }

    rows = [
        summarize(focus, "all_3to5_edge_single_fail"),
        summarize(focus[focus["movement_score"] >= 0.75], "movement>=0.75"),
        summarize(focus[focus["market_rank"].between(4, 5, inclusive="both")], "rank4-5"),
        summarize(focus[focus["market_rank"].between(6, 8, inclusive="both")], "rank6-8"),
        summarize(
            focus[
                (focus["movement_score"] >= 0.75)
                & (focus["market_rank"].between(4, 5, inclusive="both"))
            ],
            "movement>=0.75 & rank4-5",
        ),
        summarize(
            focus[
                (focus["movement_score"] >= 0.75)
                & (focus["market_rank"].between(6, 8, inclusive="both"))
            ],
            "movement>=0.75 & rank6-8",
        ),
        summarize(
            focus[
                (focus["movement_score"] < 0.75)
                & (focus["market_rank"].between(4, 5, inclusive="both"))
            ],
            "movement<0.75 & rank4-5",
        ),
        summarize(
            focus[
                (focus["movement_score"] < 0.75)
                & (focus["market_rank"].between(6, 8, inclusive="both"))
            ],
            "movement<0.75 & rank6-8",
        ),
    ]

    grouped = (
        focus.groupby("segment", dropna=False)
        .apply(lambda frame: pd.Series(summarize(frame, frame.name)))
        .reset_index(drop=True)
    )
    return pd.concat([pd.DataFrame(rows), grouped], ignore_index=True).drop_duplicates(subset=["segment"])


def run_edge_sensitivity() -> dict[str, pd.DataFrame]:
    frame, _ = _load_frame(MATCHED_PATH, ODDS_TIME_SERIES_PATH)
    scored = _add_clv_columns(_score_runners(frame))

    summary_rows: list[dict[str, object]] = []
    breakdown_frames: list[pd.DataFrame] = []
    variant_frames: dict[str, pd.DataFrame] = {}

    for variant_name, settings in VARIANTS.items():
        variant_frame = _build_variant_frame(scored, variant_name, settings["min_edge"])
        eligible = variant_frame[variant_frame["eligible"]].copy()
        selected, metrics = _simulate_flat_stake(eligible)
        variant_frames[variant_name] = variant_frame
        summary_rows.append(
            {
                "variant_name": variant_name,
                "edge_rule": "soft_edge_only" if settings["min_edge"] is None else f"edge>={settings['min_edge']:.2f}",
                **metrics,
            }
        )
        breakdown_frames.append(_build_group_breakdown(selected, variant_name, "odds_bucket", "odds_bucket"))
        breakdown_frames.append(_build_group_breakdown(selected, variant_name, "market_rank", "market_rank"))
        breakdown_frames.append(_build_group_breakdown(selected, variant_name, "movement_score_bucket", "movement_score_bucket"))
        breakdown_frames.append(_build_group_breakdown(selected, variant_name, "form_score_bucket", "form_score_bucket"))

    summary_frame = pd.DataFrame(summary_rows).sort_values(
        ["flat_roi", "flat_profit_loss", "total_bets"],
        ascending=[False, False, False],
    )
    breakdown_frame = pd.concat(breakdown_frames, ignore_index=True) if breakdown_frames else pd.DataFrame()
    near_miss_frame = _build_current_edge_near_miss_analysis(variant_frames["current_edge_filter"])
    odds_3_to_5_detail, odds_3_to_5_summary = _build_odds_3_to_5_focus_report(
        variant_frames["current_edge_filter"]
    )
    odds_3_to_5_filter_test = _build_odds_3_to_5_filter_test(
        variant_frames["current_edge_filter"]
    )

    sensitivity_export = pd.concat(
        [
            summary_frame.assign(report_type="variant_summary"),
            breakdown_frame.assign(report_type=breakdown_frame.get("report_type", "breakdown")),
        ],
        ignore_index=True,
        sort=False,
    )

    save_dataframe(sensitivity_export, SUMMARY_PATH)
    save_dataframe(near_miss_frame, NEAR_MISS_PATH)
    save_dataframe(
        breakdown_frame[breakdown_frame["report_type"] == "odds_bucket"].copy()
        if not breakdown_frame.empty
        else pd.DataFrame(),
        BY_ODDS_PATH,
    )
    save_dataframe(
        breakdown_frame[breakdown_frame["report_type"] == "market_rank"].copy()
        if not breakdown_frame.empty
        else pd.DataFrame(),
        BY_RANK_PATH,
    )
    save_dataframe(odds_3_to_5_detail, ODDS_3_TO_5_PATH)
    save_dataframe(odds_3_to_5_summary, ODDS_3_TO_5_SUMMARY_PATH)
    save_dataframe(odds_3_to_5_filter_test, ODDS_3_TO_5_FILTER_TEST_PATH)

    best_variant = summary_frame.iloc[0]
    safest_variant = summary_frame.sort_values(
        ["max_drawdown", "flat_roi", "total_bets"],
        ascending=[True, False, False],
    ).iloc[0]

    current_row = summary_frame[summary_frame["variant_name"] == "current_edge_filter"].iloc[0]
    relaxed_rows = summary_frame[
        summary_frame["variant_name"].isin(
            ["relaxed_edge_filter", "very_relaxed_edge_filter", "soft_edge_only", "hard_stop_only"]
        )
    ].copy()
    relaxed_beating_current = relaxed_rows[
        (relaxed_rows["flat_roi"] > current_row["flat_roi"])
        & (relaxed_rows["total_bets"] >= MIN_SAMPLE_BETS)
    ]

    print("Late V2 Edge Sensitivity Summary")
    print(summary_frame.to_string(index=False))
    if not near_miss_frame.empty:
        print("\nEdge Near-Miss Analysis")
        print(near_miss_frame.head(12).to_string(index=False))
    if not odds_3_to_5_summary.empty:
        print("\nOdds 3-5 Edge Near-Miss Focus")
        print(odds_3_to_5_summary.head(12).to_string(index=False))
    if not odds_3_to_5_filter_test.empty:
        print("\nOdds 3-5 Filter Test")
        print(odds_3_to_5_filter_test.to_string(index=False))
    print(
        f"\nBEST VARIANT BY VALIDATION ROI: {best_variant['variant_name']} | "
        f"ROI={best_variant['flat_roi']:.4f} | bets={int(best_variant['total_bets'])}"
    )
    print(
        f"SAFEST VARIANT BY DRAWDOWN: {safest_variant['variant_name']} | "
        f"max_drawdown={safest_variant['max_drawdown']:.4f} | ROI={safest_variant['flat_roi']:.4f}"
    )
    if relaxed_beating_current.empty:
        print(
            f"NO RELAXED-EDGE VARIANT BEATS THE CURRENT RULE WITH SAMPLE SIZE >= {MIN_SAMPLE_BETS} BETS"
        )
    else:
        print(
            "RELAXED-EDGE VARIANTS BEATING CURRENT RULE WITH ENOUGH SAMPLE:"
        )
        print(
            relaxed_beating_current[
                ["variant_name", "total_bets", "flat_roi", "flat_profit_loss", "max_drawdown"]
            ].to_string(index=False)
        )

    return {
        "summary": summary_frame,
        "breakdowns": breakdown_frame,
        "near_misses": near_miss_frame,
        "odds_3_to_5_detail": odds_3_to_5_detail,
        "odds_3_to_5_summary": odds_3_to_5_summary,
        "odds_3_to_5_filter_test": odds_3_to_5_filter_test,
    }


def main() -> None:
    run_edge_sensitivity()


if __name__ == "__main__":
    main()
