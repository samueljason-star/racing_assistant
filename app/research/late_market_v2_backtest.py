from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from app.betting.market_helpers import commission_adjusted_market_probability
from app.config import BETFAIR_COMMISSION_RATE
from app.research.form_score_optimizer import prepare_form_features
from app.research.utils import (
    RESEARCH_DATA_DIR,
    RESEARCH_REPORTS_DIR,
    assign_market_rank,
    attach_common_labels,
    compute_max_drawdown,
    parse_float,
    parse_int,
    save_dataframe,
)
from app.strategy.late_market_v2_bets import (
    DECISION_VERSION,
    MAX_DAILY_BETS,
    MAX_MARKET_RANK,
    MAX_RUNNER_ODDS,
    MIN_COMBINED_SCORE,
    MIN_EDGE,
    MIN_FORM_SCORE,
    MIN_MOVEMENT_SCORE,
    MIN_RUNNER_ODDS,
    _combined_score,
    _movement_metrics,
)

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
ODDS_TIME_SERIES_PATH = RESEARCH_DATA_DIR / "odds_time_series.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "late_market_v2_backtest_summary.csv"
BETS_PATH = RESEARCH_REPORTS_DIR / "late_market_v2_backtest_bets.csv"
DAILY_PATH = RESEARCH_REPORTS_DIR / "late_market_v2_backtest_daily.csv"
NOTES_PATH = RESEARCH_REPORTS_DIR / "late_market_v2_backtest_notes.json"
NEAR_MISS_PATH = RESEARCH_REPORTS_DIR / "late_market_v2_near_misses.csv"
NEAR_MISS_SUMMARY_PATH = RESEARCH_REPORTS_DIR / "late_market_v2_near_miss_summary.csv"
STARTING_BANK = 10000.0
STAKE_PCT = 0.005
COMMISSION_RATE = BETFAIR_COMMISSION_RATE


def _safe_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _series_or_nan(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return _safe_float(frame[column])
    return pd.Series([float("nan")] * len(frame), index=frame.index, dtype="float64")


def _clean_horse_name(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace(r"^\d+\s*[\.\-]?\s*", "", regex=True)
        .str.strip()
        .str.lower()
    )


def _parse_pipe_numbers(value: object) -> list[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    values = []
    for part in str(value).split("|"):
        number = parse_float(part)
        if number is not None:
            values.append(number)
    return values


def _build_recent_form_proxy(row: pd.Series) -> dict[str, object]:
    last_start_finish = parse_int(row.get("last_start_finish"))
    finish_values = _parse_pipe_numbers(row.get("last_3_finishes"))
    margin_values = _parse_pipe_numbers(row.get("last_3_margins"))

    avg_last3_finish = (
        round(sum(finish_values) / len(finish_values), 2) if finish_values else None
    )
    avg_last3_margin = (
        round(sum(margin_values) / len(margin_values), 2) if margin_values else None
    )
    has_recent_placing = any(value <= 3 for value in finish_values)
    has_recent_margin = any(value <= 3.0 for value in margin_values)

    reasons = []
    if avg_last3_finish is not None and avg_last3_finish <= 6:
        reasons.append("avg_finish<=6")
    if last_start_finish is not None and last_start_finish <= 4:
        reasons.append("last_start<=4")
    if has_recent_margin:
        reasons.append("recent_margin<=3")
    if has_recent_placing:
        reasons.append("recent_placing")

    finish_component = (
        0.0
        if avg_last3_finish is None
        else max(0.0, min(1.0, (10.0 - avg_last3_finish) / 9.0))
    )
    last_start_component = (
        0.0
        if last_start_finish is None
        else max(0.0, min(1.0, (10.0 - last_start_finish) / 9.0))
    )
    margin_component = (
        0.0
        if avg_last3_margin is None
        else max(0.0, min(1.0, (6.0 - avg_last3_margin) / 6.0))
    )
    placing_bonus = 0.2 if has_recent_placing else 0.0
    form_score = min(
        1.0,
        round(
            (last_start_component * 0.45)
            + (finish_component * 0.35)
            + (margin_component * 0.20)
            + placing_bonus,
            4,
        ),
    )

    return {
        "has_history": bool(last_start_finish is not None or finish_values or margin_values),
        "history_row_count": max(len(finish_values), len(margin_values), 1 if last_start_finish is not None else 0),
        "qualifies": bool(reasons),
        "qualification_reason": ", ".join(reasons) if reasons else "poor_recent_form",
        "form_score": form_score,
        "last_start_finish": last_start_finish,
        "avg_last3_finish": avg_last3_finish,
        "avg_last3_margin": avg_last3_margin,
    }


def _build_market_snapshot(row: pd.Series) -> dict[str, object]:
    latest_odds = row.get("latest_odds")
    opening_odds = row.get("opening_price")
    odds_60m = row.get("price_60m")
    odds_30m = row.get("price_30m")
    odds_10m = row.get("price_10m")
    odds_5m = row.get("price_5m")

    return {
        "latest_odds": latest_odds,
        "latest_odds_timestamp": None,
        "opening_odds": opening_odds,
        "odds_60m": odds_60m,
        "odds_30m": odds_30m,
        "odds_10m": odds_10m,
        "odds_5m": odds_5m,
        "odds_3m": None,
        "open_to_current": (opening_odds - latest_odds) if pd.notna(opening_odds) and pd.notna(latest_odds) else None,
        "60_to_current": (odds_60m - latest_odds) if pd.notna(odds_60m) and pd.notna(latest_odds) else None,
        "30_to_current": (odds_30m - latest_odds) if pd.notna(odds_30m) and pd.notna(latest_odds) else None,
        "10_to_current": (odds_10m - latest_odds) if pd.notna(odds_10m) and pd.notna(latest_odds) else None,
        "5_to_current": (odds_5m - latest_odds) if pd.notna(odds_5m) and pd.notna(latest_odds) else None,
        "3_to_current": None,
    }


def _merge_aligned_odds(frame: pd.DataFrame, odds_time_series_path: Path) -> tuple[pd.DataFrame, dict[str, int]]:
    if not odds_time_series_path.exists():
        return frame, {"aligned_rows": 0, "aligned_matches": 0}

    aligned = pd.read_csv(odds_time_series_path, low_memory=False)
    if aligned.empty:
        return frame, {"aligned_rows": 0, "aligned_matches": 0}

    aligned = aligned.copy()
    aligned["race_date"] = pd.to_datetime(aligned["jump_time"], errors="coerce").dt.date.astype(str)
    aligned["horse_name_norm_for_merge"] = _clean_horse_name(aligned["horse_name"])
    aligned["track_norm_for_merge"] = aligned["track"].astype(str).str.strip().str.lower()
    aligned["race_number_for_merge"] = pd.to_numeric(aligned["race_number"], errors="coerce")

    working = frame.copy()
    working["horse_name_norm_for_merge"] = _clean_horse_name(working["horse_name"])
    working["track_norm_for_merge"] = working["track"].astype(str).str.strip().str.lower()
    working["race_number_for_merge"] = pd.to_numeric(working["race_number"], errors="coerce")

    merged = working.merge(
        aligned[
            [
                "race_date",
                "track_norm_for_merge",
                "race_number_for_merge",
                "horse_name_norm_for_merge",
                "odds_60m",
                "odds_30m",
                "odds_10m",
                "odds_5m",
                "odds_3m",
                "odds_1m",
                "latest_odds",
                "complete_time_series_flag",
            ]
        ],
        on=["race_date", "track_norm_for_merge", "race_number_for_merge", "horse_name_norm_for_merge"],
        how="left",
        suffixes=("", "_aligned"),
    )

    aligned_latest_source = "latest_odds_aligned" if "latest_odds_aligned" in merged.columns else "latest_odds"
    aligned_latest = _series_or_nan(merged, "odds_1m").combine_first(
        _series_or_nan(merged, "odds_3m")
    ).combine_first(
        _series_or_nan(merged, "odds_5m")
    ).combine_first(
        _series_or_nan(merged, "odds_10m")
    ).combine_first(
        _series_or_nan(merged, aligned_latest_source)
    )

    merged["opening_price"] = _series_or_nan(merged, "opening_price")
    merged["price_60m"] = _series_or_nan(merged, "odds_60m").combine_first(_series_or_nan(merged, "price_60m"))
    merged["price_30m"] = _series_or_nan(merged, "odds_30m").combine_first(_series_or_nan(merged, "price_30m"))
    merged["price_10m"] = _series_or_nan(merged, "odds_10m").combine_first(_series_or_nan(merged, "price_10m"))
    merged["price_5m"] = _series_or_nan(merged, "odds_5m").combine_first(_series_or_nan(merged, "price_5m"))
    merged["price_3m_aligned"] = _series_or_nan(merged, "odds_3m")
    merged["price_1m_aligned"] = _series_or_nan(merged, "odds_1m")
    merged["latest_odds"] = aligned_latest.combine_first(_safe_float(merged.get("closing_price")))

    aligned_matches = int(_series_or_nan(merged, aligned_latest_source).notna().sum())
    return merged, {"aligned_rows": int(len(aligned)), "aligned_matches": aligned_matches}


def _load_frame(matched_path: Path, odds_time_series_path: Path) -> tuple[pd.DataFrame, dict[str, int]]:
    frame = pd.read_csv(matched_path, low_memory=False)
    frame = attach_common_labels(frame)
    frame = prepare_form_features(frame)
    frame, merge_stats = _merge_aligned_odds(frame, odds_time_series_path)

    for column in (
        "opening_price",
        "price_60m",
        "price_30m",
        "price_10m",
        "price_5m",
        "closing_price",
        "won_flag",
    ):
        if column in frame.columns:
            frame[column] = _safe_float(frame[column])

    frame["latest_odds"] = _safe_float(frame.get("latest_odds")).combine_first(
        _safe_float(frame.get("price_1m_aligned"))
    ).combine_first(
        _safe_float(frame.get("price_3m_aligned"))
    ).combine_first(
        _safe_float(frame.get("price_5m"))
    ).combine_first(
        _safe_float(frame.get("price_10m"))
    ).combine_first(_safe_float(frame.get("closing_price")))
    frame = assign_market_rank(frame.assign(closing_price=frame["latest_odds"]), odds_column="closing_price")
    return frame, merge_stats


def _score_runners(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()

    form_payloads = working.apply(_build_recent_form_proxy, axis=1)
    working["has_history"] = form_payloads.map(lambda item: item["has_history"])
    working["history_row_count"] = form_payloads.map(lambda item: item["history_row_count"])
    working["recent_form_qualifies"] = form_payloads.map(lambda item: item["qualifies"])
    working["qualification_reason"] = form_payloads.map(lambda item: item["qualification_reason"])
    working["form_score"] = form_payloads.map(lambda item: item["form_score"])
    working["last_start_finish_proxy"] = form_payloads.map(lambda item: item["last_start_finish"])
    working["avg_last3_finish_proxy"] = form_payloads.map(lambda item: item["avg_last3_finish"])
    working["avg_last3_margin_proxy"] = form_payloads.map(lambda item: item["avg_last3_margin"])

    working["_score_for_prob"] = working["form_score"].clip(lower=0.0001)
    score_totals = working.groupby(
        ["race_date", "track_norm", "race_number"], dropna=False
    )["_score_for_prob"].transform("sum")
    working["model_probability"] = (
        working["_score_for_prob"] / score_totals.replace({0.0: pd.NA})
    ).fillna(0.0)
    working.drop(columns=["_score_for_prob"], inplace=True)

    records: list[dict[str, object]] = []
    for _, row in working.iterrows():
        market_snapshot = _build_market_snapshot(row)
        latest_odds = market_snapshot["latest_odds"]
        market_probability = commission_adjusted_market_probability(
            float(latest_odds) if pd.notna(latest_odds) else None,
            COMMISSION_RATE,
        )
        edge = (
            float(row["model_probability"]) - market_probability
            if market_probability is not None
            else None
        )
        movement_metrics = _movement_metrics(
            market_snapshot,
            int(row["market_rank"]) if pd.notna(row["market_rank"]) else None,
        )
        movement_score = movement_metrics["movement_score"]
        combined_score = _combined_score(movement_score, edge, float(row["form_score"] or 0.0))
        records.append(
            {
                **row.to_dict(),
                **market_snapshot,
                "market_probability_adj": market_probability,
                "edge": edge,
                "movement_score": movement_score,
                "recent_drift": movement_metrics["recent_drift"],
                "combined_score": combined_score,
                "movement_history_ok": movement_metrics["has_movement"],
            }
        )

    return pd.DataFrame(records)


def _apply_filters(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    counters = {
        "rows_total": int(len(frame)),
        "missing_latest_odds": 0,
        "missing_form_history": 0,
        "poor_recent_form": 0,
        "form_score_too_low": 0,
        "odds_band": 0,
        "market_rank_too_low": 0,
        "missing_late_movement": 0,
        "recent_drift": 0,
        "movement_score_too_low": 0,
        "edge_too_negative": 0,
        "combined_score_too_low": 0,
    }

    working = frame.copy()
    working["rejection_reason"] = None
    rule_masks = {
        "missing_latest_odds": working["latest_odds"].isna(),
        "missing_form_history": ~working["has_history"],
        "poor_recent_form": ~working["recent_form_qualifies"],
        "form_score_too_low": working["form_score"] < MIN_FORM_SCORE,
        "odds_band": (working["latest_odds"] < MIN_RUNNER_ODDS) | (working["latest_odds"] > MAX_RUNNER_ODDS),
        "market_rank_too_low": working["market_rank"].isna() | (working["market_rank"] > MAX_MARKET_RANK),
        "missing_late_movement": ~working["movement_history_ok"],
        "recent_drift": working["recent_drift"] == True,
        "movement_score_too_low": working["movement_score"] < MIN_MOVEMENT_SCORE,
        "edge_too_negative": working["edge"].isna() | (working["edge"] < MIN_EDGE),
        "combined_score_too_low": working["combined_score"] < MIN_COMBINED_SCORE,
    }

    def reject(mask: pd.Series, reason: str) -> None:
        fresh_mask = mask & working["rejection_reason"].isna()
        counters[reason] += int(fresh_mask.sum())
        working.loc[fresh_mask, "rejection_reason"] = reason

    for reason, mask in rule_masks.items():
        reject(mask, reason)

    for reason, mask in rule_masks.items():
        working[f"fails_{reason}"] = mask.astype(int)

    failure_columns = [f"fails_{reason}" for reason in rule_masks]
    working["failure_count"] = working[failure_columns].sum(axis=1)
    working["single_fail_reason"] = working.apply(
        lambda row: next(
            (
                reason
                for reason in rule_masks
                if row[f"fails_{reason}"] == 1
            ),
            None,
        )
        if row["failure_count"] == 1
        else None,
        axis=1,
    )
    working["edge_gap"] = (working["edge"] - MIN_EDGE).round(4)
    working["movement_score_gap"] = (working["movement_score"] - MIN_MOVEMENT_SCORE).round(4)
    working["combined_score_gap"] = (working["combined_score"] - MIN_COMBINED_SCORE).round(4)
    working["form_score_gap"] = (working["form_score"] - MIN_FORM_SCORE).round(4)
    working["market_rank_gap"] = (MAX_MARKET_RANK - working["market_rank"]).round(4)
    working["odds_to_min_gap"] = (working["latest_odds"] - MIN_RUNNER_ODDS).round(4)
    working["odds_to_max_gap"] = (MAX_RUNNER_ODDS - working["latest_odds"]).round(4)

    qualifiers = working[working["rejection_reason"].isna()].copy()
    return qualifiers, working, counters


def _build_near_miss_reports(scored_with_rejections: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rejected = scored_with_rejections[scored_with_rejections["rejection_reason"].notna()].copy()
    if rejected.empty:
        return pd.DataFrame(), pd.DataFrame()

    near_misses = rejected[rejected["failure_count"] <= 2].copy()
    near_misses["near_miss_tier"] = near_misses["failure_count"].map(
        lambda value: "single_fail" if value == 1 else "double_fail" if value == 2 else "other"
    )
    near_misses = near_misses.sort_values(
        [
            "failure_count",
            "combined_score",
            "movement_score",
            "form_score",
            "edge",
        ],
        ascending=[True, False, False, False, False],
    )

    near_miss_columns = [
        "race_date",
        "track",
        "race_number",
        "horse_name",
        "latest_odds",
        "market_rank",
        "movement_score",
        "form_score",
        "edge",
        "combined_score",
        "rejection_reason",
        "single_fail_reason",
        "failure_count",
        "near_miss_tier",
        "edge_gap",
        "movement_score_gap",
        "combined_score_gap",
        "form_score_gap",
        "market_rank_gap",
        "odds_to_min_gap",
        "odds_to_max_gap",
        "opening_price",
        "price_60m",
        "price_30m",
        "price_10m",
        "price_5m",
        "price_3m_aligned",
        "price_1m_aligned",
        "qualification_reason",
    ]
    near_miss_frame = near_misses[near_miss_columns].copy()

    reason_summary = (
        rejected.groupby("rejection_reason", dropna=False)
        .agg(
            rejected_runners=("horse_name", "size"),
            single_fail_runners=("single_fail_reason", lambda values: int(values.notna().sum())),
            avg_combined_score=("combined_score", "mean"),
            avg_movement_score=("movement_score", "mean"),
            avg_form_score=("form_score", "mean"),
            avg_edge=("edge", "mean"),
        )
        .reset_index()
        .sort_values(["single_fail_runners", "rejected_runners"], ascending=[False, False])
    )
    return near_miss_frame, reason_summary


def _simulate_bets(qualifiers: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float | int]]:
    race_best = (
        qualifiers.sort_values(
            ["race_date", "track_norm", "race_number", "combined_score", "movement_score"],
            ascending=[True, True, True, False, False],
        )
        .groupby(["race_date", "track_norm", "race_number"], as_index=False, dropna=False)
        .head(1)
        .copy()
    )

    daily_selected = (
        race_best.sort_values(
            ["race_date", "combined_score", "movement_score"],
            ascending=[True, False, False],
        )
        .groupby("race_date", as_index=False, dropna=False)
        .head(MAX_DAILY_BETS)
        .copy()
    )

    bank = STARTING_BANK
    bank_history = [bank]
    bet_rows: list[dict[str, object]] = []
    daily_rows: list[dict[str, object]] = []

    for race_date, day_frame in daily_selected.groupby("race_date", dropna=False):
        day_bets = day_frame.sort_values(
            ["combined_score", "movement_score", "track_norm", "race_number"],
            ascending=[False, False, True, True],
        )
        day_profit = 0.0
        day_staked = 0.0
        day_bets_count = 0
        for _, row in day_bets.iterrows():
            stake = round(bank * STAKE_PCT, 2)
            odds_taken = float(row["latest_odds"])
            won_flag = int(row["won_flag"]) if pd.notna(row["won_flag"]) else 0
            profit_loss = round(
                ((odds_taken - 1.0) * (1.0 - COMMISSION_RATE) * stake) if won_flag == 1 else -stake,
                2,
            )
            bank = round(bank + profit_loss, 2)
            bank_history.append(bank)
            day_profit += profit_loss
            day_staked += stake
            day_bets_count += 1
            bet_rows.append(
                {
                    **row.to_dict(),
                    "decision_version": DECISION_VERSION,
                    "stake": stake,
                    "profit_loss": profit_loss,
                    "bank_after_bet": bank,
                    "odds_taken": odds_taken,
                }
            )

        daily_rows.append(
            {
                "race_date": race_date,
                "bets": day_bets_count,
                "total_staked": round(day_staked, 2),
                "profit_loss": round(day_profit, 2),
                "roi": round(day_profit / day_staked, 4) if day_staked else 0.0,
                "bank_after_day": bank,
            }
        )

    bets = pd.DataFrame(bet_rows)
    daily = pd.DataFrame(daily_rows)
    total_bets = len(bets)
    total_staked = float(bets["stake"].sum()) if total_bets else 0.0
    profit_loss = round(float(bets["profit_loss"].sum()) if total_bets else 0.0, 2)
    summary = {
        "decision_version": DECISION_VERSION,
        "approximation": "historical_proxy_uses_closing_as_latest_and_no_true_3m_checkpoint",
        "total_bets": total_bets,
        "race_level_qualifiers": int(len(race_best)),
        "wins": int(bets["won_flag"].sum()) if total_bets else 0,
        "losses": int(total_bets - int(bets["won_flag"].sum())) if total_bets else 0,
        "strike_rate": round(float(bets["won_flag"].mean()) if total_bets else 0.0, 4),
        "total_staked": round(total_staked, 2),
        "profit_loss": profit_loss,
        "roi": round(profit_loss / total_staked, 4) if total_staked else 0.0,
        "max_drawdown": compute_max_drawdown(bank_history),
        "final_bank": round(bank, 2),
        "average_odds": round(float(bets["odds_taken"].mean()) if total_bets else 0.0, 4),
        "average_edge": round(float(bets["edge"].mean()) if total_bets else 0.0, 4),
        "average_movement_score": round(float(bets["movement_score"].mean()) if total_bets else 0.0, 4),
        "average_form_score": round(float(bets["form_score"].mean()) if total_bets else 0.0, 4),
        "average_combined_score": round(float(bets["combined_score"].mean()) if total_bets else 0.0, 4),
        "bet_days": int(daily["bets"].gt(0).sum()) if not daily.empty else 0,
    }
    return bets, daily, summary


def run_late_market_v2_backtest(
    matched_path: Path = MATCHED_PATH,
    odds_time_series_path: Path = ODDS_TIME_SERIES_PATH,
    summary_path: Path = SUMMARY_PATH,
    bets_path: Path = BETS_PATH,
    daily_path: Path = DAILY_PATH,
    notes_path: Path = NOTES_PATH,
    near_miss_path: Path = NEAR_MISS_PATH,
    near_miss_summary_path: Path = NEAR_MISS_SUMMARY_PATH,
) -> dict[str, pd.DataFrame]:
    frame, merge_stats = _load_frame(matched_path, odds_time_series_path)
    scored = _score_runners(frame)
    qualifiers, scored_with_rejections, counters = _apply_filters(scored)
    bets, daily, summary = _simulate_bets(qualifiers)
    near_miss_frame, near_miss_summary = _build_near_miss_reports(scored_with_rejections)

    summary_frame = pd.DataFrame([{**summary, **counters, **merge_stats}])
    save_dataframe(summary_frame, summary_path)
    save_dataframe(bets, bets_path)
    save_dataframe(daily, daily_path)
    save_dataframe(near_miss_frame, near_miss_path)
    save_dataframe(near_miss_summary, near_miss_summary_path)
    notes_path.parent.mkdir(parents=True, exist_ok=True)
    notes_path.write_text(
        json.dumps(
            {
                "decision_version": DECISION_VERSION,
                "limitations": [
                "Historical proxy uses closing_price as the latest pre-jump price.",
                    "Where available, aligned odds_time_series checkpoints override matched CSV checkpoints and prefer 1m/3m/5m/10m as latest pre-jump odds.",
                    "Matched research data still does not contain a native live event stream, so this remains a proxy rather than a perfect scheduler replay.",
                    "Daily cap is applied to the top 3 qualified race winners by combined_score per day because exact chronological race times are not available in matched_runner_data.csv.",
                    "Model probability is proxied from race-normalized live-style form_score because live Prediction rows are not present in the matched research dataset.",
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print("Late Market V2 Backtest Summary")
    print(summary_frame.to_string(index=False))
    if not near_miss_summary.empty:
        print("\nNear-Miss Rejection Summary")
        print(near_miss_summary.head(10).to_string(index=False))
    if not near_miss_frame.empty:
        print("\nTop Near-Miss Candidates")
        preview = near_miss_frame.head(10)[
            [
                "race_date",
                "track",
                "race_number",
                "horse_name",
                "rejection_reason",
                "single_fail_reason",
                "failure_count",
                "latest_odds",
                "movement_score",
                "form_score",
                "edge",
                "combined_score",
            ]
        ]
        print(preview.to_string(index=False))
    if not bets.empty:
        print("\nTop 10 Backtest Bets")
        preview = bets[
            [
                "race_date",
                "track",
                "race_number",
                "horse_name",
                "odds_taken",
                "movement_score",
                "form_score",
                "edge",
                "combined_score",
                "profit_loss",
            ]
        ].head(10)
        print(preview.to_string(index=False))
    return {
        "summary": summary_frame,
        "bets": bets,
        "daily": daily,
        "near_misses": near_miss_frame,
        "near_miss_summary": near_miss_summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest the live-style model_edge_late_v2 rules on matched research data.")
    parser.add_argument(
        "--matched-path",
        type=Path,
        default=MATCHED_PATH,
        help="Path to matched_runner_data.csv.",
    )
    parser.add_argument(
        "--odds-time-series-path",
        type=Path,
        default=ODDS_TIME_SERIES_PATH,
        help="Path to aligned odds_time_series.csv.",
    )
    args = parser.parse_args()
    run_late_market_v2_backtest(args.matched_path, args.odds_time_series_path)


if __name__ == "__main__":
    main()
