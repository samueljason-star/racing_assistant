from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.betting.market_helpers import closing_line_metrics
from app.research.market_residual_model import (
    BEST_MODEL_PATH as RESIDUAL_BEST_MODEL_PATH,
    EXECUTION_PATH as RESIDUAL_EXECUTION_PATH,
    MATCHED_PATH,
    ModelSpec,
    _add_interaction_columns,
    _build_leave_one_month_out_folds,
    _build_walk_forward_folds,
    _candidate_feature_columns,
    _execution_rules,
    _predict_frame,
    _prepare_bets,
    _prepare_research_frame,
    _split_train_test,
)
from app.research.utils import RESEARCH_REPORTS_DIR, ensure_research_dirs, save_dataframe

MONTH_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_profit_by_month.csv"
TRACK_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_profit_by_track.csv"
ODDS_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_profit_by_odds_band.csv"
MARKET_RANK_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_profit_by_market_rank.csv"
TOP_WINNERS_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_top_winners.csv"
BET_LIST_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_bet_list.csv"
CLV_PROFILE_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_clv_profile.csv"
STRESS_TESTS_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_stress_tests.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "suspicious_roi_forensics_summary.md"

TARGET_MODEL = "calibrated_logistic"
TARGET_RULE = "positive_residual_in_small_fields_only"


def _odds_band(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        values,
        bins=[0, 2, 4, 6, 10, 20, np.inf],
        labels=["1-2", "2-4", "4-6", "6-10", "10-20", "20+"],
        include_lowest=True,
    ).astype(str)


def _market_rank_band(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        values,
        bins=[0, 1, 2, 3, 5, np.inf],
        labels=["rank_1", "rank_2", "rank_3", "rank_4_5", "rank_6_plus"],
        include_lowest=True,
    ).astype(str)


def _roi(profit_loss: pd.Series, stake: pd.Series) -> float:
    total_stake = float(stake.sum())
    if total_stake <= 0:
        return 0.0
    return float(profit_loss.sum() / total_stake)


def _attach_clv_columns(selection: pd.DataFrame) -> pd.DataFrame:
    working = selection.copy()
    open_metrics = [
        closing_line_metrics(open_price if pd.notna(open_price) else current_price, current_price)
        for open_price, current_price in zip(working["opening_price"], working["current_price"])
    ]
    sixty_metrics = [
        closing_line_metrics(price_60 if pd.notna(price_60) else current_price, current_price)
        for price_60, current_price in zip(working["price_60m"], working["current_price"])
    ]
    working["clv_open_percent"] = [metric["clv_percent"] for metric in open_metrics]
    working["clv_60_percent"] = [metric["clv_percent"] for metric in sixty_metrics]
    working["shortened_from_open"] = [metric["beat_closing_line"] for metric in open_metrics]
    working["shortened_from_60"] = [metric["beat_closing_line"] for metric in sixty_metrics]
    return working


def _profit_by_group(selection: pd.DataFrame, group_column: str) -> pd.DataFrame:
    bets = _prepare_bets(selection)
    grouped = bets.groupby(group_column, dropna=False).agg(
        bets=("stake", "size"),
        wins=("won_flag", "sum"),
        profit=("profit_loss", "sum"),
        average_odds=("current_price", "mean"),
        average_market_rank=("market_rank_current", "mean"),
        average_clv_open=("clv_open_percent", "mean"),
        average_clv_60=("clv_60_percent", "mean"),
    ).reset_index()
    grouped["strike_rate"] = grouped["wins"] / grouped["bets"].replace({0: np.nan})
    grouped["roi"] = grouped["profit"] / (grouped["bets"] * 100.0).replace({0: np.nan})
    return grouped.fillna(0.0)


def _top_winner_concentration(bets: pd.DataFrame) -> pd.DataFrame:
    winners = bets[bets["profit_loss"] > 0].sort_values("profit_loss", ascending=False).copy()
    winners["profit_share"] = winners["profit_loss"] / winners["profit_loss"].sum() if not winners.empty else 0.0
    return winners


def _remove_top_winners_roi(bets: pd.DataFrame, winners_to_remove: int) -> float:
    winners = bets[bets["profit_loss"] > 0].sort_values("profit_loss", ascending=False)
    trimmed = bets.drop(index=winners.head(winners_to_remove).index)
    return _roi(trimmed["profit_loss"], trimmed["stake"])


def _stress_test_rows(selection: pd.DataFrame, frame: pd.DataFrame, feature_columns: list[str], test_size: float, walk_forward_limit: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    bets = _prepare_bets(selection)
    rows.append({"stress_test": "base", "bets": int(len(bets)), "roi": _roi(bets["profit_loss"], bets["stake"])})
    for winners_to_remove in [1, 2, 5]:
        rows.append(
            {
                "stress_test": f"remove_top_{winners_to_remove}_winners",
                "bets": int(len(bets)),
                "roi": _remove_top_winners_roi(bets, winners_to_remove),
            }
        )

    first_half_cut = int(len(selection) * 0.5)
    first_half = _prepare_bets(selection.sort_values("race_day").head(first_half_cut))
    second_half = _prepare_bets(selection.sort_values("race_day").tail(len(selection) - first_half_cut))
    rows.append({"stress_test": "first_half", "bets": int(len(first_half)), "roi": _roi(first_half["profit_loss"], first_half["stake"])})
    rows.append({"stress_test": "second_half", "bets": int(len(second_half)), "roi": _roi(second_half["profit_loss"], second_half["stake"])})

    walk_forward_folds = _build_walk_forward_folds(frame, walk_forward_limit)
    for fold_name, fold_train, fold_test in walk_forward_folds:
        fold_predictions = _predict_frame(ModelSpec(TARGET_MODEL, "logistic", True), fold_train, fold_test, feature_columns)
        fold_selection = _attach_clv_columns(_execution_rules(fold_predictions).get(TARGET_RULE, pd.DataFrame()).copy())
        fold_bets = _prepare_bets(fold_selection)
        rows.append({"stress_test": fold_name, "bets": int(len(fold_bets)), "roi": _roi(fold_bets["profit_loss"], fold_bets["stake"])})

    lomo_folds = _build_leave_one_month_out_folds(frame, walk_forward_limit)
    for fold_name, fold_train, fold_test in lomo_folds:
        fold_predictions = _predict_frame(ModelSpec(TARGET_MODEL, "logistic", True), fold_train, fold_test, feature_columns)
        fold_selection = _attach_clv_columns(_execution_rules(fold_predictions).get(TARGET_RULE, pd.DataFrame()).copy())
        fold_bets = _prepare_bets(fold_selection)
        rows.append({"stress_test": fold_name, "bets": int(len(fold_bets)), "roi": _roi(fold_bets["profit_loss"], fold_bets["stake"])})

    rng = np.random.default_rng(42)
    unique_races = selection[["race_date", "track_norm", "race_number"]].drop_duplicates().reset_index(drop=True)
    for iteration in range(1, 11):
        if unique_races.empty:
            break
        keep_mask = rng.random(len(unique_races)) > 0.1
        kept_races = unique_races[keep_mask].copy()
        reduced = selection.merge(kept_races, on=["race_date", "track_norm", "race_number"], how="inner")
        reduced_bets = _prepare_bets(reduced)
        rows.append({"stress_test": f"random_drop_10pct_{iteration:02d}", "bets": int(len(reduced_bets)), "roi": _roi(reduced_bets["profit_loss"], reduced_bets["stake"])})
    return pd.DataFrame(rows)


def _leakage_notes(feature_columns: list[str], frame: pd.DataFrame) -> list[str]:
    notes: list[str] = []
    late_columns = [column for column in feature_columns if column in {"price_60m", "price_30m", "price_10m", "price_5m", "opening_price"}]
    if late_columns:
        notes.append(
            "Model uses pre-race market columns: " + ", ".join(late_columns) + ". These are late-market but still pre-race, not post-race leakage."
        )
    forbidden = [column for column in feature_columns if column in {"closing_price", "finish_position", "won_flag", "open_to_close_change", "60_to_close_change"}]
    if forbidden:
        notes.append("Potential leakage risk detected in feature list: " + ", ".join(forbidden))
    else:
        notes.append("No closing-price target columns or post-race outcome columns were used as model inputs.")
    if "current_price" in frame.columns:
        notes.append("Residual model current_price is a near-jump proxy in this dataset and resolves to price_5m here, not closing_price.")
    return notes


def run_suspicious_roi_forensics(
    matched_path: Path,
    *,
    test_size: float,
    walk_forward_limit: int,
    save_artifacts: bool,
) -> dict[str, pd.DataFrame | str]:
    ensure_research_dirs()
    print("Rebuilding residual holdout selection for forensic review...")
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
    holdout_predictions = _predict_frame(ModelSpec(TARGET_MODEL, "logistic", True), train_frame, test_frame, feature_columns)
    selection = _execution_rules(holdout_predictions).get(TARGET_RULE, pd.DataFrame()).copy()
    selection = _attach_clv_columns(selection)
    bets = _prepare_bets(selection)

    if selection.empty:
        raise RuntimeError("Suspicious ROI rule produced no holdout selections.")

    selection["odds_band"] = _odds_band(selection["current_price"])
    selection["market_rank_band"] = _market_rank_band(selection["market_rank_current"])

    month_report = _profit_by_group(selection, "race_month")
    track_report = _profit_by_group(selection, "track_norm")
    odds_report = _profit_by_group(selection, "odds_band")
    market_rank_report = _profit_by_group(selection, "market_rank_band")
    top_winners = _top_winner_concentration(bets).head(20).copy()
    clv_profile = pd.DataFrame(
        [
            {
                "clv_profile": "overall",
                "bets": int(len(selection)),
                "shorten_rate_open": float(pd.Series(selection["shortened_from_open"]).fillna(False).mean()),
                "shorten_rate_60": float(pd.Series(selection["shortened_from_60"]).fillna(False).mean()),
                "average_clv_open_percent": float(pd.Series(selection["clv_open_percent"]).dropna().mean()) if selection["clv_open_percent"].notna().any() else 0.0,
                "average_clv_60_percent": float(pd.Series(selection["clv_60_percent"]).dropna().mean()) if selection["clv_60_percent"].notna().any() else 0.0,
                "roi_when_clv_open_positive": _roi(
                    bets.loc[pd.Series(selection["clv_open_percent"]).fillna(-999) > 0, "profit_loss"],
                    bets.loc[pd.Series(selection["clv_open_percent"]).fillna(-999) > 0, "stake"],
                ),
                "roi_when_clv_open_negative": _roi(
                    bets.loc[pd.Series(selection["clv_open_percent"]).fillna(999) <= 0, "profit_loss"],
                    bets.loc[pd.Series(selection["clv_open_percent"]).fillna(999) <= 0, "stake"],
                ),
            }
        ]
    )
    stress_tests = _stress_test_rows(selection, frame, feature_columns, test_size, walk_forward_limit)

    bet_list = selection[
        [
            "race_date",
            "track_norm",
            "race_number",
            "horse_name",
            "current_price",
            "market_rank_current",
            "residual_score",
            "predicted_win_probability",
            "market_prob_norm",
            "won_flag",
            "opening_price",
            "price_60m",
            "clv_open_percent",
            "clv_60_percent",
        ]
    ].copy()
    bet_list["profit_loss"] = bets["profit_loss"].values

    matching_execution = pd.DataFrame()
    if RESIDUAL_EXECUTION_PATH.exists():
        execution_report = pd.read_csv(RESIDUAL_EXECUTION_PATH)
        matching_execution = execution_report[
            execution_report["model_name"].eq(TARGET_MODEL) & execution_report["execution_rule"].eq(TARGET_RULE)
        ].copy()

    reproduced_roi = _roi(bets["profit_loss"], bets["stake"])
    top_1_share = float(top_winners.head(1)["profit_loss"].sum() / bets["profit_loss"].sum()) if bets["profit_loss"].sum() > 0 else 0.0
    top_2_share = float(top_winners.head(2)["profit_loss"].sum() / bets["profit_loss"].sum()) if bets["profit_loss"].sum() > 0 else 0.0
    top_5_share = float(top_winners.head(5)["profit_loss"].sum() / bets["profit_loss"].sum()) if bets["profit_loss"].sum() > 0 else 0.0
    best_month_share = float(month_report["profit"].max() / month_report["profit"].sum()) if month_report["profit"].sum() > 0 else 0.0
    leakage_notes = _leakage_notes(feature_columns, frame)

    summary_lines = [
        "# Suspicious ROI Forensics Summary",
        "",
        f"- Investigated rule: `{TARGET_MODEL}` / `{TARGET_RULE}`",
        f"- Reproduced holdout selections: `{len(selection)}` bets",
        f"- Reproduced ROI: `{reproduced_roi:.4f}`",
    ]
    if not matching_execution.empty:
        row = matching_execution.iloc[0]
        summary_lines.extend(
            [
                f"- Existing report row: bets=`{int(row['bets'])}` roi=`{float(row['roi']):.4f}` robustness=`{float(row['robustness_score']):.4f}`",
                "- Note: if this differs from an older quoted +53% / 534-bet result, the local current report is the source of truth for this workspace.",
            ]
        )
    summary_lines.extend(
        [
            "",
            "## Verdict",
            f"- Likely real? {'No' if reproduced_roi <= 0 or top_2_share > 0.6 else 'Unclear'}",
            f"- Came from one or two lucky winners? {'Yes' if top_2_share > 0.4 else 'Not obviously'}",
            f"- Remove-best-winner ROI: `{_remove_top_winners_roi(bets, 1):.4f}`",
            f"- Remove-top2-winners ROI: `{_remove_top_winners_roi(bets, 2):.4f}`",
            f"- Remove-top5-winners ROI: `{_remove_top_winners_roi(bets, 5):.4f}`",
            f"- Positive CLV from open? `{float(clv_profile.iloc[0]['average_clv_open_percent']):.2f}%` average",
            f"- Positive CLV from 60? `{float(clv_profile.iloc[0]['average_clv_60_percent']):.2f}%` average",
            f"- Month concentration risk: `{best_month_share:.4f}` of total profit in best month",
            f"- Top winner profit share: `{top_1_share:.4f}`",
            f"- Top 2 winners profit share: `{top_2_share:.4f}`",
            f"- Top 5 winners profit share: `{top_5_share:.4f}`",
            "",
            "## Leakage Review",
        ]
    )
    summary_lines.extend([f"- {note}" for note in leakage_notes])
    summary_lines.extend(
        [
            "",
            "## Recommendation",
            "- If the reproduced rule depends on a tiny number of winners, negative CLV, or collapses after removing top winners, discard it as a false lead.",
            "- If any value remains, it is a forensic hint about where the model overreaches, not a live candidate.",
        ]
    )
    summary_text = "\n".join(summary_lines) + "\n"

    if save_artifacts:
        save_dataframe(month_report, MONTH_PATH)
        save_dataframe(track_report, TRACK_PATH)
        save_dataframe(odds_report, ODDS_PATH)
        save_dataframe(market_rank_report, MARKET_RANK_PATH)
        save_dataframe(top_winners, TOP_WINNERS_PATH)
        save_dataframe(bet_list, BET_LIST_PATH)
        save_dataframe(clv_profile, CLV_PROFILE_PATH)
        save_dataframe(stress_tests, STRESS_TESTS_PATH)
        SUMMARY_PATH.write_text(summary_text, encoding="utf-8")

    print()
    print("Suspicious ROI Forensics Summary")
    print(f"Reproduced bets={len(selection)} roi={reproduced_roi:.4f} remove_top2={_remove_top_winners_roi(bets, 2):.4f}")
    return {
        "profit_by_month": month_report,
        "profit_by_track": track_report,
        "profit_by_odds_band": odds_report,
        "profit_by_market_rank": market_rank_report,
        "top_winners": top_winners,
        "bet_list": bet_list,
        "clv_profile": clv_profile,
        "stress_tests": stress_tests,
        "summary": summary_text,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Forensic investigation of suspicious residual-model ROI.")
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    parser.add_argument("--min-bets", type=int, default=50)  # accepted for CLI symmetry
    parser.add_argument("--test-size", type=float, default=0.3)
    parser.add_argument("--walk-forward", type=int, default=6)
    parser.add_argument("--save-artifacts", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_suspicious_roi_forensics(
        args.matched_path,
        test_size=args.test_size,
        walk_forward_limit=args.walk_forward,
        save_artifacts=args.save_artifacts,
    )


if __name__ == "__main__":
    main()
