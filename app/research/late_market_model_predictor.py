from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from app.betting.market_helpers import commission_adjusted_market_probability
from app.config import BETFAIR_COMMISSION_RATE
from app.research.form_score_optimizer import prepare_form_features
from app.research.late_market_v2_backtest import MATCHED_PATH, ODDS_TIME_SERIES_PATH, _load_frame, _score_runners
from app.research.utils import RESEARCH_REPORTS_DIR, compute_max_drawdown, save_dataframe

PREDICTION_RESULTS_PATH = RESEARCH_REPORTS_DIR / "late_model_prediction_results.csv"
CALIBRATION_REPORT_PATH = RESEARCH_REPORTS_DIR / "late_model_calibration_report.csv"
THRESHOLD_SWEEP_PATH = RESEARCH_REPORTS_DIR / "late_model_threshold_sweep.csv"
TOPN_BACKTEST_PATH = RESEARCH_REPORTS_DIR / "late_model_topn_backtest.csv"
ODDS_RANK_BREAKDOWN_PATH = RESEARCH_REPORTS_DIR / "late_model_odds_rank_breakdown.csv"

FLAT_STAKE = 100.0
STARTING_BANK = 10000.0
COMMISSION_MULTIPLIER = 1.0 - BETFAIR_COMMISSION_RATE
MIN_BET_ODDS = 3.0
MAX_BET_ODDS = 20.0
MEANINGFUL_BET_COUNT = 30
MAX_DAILY_BETS = 3

FEATURE_COLUMNS = [
    "form_score",
    "last_start_finish",
    "best_last_3_finish",
    "market_rank",
    "odds",
    "open_to_current",
    "60_to_current",
    "30_to_current",
    "10_to_current",
    "5_to_current",
    "3_to_current",
]

CALIBRATION_BUCKETS = [0.0, 0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.35, 0.50, 1.0]
EDGE_THRESHOLDS = [0.02, 0.05, 0.08]
PROBABILITY_CAPS: list[float | None] = [0.35, None]
MOVEMENT_THRESHOLDS: list[float | None] = [None, 0.45, 0.75]
FORM_THRESHOLDS: list[float | None] = [None, 0.30, 0.60]
ODDS_BUCKETS = {
    "3-5": (3.0, 5.0),
    "5-8": (5.0, 8.0),
    "8-12": (8.0, 12.0),
    "12-20": (12.0, 20.0),
}
RANK_BUCKETS = {
    "1-3": {1, 2, 3},
    "4-6": {4, 5, 6},
    "7-8": {7, 8},
}


def _safe_float_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame.get(column), errors="coerce")


def _safe_mean(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    value = pd.to_numeric(frame[column], errors="coerce").mean()
    return round(float(value), 4) if pd.notna(value) else 0.0


def _safe_log_loss(y_true: pd.Series, probabilities: pd.Series) -> float:
    if len(y_true) == 0:
        return 0.0
    clipped = probabilities.clip(lower=1e-6, upper=1 - 1e-6)
    return round(float(log_loss(y_true, clipped, labels=[0, 1])), 6)


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


def _build_model_frame() -> pd.DataFrame:
    frame, _ = _load_frame(MATCHED_PATH, ODDS_TIME_SERIES_PATH)
    frame = prepare_form_features(frame)
    scored = _score_runners(frame).copy()

    scored["race_date"] = pd.to_datetime(scored["race_date"], errors="coerce")
    scored["odds"] = pd.to_numeric(scored["latest_odds"], errors="coerce")
    scored["market_rank"] = pd.to_numeric(scored["market_rank"], errors="coerce")
    scored["last_start_finish"] = pd.to_numeric(
        scored.get("last_start_finish").combine_first(scored.get("last_start_finish_proxy")),
        errors="coerce",
    )
    scored["best_last_3_finish"] = pd.to_numeric(scored.get("best_last_3_finish"), errors="coerce")
    scored["won_flag"] = pd.to_numeric(scored["won_flag"], errors="coerce").fillna(0).astype(int)

    for column in FEATURE_COLUMNS:
        scored[column] = pd.to_numeric(scored.get(column), errors="coerce")
        if scored[column].notna().sum() == 0:
            scored[column] = 0.0

    scored = scored[
        scored["race_date"].notna()
        & scored["odds"].notna()
        & (scored["odds"] > 0)
        & scored["market_rank"].notna()
    ].copy()
    scored["race_key"] = (
        scored["race_date"].dt.strftime("%Y-%m-%d")
        + "|"
        + scored["track"].astype(str)
        + "|"
        + scored["race_number"].astype(str)
    )
    scored["combined_research_score"] = (
        (pd.to_numeric(scored["movement_score"], errors="coerce").fillna(0.0) * 0.40)
        + (pd.to_numeric(scored["form_score"], errors="coerce").fillna(0.0) * 0.30)
        + (1.0 / pd.to_numeric(scored["odds"], errors="coerce").replace({0.0: pd.NA}).fillna(999.0) * 0.30)
    ).round(4)
    return scored


def _time_split(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    ordered_dates = sorted(frame["race_date"].dt.date.unique().tolist())
    split_index = max(1, int(len(ordered_dates) * 0.70))
    split_index = min(split_index, len(ordered_dates) - 1)
    train_dates = ordered_dates[:split_index]
    test_dates = ordered_dates[split_index:]

    train = frame[frame["race_date"].dt.date.isin(train_dates)].copy()
    test = frame[frame["race_date"].dt.date.isin(test_dates)].copy()
    return train, test, [str(item) for item in train_dates], [str(item) for item in test_dates]


def _build_base_models() -> dict[str, Pipeline]:
    return {
        "logistic_regression": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        max_iter=3000,
                        class_weight="balanced",
                        random_state=42,
                    ),
                ),
            ]
        ),
        "random_forest": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestClassifier(
                        n_estimators=400,
                        max_depth=8,
                        min_samples_leaf=5,
                        class_weight="balanced_subsample",
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
    }


def _fit_prediction_frames(
    train: pd.DataFrame,
    test: pd.DataFrame,
    train_dates: list[str],
    test_dates: list[str],
) -> pd.DataFrame:
    X_train = train[FEATURE_COLUMNS]
    y_train = train["won_flag"]
    X_test = test[FEATURE_COLUMNS]

    base_models = _build_base_models()
    calibration_folds = _choose_calibration_folds(y_train)
    prediction_frames: list[pd.DataFrame] = []

    for base_name, base_model in base_models.items():
        model_variants: list[tuple[str, object]] = [(f"{base_name}__uncalibrated", base_model)]
        if calibration_folds:
            model_variants.append(
                (
                    f"{base_name}__sigmoid",
                    CalibratedClassifierCV(estimator=base_model, method="sigmoid", cv=calibration_folds),
                )
            )
            if _can_use_isotonic(y_train):
                model_variants.append(
                    (
                        f"{base_name}__isotonic",
                        CalibratedClassifierCV(estimator=base_model, method="isotonic", cv=calibration_folds),
                    )
                )

        for model_name, model in model_variants:
            model.fit(X_train, y_train)
            probabilities = model.predict_proba(X_test)[:, 1]

            working = test.copy()
            working["model_name"] = model_name
            working["base_model_name"] = base_name
            working["model_probability"] = probabilities
            working["market_probability"] = working["odds"].apply(
                lambda value: commission_adjusted_market_probability(float(value), BETFAIR_COMMISSION_RATE)
            )
            working["edge"] = working["model_probability"] - working["market_probability"].fillna(0.0)
            working["train_start"] = train_dates[0]
            working["train_end"] = train_dates[-1]
            working["test_start"] = test_dates[0]
            working["test_end"] = test_dates[-1]
            prediction_frames.append(working)

    return pd.concat(prediction_frames, ignore_index=True)


def _bucket_probability(probability: float) -> str:
    if pd.isna(probability):
        return "unknown"
    for lower, upper in zip(CALIBRATION_BUCKETS[:-1], CALIBRATION_BUCKETS[1:]):
        if lower <= probability < upper or (upper == 1.0 and probability <= upper):
            return f"{lower:.2f}-{upper:.2f}"
    return "unknown"


def _build_calibration_report(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    working = predictions.copy()
    working["probability_bucket"] = working["model_probability"].map(_bucket_probability)

    for model_name, model_frame in working.groupby("model_name", dropna=False):
        rows.append(
            {
                "report_section": "overall",
                "model_name": model_name,
                "probability_bucket": "all",
                "count": len(model_frame),
                "average_predicted_probability": _safe_mean(model_frame, "model_probability"),
                "actual_win_rate": _safe_mean(model_frame, "won_flag"),
                "brier_score": round(float(brier_score_loss(model_frame["won_flag"], model_frame["model_probability"])), 6),
                "log_loss": _safe_log_loss(model_frame["won_flag"], model_frame["model_probability"]),
                "overconfidence_gap": round(
                    _safe_mean(model_frame, "model_probability") - _safe_mean(model_frame, "won_flag"),
                    4,
                ),
            }
        )

        for bucket, bucket_frame in model_frame.groupby("probability_bucket", dropna=False):
            if bucket_frame.empty:
                continue
            rows.append(
                {
                    "report_section": "bucket",
                    "model_name": model_name,
                    "probability_bucket": bucket,
                    "count": len(bucket_frame),
                    "average_predicted_probability": _safe_mean(bucket_frame, "model_probability"),
                    "actual_win_rate": _safe_mean(bucket_frame, "won_flag"),
                    "brier_score": round(float(brier_score_loss(bucket_frame["won_flag"], bucket_frame["model_probability"])), 6),
                    "log_loss": _safe_log_loss(bucket_frame["won_flag"], bucket_frame["model_probability"]),
                    "overconfidence_gap": round(
                        _safe_mean(bucket_frame, "model_probability") - _safe_mean(bucket_frame, "won_flag"),
                        4,
                    ),
                }
            )

    report = pd.DataFrame(rows).sort_values(["model_name", "report_section", "probability_bucket"])
    save_dataframe(report, CALIBRATION_REPORT_PATH)
    return report


def _apply_filters(
    frame: pd.DataFrame,
    *,
    edge_threshold: float,
    probability_cap: float | None,
    odds_range: tuple[float, float],
    rank_values: set[int],
    min_movement_score: float | None,
    min_form_score: float | None,
) -> pd.DataFrame:
    working = frame.copy()
    mask = (
        (working["edge"] >= edge_threshold)
        & working["odds"].between(odds_range[0], odds_range[1], inclusive="both")
        & pd.to_numeric(working["market_rank"], errors="coerce").isin(rank_values)
    )
    if probability_cap is not None:
        mask &= working["model_probability"] <= probability_cap
    if min_movement_score is not None:
        mask &= pd.to_numeric(working["movement_score"], errors="coerce") >= min_movement_score
    if min_form_score is not None:
        mask &= pd.to_numeric(working["form_score"], errors="coerce") >= min_form_score
    return working[mask].copy()


def _simulate_flat_bets(
    selected: pd.DataFrame,
    *,
    selection_mode: str,
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    working = selected.copy()
    if working.empty:
        return working, {
            "bet_count": 0,
            "wins": 0,
            "losses": 0,
            "strike_rate": 0.0,
            "flat_profit_loss": 0.0,
            "flat_roi": 0.0,
            "max_drawdown": 0.0,
            "average_odds": 0.0,
            "average_edge": 0.0,
            "average_model_probability": 0.0,
            "average_market_probability": 0.0,
            "average_movement_score": 0.0,
            "average_form_score": 0.0,
        }

    if selection_mode == "top1_edge_per_race":
        working = (
            working.sort_values(["race_date", "race_key", "edge", "model_probability"], ascending=[True, True, False, False])
            .groupby("race_key", as_index=False, sort=False)
            .head(1)
        )
    elif selection_mode == "top1_combined_per_race":
        working = (
            working.sort_values(
                ["race_date", "race_key", "combined_research_score", "edge", "model_probability"],
                ascending=[True, True, False, False, False],
            )
            .groupby("race_key", as_index=False, sort=False)
            .head(1)
        )

    sort_column = "edge" if selection_mode == "top1_edge_per_race" else "combined_research_score"
    working = (
        working.sort_values(["race_date", sort_column, "model_probability"], ascending=[True, False, False])
        .groupby(working["race_date"].dt.date, as_index=False, sort=False)
        .head(MAX_DAILY_BETS)
        .sort_values(["race_date", "track", "race_number", "horse_name"])
    )

    if working.empty:
        return _simulate_flat_bets(working, selection_mode="all_qualifiers")

    working["stake"] = FLAT_STAKE
    working["profit_loss"] = working.apply(
        lambda row: round(((float(row["odds"]) - 1.0) * COMMISSION_MULTIPLIER * FLAT_STAKE), 2)
        if int(row["won_flag"]) == 1
        else -FLAT_STAKE,
        axis=1,
    )

    bank = STARTING_BANK
    bank_history = [bank]
    for profit in working["profit_loss"].tolist():
        bank = round(bank + float(profit), 2)
        bank_history.append(bank)

    bet_count = len(working)
    wins = int(working["won_flag"].sum())
    losses = bet_count - wins
    flat_profit_loss = round(float(working["profit_loss"].sum()), 2)
    total_staked = bet_count * FLAT_STAKE
    summary = {
        "bet_count": bet_count,
        "wins": wins,
        "losses": losses,
        "strike_rate": round(wins / bet_count, 4) if bet_count else 0.0,
        "flat_profit_loss": flat_profit_loss,
        "flat_roi": round(flat_profit_loss / total_staked, 4) if total_staked else 0.0,
        "max_drawdown": compute_max_drawdown(bank_history),
        "average_odds": _safe_mean(working, "odds"),
        "average_edge": _safe_mean(working, "edge"),
        "average_model_probability": _safe_mean(working, "model_probability"),
        "average_market_probability": _safe_mean(working, "market_probability"),
        "average_movement_score": _safe_mean(working, "movement_score"),
        "average_form_score": _safe_mean(working, "form_score"),
    }
    return working, summary


def _build_threshold_sweep(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for model_name, model_frame in predictions.groupby("model_name", dropna=False):
        for edge_threshold in EDGE_THRESHOLDS:
            for probability_cap in PROBABILITY_CAPS:
                for odds_label, odds_range in ODDS_BUCKETS.items():
                    for rank_label, rank_values in RANK_BUCKETS.items():
                        for movement_threshold in MOVEMENT_THRESHOLDS:
                            for form_threshold in FORM_THRESHOLDS:
                                filtered = _apply_filters(
                                    model_frame,
                                    edge_threshold=edge_threshold,
                                    probability_cap=probability_cap,
                                    odds_range=odds_range,
                                    rank_values=rank_values,
                                    min_movement_score=movement_threshold,
                                    min_form_score=form_threshold,
                                )
                                _, summary = _simulate_flat_bets(filtered, selection_mode="all_qualifiers")
                                rows.append(
                                    {
                                        "model_name": model_name,
                                        "selection_mode": "all_qualifiers",
                                        "edge_threshold": edge_threshold,
                                        "probability_cap": "none" if probability_cap is None else probability_cap,
                                        "odds_bucket": odds_label,
                                        "rank_bucket": rank_label,
                                        "movement_threshold": "none" if movement_threshold is None else movement_threshold,
                                        "form_threshold": "none" if form_threshold is None else form_threshold,
                                        **summary,
                                    }
                                )

    report = pd.DataFrame(rows).sort_values(["flat_roi", "bet_count"], ascending=[False, False])
    save_dataframe(report, THRESHOLD_SWEEP_PATH)
    return report


def _build_topn_backtest(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    top_modes = ["top1_edge_per_race", "top1_combined_per_race"]
    for model_name, model_frame in predictions.groupby("model_name", dropna=False):
        for selection_mode in top_modes:
            for edge_threshold in EDGE_THRESHOLDS:
                for probability_cap in PROBABILITY_CAPS:
                    for movement_threshold in [0.45, 0.75]:
                        for form_threshold in [0.30, 0.60]:
                            filtered = _apply_filters(
                                model_frame,
                                edge_threshold=edge_threshold,
                                probability_cap=probability_cap,
                                odds_range=(MIN_BET_ODDS, MAX_BET_ODDS),
                                rank_values={1, 2, 3, 4, 5, 6, 7, 8},
                                min_movement_score=movement_threshold,
                                min_form_score=form_threshold,
                            )
                            _, summary = _simulate_flat_bets(filtered, selection_mode=selection_mode)
                            rows.append(
                                {
                                    "model_name": model_name,
                                    "selection_mode": selection_mode,
                                    "edge_threshold": edge_threshold,
                                    "probability_cap": "none" if probability_cap is None else probability_cap,
                                    "movement_threshold": movement_threshold,
                                    "form_threshold": form_threshold,
                                    "daily_cap": MAX_DAILY_BETS,
                                    **summary,
                                }
                            )

    report = pd.DataFrame(rows).sort_values(["flat_roi", "bet_count"], ascending=[False, False])
    save_dataframe(report, TOPN_BACKTEST_PATH)
    return report


def _build_odds_rank_breakdown(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    working = predictions.copy()

    def _odds_bucket(odds: float) -> str:
        if pd.isna(odds):
            return "unknown"
        for label, (lower, upper) in ODDS_BUCKETS.items():
            if lower <= float(odds) <= upper:
                return label
        return "outside"

    def _rank_bucket(rank: float) -> str:
        if pd.isna(rank):
            return "unknown"
        rank_int = int(rank)
        for label, values in RANK_BUCKETS.items():
            if rank_int in values:
                return label
        return "outside"

    working["odds_bucket"] = working["odds"].map(_odds_bucket)
    working["rank_bucket"] = working["market_rank"].map(_rank_bucket)
    conservative = _apply_filters(
        working,
        edge_threshold=0.05,
        probability_cap=0.35,
        odds_range=(MIN_BET_ODDS, MAX_BET_ODDS),
        rank_values={1, 2, 3, 4, 5, 6},
        min_movement_score=0.45,
        min_form_score=0.30,
    )
    conservative["_breakdown_filter"] = "conservative_candidates"
    working["_breakdown_filter"] = "all_test_rows"
    merged = pd.concat([working, conservative], ignore_index=True)

    for (model_name, filter_name, odds_bucket, rank_bucket), bucket_frame in merged.groupby(
        ["model_name", "_breakdown_filter", "odds_bucket", "rank_bucket"], dropna=False
    ):
        rows.append(
            {
                "model_name": model_name,
                "filter_name": filter_name,
                "odds_bucket": odds_bucket,
                "rank_bucket": rank_bucket,
                "count": len(bucket_frame),
                "actual_win_rate": _safe_mean(bucket_frame, "won_flag"),
                "average_model_probability": _safe_mean(bucket_frame, "model_probability"),
                "average_market_probability": _safe_mean(bucket_frame, "market_probability"),
                "average_edge": _safe_mean(bucket_frame, "edge"),
                "average_movement_score": _safe_mean(bucket_frame, "movement_score"),
                "average_form_score": _safe_mean(bucket_frame, "form_score"),
                "brier_score": round(float(brier_score_loss(bucket_frame["won_flag"], bucket_frame["model_probability"])), 6)
                if len(bucket_frame)
                else 0.0,
            }
        )

    report = pd.DataFrame(rows).sort_values(["model_name", "filter_name", "odds_bucket", "rank_bucket"])
    save_dataframe(report, ODDS_RANK_BREAKDOWN_PATH)
    return report


def _build_prediction_results(predictions: pd.DataFrame, calibration_report: pd.DataFrame, threshold_sweep: pd.DataFrame, topn_report: pd.DataFrame) -> pd.DataFrame:
    summary_rows: list[dict[str, object]] = []
    overall_calibration = calibration_report[calibration_report["report_section"] == "overall"].copy()

    for model_name, model_frame in predictions.groupby("model_name", dropna=False):
        calibration_row = overall_calibration[overall_calibration["model_name"] == model_name].iloc[0]
        best_threshold = threshold_sweep[threshold_sweep["model_name"] == model_name].head(1)
        best_topn = topn_report[topn_report["model_name"] == model_name].head(1)

        summary_rows.append(
            {
                "report_section": "summary",
                "model_name": model_name,
                "train_start": model_frame["train_start"].iloc[0],
                "train_end": model_frame["train_end"].iloc[0],
                "test_start": model_frame["test_start"].iloc[0],
                "test_end": model_frame["test_end"].iloc[0],
                "test_rows": len(model_frame),
                "test_positive_rate": _safe_mean(model_frame, "won_flag"),
                "average_test_model_probability": _safe_mean(model_frame, "model_probability"),
                "average_test_market_probability": _safe_mean(model_frame, "market_probability"),
                "brier_score": calibration_row["brier_score"],
                "log_loss": calibration_row["log_loss"],
                "overconfidence_gap": calibration_row["overconfidence_gap"],
                "best_threshold_flat_roi": float(best_threshold["flat_roi"].iloc[0]) if not best_threshold.empty else 0.0,
                "best_threshold_bet_count": int(best_threshold["bet_count"].iloc[0]) if not best_threshold.empty else 0,
                "best_topn_flat_roi": float(best_topn["flat_roi"].iloc[0]) if not best_topn.empty else 0.0,
                "best_topn_bet_count": int(best_topn["bet_count"].iloc[0]) if not best_topn.empty else 0,
                "best_topn_selection_mode": str(best_topn["selection_mode"].iloc[0]) if not best_topn.empty else None,
            }
        )

    report = pd.concat(
        [
            predictions.assign(report_section="prediction")[
                [
                    "report_section",
                    "model_name",
                    "race_date",
                    "track",
                    "race_number",
                    "horse_name",
                    "won_flag",
                    "odds",
                    "market_rank",
                    "form_score",
                    "movement_score",
                    "last_start_finish",
                    "best_last_3_finish",
                    "open_to_current",
                    "60_to_current",
                    "30_to_current",
                    "10_to_current",
                    "5_to_current",
                    "3_to_current",
                    "model_probability",
                    "market_probability",
                    "edge",
                    "combined_research_score",
                    "train_start",
                    "train_end",
                    "test_start",
                    "test_end",
                ]
            ],
            pd.DataFrame(summary_rows),
        ],
        ignore_index=True,
    )
    save_dataframe(report, PREDICTION_RESULTS_PATH)
    return report


def run_late_model_prediction() -> dict[str, pd.DataFrame]:
    frame = _build_model_frame()
    train, test, train_dates, test_dates = _time_split(frame)
    predictions = _fit_prediction_frames(train, test, train_dates, test_dates)
    calibration_report = _build_calibration_report(predictions)
    threshold_sweep = _build_threshold_sweep(predictions)
    topn_report = _build_topn_backtest(predictions)
    odds_rank_breakdown = _build_odds_rank_breakdown(predictions)
    prediction_results = _build_prediction_results(predictions, calibration_report, threshold_sweep, topn_report)

    overall_calibration = calibration_report[calibration_report["report_section"] == "overall"].copy()
    meaningful_thresholds = threshold_sweep[threshold_sweep["bet_count"] >= MEANINGFUL_BET_COUNT].copy()
    meaningful_topn = topn_report[topn_report["bet_count"] >= MEANINGFUL_BET_COUNT].copy()
    any_positive_threshold = bool((meaningful_thresholds["flat_roi"] > 0).any()) if not meaningful_thresholds.empty else False
    any_positive_topn = bool((meaningful_topn["flat_roi"] > 0).any()) if not meaningful_topn.empty else False
    any_meaningful = not meaningful_thresholds.empty or not meaningful_topn.empty
    still_overconfident = bool((overall_calibration["overconfidence_gap"] > 0.05).any())

    print("Late Market Model Prediction Results")
    print(f"Train dates: {train_dates[0]} -> {train_dates[-1]} ({len(train_dates)} days)")
    print(f"Test dates: {test_dates[0]} -> {test_dates[-1]} ({len(test_dates)} days)")
    print("\nCalibration Overview")
    print(
        overall_calibration[
            [
                "model_name",
                "count",
                "average_predicted_probability",
                "actual_win_rate",
                "brier_score",
                "log_loss",
                "overconfidence_gap",
            ]
        ].to_string(index=False)
    )
    print("\nBest Threshold Sweep Rows")
    print(
        threshold_sweep.head(10)[
            [
                "model_name",
                "selection_mode",
                "edge_threshold",
                "probability_cap",
                "odds_bucket",
                "rank_bucket",
                "movement_threshold",
                "form_threshold",
                "bet_count",
                "flat_roi",
                "max_drawdown",
            ]
        ].to_string(index=False)
    )
    print("\nBest Top-N Rows")
    print(
        topn_report.head(10)[
            [
                "model_name",
                "selection_mode",
                "edge_threshold",
                "probability_cap",
                "movement_threshold",
                "form_threshold",
                "bet_count",
                "flat_roi",
                "max_drawdown",
            ]
        ].to_string(index=False)
    )
    print(
        f"\nANY CALIBRATED MODEL BEATS MARKET ON TEST PERIOD: "
        f"{any_positive_threshold or any_positive_topn}"
    )
    print(f"ANY RESULT HAS ENOUGH BETS TO BE MEANINGFUL (>= {MEANINGFUL_BET_COUNT}): {any_meaningful}")
    print(f"PROBABILITIES STILL OVERCONFIDENT: {still_overconfident}")

    return {
        "predictions": predictions,
        "calibration_report": calibration_report,
        "threshold_sweep": threshold_sweep,
        "topn_report": topn_report,
        "odds_rank_breakdown": odds_rank_breakdown,
        "prediction_results": prediction_results,
    }


def main() -> None:
    run_late_model_prediction()


if __name__ == "__main__":
    main()
