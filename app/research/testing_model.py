from __future__ import annotations

from itertools import product
from pathlib import Path

import joblib
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from app.research.form_score_optimizer import prepare_form_features
from app.research.market_pattern_analysis import build_analysis_frame
from app.research.utils import RESEARCH_ARTIFACTS_DIR, RESEARCH_DATA_DIR, RESEARCH_REPORTS_DIR, compute_max_drawdown, json_dump, save_dataframe

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
MODEL_ARTIFACT_PATH = RESEARCH_ARTIFACTS_DIR / "testing_model.joblib"
MODEL_CONFIG_PATH = RESEARCH_ARTIFACTS_DIR / "best_testing_model_config.json"
MODEL_RESULTS_PATH = RESEARCH_REPORTS_DIR / "testing_model_results.csv"
MODEL_BACKTEST_PATH = RESEARCH_REPORTS_DIR / "testing_model_backtest.csv"
MODEL_FEATURES_PATH = RESEARCH_REPORTS_DIR / "testing_model_feature_importance.csv"

FEATURE_COLUMNS = [
    "form_score",
    "edge",
    "market_probability_adj",
    "price_60m",
    "price_30m",
    "price_10m",
    "closing_price",
    "open_to_close_change",
    "60_to_close_change",
    "10_to_close_change",
    "market_rank",
    "total_matched",
    "barrier",
    "distance",
    "average_last_3_finish",
    "best_last_3_finish",
    "average_margin_last_3",
    "last_start_finish",
    "last_start_margin",
    "track_condition_match",
    "similar_distance_flag",
]


def build_model_frame(matched_path: Path = MATCHED_PATH) -> pd.DataFrame:
    frame = build_analysis_frame(matched_path)
    prepared = prepare_form_features(frame)
    prepared["won_flag"] = pd.to_numeric(prepared.get("won_flag"), errors="coerce").fillna(0).astype(int)
    prepared["race_day"] = pd.to_datetime(prepared["race_date"], errors="coerce")
    prepared = prepared.sort_values(["race_day", "track", "race_number", "horse_name"])
    prepared["odds_used"] = pd.to_numeric(prepared.get("price_10m"), errors="coerce").combine_first(
        pd.to_numeric(prepared.get("closing_price"), errors="coerce")
    )
    return prepared


def split_train_validation(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordered_dates = sorted(frame["race_day"].dropna().unique())
    if not ordered_dates:
        return frame.copy(), frame.copy()
    cutoff = ordered_dates[max(int(len(ordered_dates) * 0.7) - 1, 0)]
    train_frame = frame[frame["race_day"] <= cutoff].copy()
    validation_frame = frame[frame["race_day"] > cutoff].copy()
    if validation_frame.empty:
        validation_frame = train_frame.copy()
    return train_frame, validation_frame


def build_candidate_models() -> dict[str, Pipeline]:
    return {
        "logistic": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", LogisticRegression(max_iter=1000, class_weight="balanced")),
            ]
        ),
        "calibrated_logistic": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "model",
                    CalibratedClassifierCV(
                        estimator=LogisticRegression(max_iter=1000, class_weight="balanced"),
                        cv=3,
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
                        n_estimators=300,
                        min_samples_leaf=5,
                        random_state=42,
                        class_weight="balanced_subsample",
                    ),
                ),
            ]
        ),
    }


def _extract_feature_importance(model: Pipeline, feature_columns: list[str]) -> pd.DataFrame:
    final_model = model.named_steps["model"]
    if hasattr(final_model, "feature_importances_"):
        values = final_model.feature_importances_
    elif hasattr(final_model, "coef_"):
        values = final_model.coef_[0]
    elif hasattr(final_model, "calibrated_classifiers_"):
        base = final_model.calibrated_classifiers_[0].estimator
        values = getattr(base, "coef_", [[0.0] * len(feature_columns)])[0]
    else:
        values = [0.0] * len(feature_columns)
    return pd.DataFrame(
        {"feature": feature_columns, "importance": values}
    ).sort_values("importance", ascending=False)


def _simulate_model_strategy(
    frame: pd.DataFrame,
    *,
    model_name: str,
    probability_threshold: float,
    min_edge: float,
    min_form_score: float,
    max_odds: float,
    max_bets_per_day: int,
) -> tuple[dict[str, object], pd.DataFrame]:
    working = frame.copy()
    working = working[
        (working["predicted_win_probability"] >= probability_threshold)
        & (working["edge"] >= min_edge)
        & (working["form_score"] >= min_form_score)
        & (working["odds_used"] >= 3.0)
        & (working["odds_used"] <= max_odds)
    ].copy()

    if working.empty:
        return {
            "model_name": model_name,
            "probability_threshold": probability_threshold,
            "min_edge": min_edge,
            "min_form_score": min_form_score,
            "max_odds": max_odds,
            "max_bets_per_day": max_bets_per_day,
            "bets": 0,
            "wins": 0,
            "roi": 0.0,
            "drawdown": 0.0,
            "final_bank": 10000.0,
            "score": -1.0,
        }, pd.DataFrame()

    working["race_rank"] = working.groupby(
        ["race_date", "track_norm", "race_number"], dropna=False
    )["predicted_win_probability"].rank(method="dense", ascending=False)
    working = working[working["race_rank"] == 1].copy()
    working = working.sort_values(["race_day", "predicted_win_probability"], ascending=[True, False])
    working["daily_rank"] = working.groupby("race_date").cumcount() + 1
    working = working[working["daily_rank"] <= max_bets_per_day].copy()

    bank = 10000.0
    bank_history = [bank]
    profit_rows = []
    for _, row in working.iterrows():
        stake = round(bank * 0.01, 2)
        profit_loss = round(((row["odds_used"] - 1.0) * 0.92 * stake) if row["won_flag"] == 1 else -stake, 2)
        bank = round(bank + profit_loss, 2)
        bank_history.append(bank)
        profit_rows.append(
            {
                **row.to_dict(),
                "strategy_version": f"testing_model::{model_name}",
                "stake": stake,
                "profit_loss": profit_loss,
                "bank_after_bet": bank,
                "selection_rule": (
                    f"p>={probability_threshold:.2f} edge>={min_edge:.3f} "
                    f"form>={min_form_score:.2f} max_odds<={max_odds:.1f} daily<={max_bets_per_day}"
                ),
            }
        )

    bets = pd.DataFrame(profit_rows)
    total_bets = len(bets)
    wins = int(bets["won_flag"].sum()) if total_bets else 0
    total_staked = float(bets["stake"].sum()) if total_bets else 0.0
    profit_loss = float(bets["profit_loss"].sum()) if total_bets else 0.0
    roi = profit_loss / total_staked if total_staked else 0.0
    drawdown = compute_max_drawdown(bank_history)
    score = roi - (drawdown * 0.6) - (0.2 if total_bets < 20 else 0.0)

    return {
        "model_name": model_name,
        "probability_threshold": probability_threshold,
        "min_edge": min_edge,
        "min_form_score": min_form_score,
        "max_odds": max_odds,
        "max_bets_per_day": max_bets_per_day,
        "bets": total_bets,
        "wins": wins,
        "strike_rate": round(wins / total_bets, 4) if total_bets else 0.0,
        "profit_loss": round(profit_loss, 2),
        "roi": round(roi, 4),
        "drawdown": round(drawdown, 4),
        "final_bank": bank,
        "average_odds": round(float(bets["odds_used"].mean()) if total_bets else 0.0, 4),
        "average_edge": round(float(bets["edge"].mean()) if total_bets else 0.0, 4),
        "average_clv": round(float(bets["clv_percent"].mean()) if total_bets else 0.0, 4),
        "score": round(score, 4),
    }, bets


def develop_testing_model(matched_path: Path = MATCHED_PATH) -> dict[str, pd.DataFrame]:
    frame = build_model_frame(matched_path)
    train_frame, validation_frame = split_train_validation(frame)

    x_train = train_frame[FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")
    y_train = train_frame["won_flag"]
    x_validation = validation_frame[FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")
    y_validation = validation_frame["won_flag"]

    model_results: list[dict[str, object]] = []
    feature_frames: list[pd.DataFrame] = []
    backtest_frames: list[pd.DataFrame] = []
    best_overall: dict[str, object] | None = None
    best_model: Pipeline | None = None

    for model_name, model in build_candidate_models().items():
        model.fit(x_train, y_train)
        validation_probabilities = model.predict_proba(x_validation)[:, 1]

        model_auc = roc_auc_score(y_validation, validation_probabilities) if len(y_validation.unique()) > 1 else 0.0
        model_brier = brier_score_loss(y_validation, validation_probabilities)

        scored_validation = validation_frame.copy()
        scored_validation["predicted_win_probability"] = validation_probabilities

        strategy_results = []
        strategy_bets = []
        for probability_threshold, min_edge, min_form_score, max_odds, max_bets_per_day in product(
            (0.10, 0.12, 0.15, 0.18),
            (0.00, 0.01, 0.02, 0.03),
            (0.30, 0.40, 0.50, 0.60),
            (10.0, 12.0, 15.0),
            (3, 5, 7),
        ):
            summary, bets = _simulate_model_strategy(
                scored_validation,
                model_name=model_name,
                probability_threshold=probability_threshold,
                min_edge=min_edge,
                min_form_score=min_form_score,
                max_odds=max_odds,
                max_bets_per_day=max_bets_per_day,
            )
            summary["auc"] = round(float(model_auc), 4)
            summary["brier_score"] = round(float(model_brier), 4)
            strategy_results.append(summary)
            if not bets.empty:
                strategy_bets.append(bets)

        strategy_frame = pd.DataFrame(strategy_results).sort_values(
            ["score", "roi", "bets"], ascending=[False, False, False]
        )
        best_row = strategy_frame.iloc[0].to_dict()
        model_results.append(best_row)

        if best_overall is None or best_row["score"] > best_overall["score"]:
            best_overall = best_row
            best_model = model

        if strategy_bets:
            best_rule = (
                f"p>={best_row['probability_threshold']:.2f} edge>={best_row['min_edge']:.3f} "
                f"form>={best_row['min_form_score']:.2f} max_odds<={best_row['max_odds']:.1f} "
                f"daily<={int(best_row['max_bets_per_day'])}"
            )
            selected = pd.concat(strategy_bets, ignore_index=True)
            selected = selected[selected["selection_rule"] == best_rule]
            backtest_frames.append(selected)

        feature_importance = _extract_feature_importance(model, FEATURE_COLUMNS)
        feature_importance["model_name"] = model_name
        feature_frames.append(feature_importance)

    results_frame = pd.DataFrame(model_results).sort_values(
        ["score", "roi", "auc"], ascending=[False, False, False]
    )
    feature_frame = pd.concat(feature_frames, ignore_index=True)
    backtest_frame = pd.concat(backtest_frames, ignore_index=True) if backtest_frames else pd.DataFrame()

    save_dataframe(results_frame, MODEL_RESULTS_PATH)
    save_dataframe(feature_frame, MODEL_FEATURES_PATH)
    save_dataframe(backtest_frame, MODEL_BACKTEST_PATH)

    if best_model is not None and best_overall is not None:
        joblib.dump(
            {
                "model": best_model,
                "feature_columns": FEATURE_COLUMNS,
                "best_config": best_overall,
            },
            MODEL_ARTIFACT_PATH,
        )
        json_dump(best_overall, MODEL_CONFIG_PATH)

    print("Testing Model Results")
    print(results_frame.head(20).to_string(index=False))
    print("Top Feature Importances")
    print(feature_frame.groupby("feature", as_index=False)["importance"].mean().sort_values("importance", ascending=False).head(20).to_string(index=False))

    return {
        "results": results_frame,
        "feature_importance": feature_frame,
        "backtest_bets": backtest_frame,
    }


def main() -> None:
    develop_testing_model()


if __name__ == "__main__":
    main()
