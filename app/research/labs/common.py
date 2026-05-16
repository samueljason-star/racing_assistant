from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from app.research.clv_market_agreement_model import (
    _month_concentration,
    _prediction_bucket,
    _prepare_feature_set,
    _safe_auc,
    _safe_brier,
    _safe_divide,
    _safe_log_loss,
    _track_concentration,
    _weekly_positive_rate,
    _monthly_positive_rate,
)
from app.research.clv_prediction_model import MATCHED_PATH, _prepare_bets, _prepare_clv_frame
from app.research.market_residual_model import RACE_KEYS
from app.research.utils import RESEARCH_REPORTS_DIR, ensure_research_dirs, save_dataframe


@dataclass(frozen=True)
class ReportSpec:
    filename: str
    title: str
    builder: Callable[[pd.DataFrame, Any], pd.DataFrame]


@dataclass(frozen=True)
class LabConfig:
    slug: str
    title: str
    purpose: str
    questions: list[str]
    reports: list[ReportSpec]
    assumptions: list[str]


def build_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--target", choices=["sixty_to_close", "open_to_close"], default="sixty_to_close")
    parser.add_argument("--matched-path", type=Path, default=MATCHED_PATH)
    return parser


def _zscore(series: pd.Series, invert: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() < 2:
        return pd.Series(0.0, index=series.index)
    std = float(numeric.std(ddof=0))
    if std == 0 or np.isnan(std):
        values = pd.Series(0.0, index=series.index)
    else:
        values = (numeric - float(numeric.mean())) / std
    return -values if invert else values


def _safe_numeric_series(frame: pd.DataFrame, column: str, fill: float = 0.0) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(fill, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(fill)


def _string_series(frame: pd.DataFrame, column: str, fill: str = "unknown") -> pd.Series:
    if column not in frame.columns:
        return pd.Series(fill, index=frame.index, dtype=str)
    values = frame[column].fillna(fill).astype(str)
    values = values.replace({"nan": fill, "None": fill})
    return values


def _safe_qbucket(series: pd.Series, labels: list[str]) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.nunique() < len(labels):
        return pd.Series(["unknown"] * len(series), index=series.index, dtype=str)
    ranked = valid.rank(method="first")
    bucket = pd.qcut(ranked, q=len(labels), labels=labels, duplicates="drop").astype(str)
    output = pd.Series("unknown", index=series.index, dtype=str)
    output.loc[valid.index] = bucket
    return output


def market_rank_bucket(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        numeric,
        bins=[0, 1, 2, 3, 5, np.inf],
        labels=["rank_1", "rank_2", "rank_3", "rank_4_to_5", "rank_6_plus"],
        include_lowest=True,
    ).astype(str)


def odds_bucket(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.cut(
        numeric,
        bins=[0, 2, 4, 6, 10, np.inf],
        labels=["odds_1_to_2", "odds_2_to_4", "odds_4_to_6", "odds_6_to_10", "odds_10_plus"],
        include_lowest=True,
    ).astype(str)


def load_lab_frame(matched_path: Path, target: str) -> tuple[pd.DataFrame, Any]:
    ensure_research_dirs()
    frame, target_spec, _ = _prepare_clv_frame(matched_path, target)
    frame = _prepare_bets(frame.copy())
    frame["market_rank_bucket"] = market_rank_bucket(frame["market_rank_current"])
    frame["odds_bucket"] = odds_bucket(frame["anchor_price"])
    frame["field_size_bucket"] = _string_series(frame, "field_size_bucket")
    frame["distance_bucket"] = pd.cut(
        _safe_numeric_series(frame, "distance", fill=np.nan),
        bins=[0, 1200, 1600, 2200, np.inf],
        labels=["sprint", "mile", "middle", "staying"],
        include_lowest=True,
    ).astype(str)
    frame["weekend_flag"] = np.where(pd.to_datetime(frame["race_date"], errors="coerce").dt.weekday >= 5, "weekend", "weekday")
    frame["clv_hit_flag"] = (frame[target_spec.target_clv_column] > 0).astype(int)
    frame["steam_flag"] = (_safe_numeric_series(frame, "movement_60_to_close") > 0).astype(int)
    frame["drift_flag"] = (_safe_numeric_series(frame, "movement_60_to_close") < 0).astype(int)
    frame["fake_steam_flag"] = ((frame["steam_flag"] == 1) & (frame["won_flag"] == 0)).astype(int)
    frame["fake_drift_flag"] = ((frame["drift_flag"] == 1) & (frame["won_flag"] == 1)).astype(int)
    frame["late_price_volatility"] = (_safe_numeric_series(frame, "movement_60_to_close").abs() + _safe_numeric_series(frame, "movement_open_to_60").abs())
    frame["market_disagreement_proxy"] = _safe_numeric_series(frame, "model_minus_market_prob").abs() + _safe_numeric_series(frame, "residual_score").abs()
    frame["barrier_bucket"] = pd.cut(
        _safe_numeric_series(frame, "barrier", fill=np.nan),
        bins=[0, 4, 8, 20, np.inf],
        labels=["inside", "middle", "wide", "extreme"],
        include_lowest=True,
    ).astype(str)
    frame["top3_market_prob_sum"] = frame.groupby(RACE_KEYS, dropna=False)["anchor_market_prob_norm"].transform(
        lambda s: float(s.sort_values(ascending=False).head(3).sum())
    )
    leader_proxy = (
        (pd.to_numeric(frame["barrier"], errors="coerce") <= 4)
        & (pd.to_numeric(frame["market_rank_current"], errors="coerce") <= 4)
    ).astype(int)
    frame["expected_leader_count"] = leader_proxy.groupby([frame[key] for key in RACE_KEYS], dropna=False).transform("sum")
    frame["favourite_density"] = frame["top3_market_prob_sum"]
    frame["pace_pressure_proxy"] = (
        _zscore(frame["expected_leader_count"])
        + _zscore(frame["field_size"])
        - _zscore(frame["top3_market_prob_sum"])
    )
    frame["pace_regime"] = pd.cut(
        frame["pace_pressure_proxy"],
        bins=[-np.inf, -0.5, 0.5, np.inf],
        labels=["low_pressure", "balanced", "high_pressure"],
        include_lowest=True,
    ).astype(str)
    frame["stable_intent_proxy"] = (
        _zscore(_safe_numeric_series(frame, "trainer_stat"))
        + _zscore(_safe_numeric_series(frame, "jockey_stat"))
        - _zscore(_safe_numeric_series(frame, "class_change").abs())
        - _zscore(_safe_numeric_series(frame, "days_since_last_start").abs())
    ) / 4.0
    frame["prep_stage"] = np.select(
        [
            _safe_numeric_series(frame, "days_since_last_start") >= 120,
            _safe_numeric_series(frame, "days_since_last_start").between(30, 119),
            _safe_numeric_series(frame, "days_since_last_start").between(8, 29),
        ],
        ["first_up", "fresh", "fit"],
        default="unknown",
    )
    frame["track_bias_proxy"] = (
        _zscore(_safe_numeric_series(frame, "barrier"), invert=True)
        + _zscore(_safe_numeric_series(frame, "track_condition_match"))
    ) / 2.0
    frame["sectional_efficiency_proxy"] = (
        -_zscore(_safe_numeric_series(frame, "last_start_margin"))
        - _zscore(_safe_numeric_series(frame, "average_margin_last_3"))
        + _zscore(_safe_numeric_series(frame, "best_last_3_finish"), invert=True)
    ) / 3.0
    frame["runner_efficiency_proxy"] = _safe_numeric_series(frame, "market_rank_current") - _safe_numeric_series(frame, "finish_position", fill=np.nan)
    frame["field_compression_proxy"] = frame.groupby(RACE_KEYS, dropna=False)["anchor_price"].transform(
        lambda s: float(pd.to_numeric(s, errors="coerce").std(ddof=0)) if len(s) else 0.0
    )
    frame["race_class_group"] = _string_series(frame, "race_class_group")
    return frame.reset_index(drop=True), target_spec


def summarize_groups(frame: pd.DataFrame, group_columns: list[str], target_spec: Any, *, min_sample: int = 80, sort_by: list[str] | None = None) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    working = frame.copy()
    for column in group_columns:
        if column not in working.columns:
            working[column] = "unknown"
    grouped = working.groupby(group_columns, dropna=False, observed=False)
    rows: list[dict[str, Any]] = []
    for keys, subset in grouped:
        keys_tuple = keys if isinstance(keys, tuple) else (keys,)
        row = {column: value for column, value in zip(group_columns, keys_tuple)}
        row.update(
            {
                "selections": int(len(subset)),
                "races": int(subset[RACE_KEYS].drop_duplicates().shape[0]),
                "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                "average_clv": float(subset[target_spec.target_clv_column].mean()),
                "median_clv": float(subset[target_spec.target_clv_column].median()),
                "clv_hit_rate": float(subset["clv_hit_flag"].mean()),
                "roi": _safe_divide(float(subset["profit_loss"].sum()), float(subset["stake"].sum())),
                "strike_rate": float(subset["won_flag"].mean()),
                "average_odds": float(subset["anchor_price"].mean()),
                "average_market_rank": float(subset["market_rank_current"].mean()),
                "average_field_size": float(subset["field_size"].mean()),
                "track_concentration": _track_concentration(subset),
                "month_concentration": _month_concentration(subset),
                "weekly_positive_clv_rate": _weekly_positive_rate(subset, target_spec.target_clv_column),
                "monthly_positive_clv_rate": _monthly_positive_rate(subset, target_spec.target_clv_column),
            }
        )
        row["low_sample_size"] = bool(row["selections"] < min_sample)
        row["high_concentration"] = bool(row["track_concentration"] > 0.45 or row["month_concentration"] > 0.45)
        row["likely_longshot_noise"] = bool(row["average_odds"] >= 15 or row["average_market_rank"] > 5.5)
        row["likely_false_positive"] = bool(row["low_sample_size"] or row["high_concentration"] or row["likely_longshot_noise"])
        row["commercially_plausible"] = bool(
            not row["likely_false_positive"]
            and row["average_odds"] <= 10
            and row["average_market_rank"] <= 5
            and row["average_clv"] > 0
        )
        rows.append(row)
    report = pd.DataFrame(rows)
    if report.empty:
        return report
    sort_columns = sort_by or ["commercially_plausible", "average_clv", "actual_shorten_rate"]
    ascending = [False] * len(sort_columns)
    return report.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)


def simple_calibration(frame: pd.DataFrame, probability_column: str, target_spec: Any, group_column: str) -> pd.DataFrame:
    if probability_column not in frame.columns:
        return pd.DataFrame()
    working = frame.copy()
    working[group_column] = _string_series(working, group_column)
    return summarize_groups(working.assign(prob_bucket=_prediction_bucket(working[probability_column])), [group_column, "prob_bucket"], target_spec, min_sample=40)


def rolling_month_report(frame: pd.DataFrame, target_spec: Any) -> pd.DataFrame:
    month_rows: list[dict[str, Any]] = []
    monthly = frame.groupby("race_month", dropna=False, observed=False)
    for month, subset in monthly:
        month_rows.append(
            {
                "race_month": str(month),
                "selections": int(len(subset)),
                "actual_shorten_rate": float(subset[target_spec.target_flag_column].mean()),
                "average_clv": float(subset[target_spec.target_clv_column].mean()),
                "roi": _safe_divide(float(subset["profit_loss"].sum()), float(subset["stake"].sum())),
                "volatility": float(subset["late_price_volatility"].mean()),
            }
        )
    return pd.DataFrame(month_rows)


def write_summary(config: LabConfig, target_spec: Any, reports: dict[str, pd.DataFrame]) -> str:
    top_finding = "No useful signal found."
    plausible = False
    for frame in reports.values():
        if frame.empty:
            continue
        if "commercially_plausible" in frame.columns and frame["commercially_plausible"].any():
            best = frame[frame["commercially_plausible"]].iloc[0]
            top_finding = ", ".join(
                [
                    f"{column}={best[column]}"
                    for column in frame.columns
                    if column not in {"commercially_plausible", "likely_false_positive"} and str(best[column]) not in {"nan", "None"}
                ][:5]
            )
            plausible = True
            break
        best = frame.iloc[0]
        top_finding = ", ".join(
            [f"{column}={best[column]}" for column in frame.columns[:5]]
        )
    verdict = "worth further research" if plausible else "likely weak/noisy"
    lines = [
        f"# {config.title}",
        "",
        f"- Target: `{target_spec.name}`",
        f"- Purpose: {config.purpose}",
        "",
        "## Research Questions",
    ]
    lines.extend([f"- {question}" for question in config.questions])
    lines.extend(
        [
            "",
            "## Structural Read",
            f"1. What structural behaviour was observed? {top_finding}",
            f"2. Is there evidence of real market inefficiency? {'Some weak evidence' if plausible else 'Not clearly'}",
            f"3. Is the signal likely noise? {'Noisy / mixed' if not plausible else 'Partly'}",
            f"4. Is the signal commercially plausible? {'Yes, in a narrow sense' if plausible else 'Not yet'}",
            f"5. Is the signal worth further research? {'Yes' if plausible else 'Only as feature discovery'}",
            f"6. What features or follow-up labs should be built next? {config.assumptions[-1] if config.assumptions else 'Investigate stronger non-market context features.'}",
            "",
            "## Assumptions",
        ]
    )
    lines.extend([f"- {assumption}" for assumption in config.assumptions])
    lines.extend(
        [
            "",
            "## Verdict",
            f"- {verdict}",
            "- Truth over optimism: if a lab only finds longshot noise or concentration spikes, it is not an edge.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_lab(config: LabConfig, matched_path: Path, target: str) -> dict[str, pd.DataFrame | str]:
    frame, target_spec = load_lab_frame(matched_path, target)
    print(f"Running {config.slug} on target={target} with {len(frame)} rows...")
    outputs: dict[str, pd.DataFrame] = {}
    for spec in config.reports:
        report = spec.builder(frame.copy(), target_spec)
        outputs[spec.filename] = report
        save_dataframe(report, RESEARCH_REPORTS_DIR / spec.filename)
    summary = write_summary(config, target_spec, outputs)
    summary_path = RESEARCH_REPORTS_DIR / f"{config.slug}_summary.md"
    summary_path.write_text(summary, encoding="utf-8")
    print(f"Wrote summary: {summary_path}")
    return {**outputs, "summary": summary}
