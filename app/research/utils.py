from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from app.betting.market_helpers import (
    closing_line_metrics,
    commission_adjusted_market_probability,
    edge_bucket_label,
    odds_bucket_label,
)
from app.utils.name_matching import normalize_horse_name

ROOT_DIR = Path(__file__).resolve().parents[2]
PUNTING_FORM_INPUT_DIR = ROOT_DIR / "data" / "punting_form"
RAW_PUNTING_FORM_INPUT_DIR = ROOT_DIR / "data" / "raw" / "punting_form"
BETFAIR_HISTORY_INPUT_DIR = ROOT_DIR / "data" / "betfair_history"
RESEARCH_DATA_DIR = ROOT_DIR / "data" / "research"
RESEARCH_REPORTS_DIR = RESEARCH_DATA_DIR / "reports"
RESEARCH_ARTIFACTS_DIR = ROOT_DIR / "app" / "research" / "artifacts"


def ensure_research_dirs() -> None:
    for path in (RESEARCH_DATA_DIR, RESEARCH_REPORTS_DIR, RESEARCH_ARTIFACTS_DIR):
        path.mkdir(parents=True, exist_ok=True)


def normalize_column_name(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def clean_text(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)


def clean_horse_name(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    text = re.sub(r"^\d+\s*[\.\-]?\s*", "", text)
    return text.strip()


def normalize_track_name(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def parse_date(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None

    for dayfirst in (False, True):
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=dayfirst, utc=False)
        if pd.notna(parsed):
            return parsed.date().isoformat()
    return None


def parse_datetime(value: Any) -> datetime | None:
    text = clean_text(value)
    if text is None:
        return None
    parsed = pd.to_datetime(text, errors="coerce", utc=False)
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def parse_int(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = clean_text(value)
    if text is None:
        return None
    match = re.search(r"-?\d+", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def parse_float(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = clean_text(value)
    if text is None:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def parse_distance(value: Any) -> int | None:
    return parse_int(value)


def parse_finish_position(value: Any) -> int | None:
    text = clean_text(value)
    if text is None:
        return None
    if text.lower() in {"scr", "scratch", "scratched", "nr", "dnf", "np"}:
        return None
    return parse_int(text)


def parse_margin(value: Any) -> float | None:
    text = clean_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"won", "nose", "short head", "head"}:
        lookup = {"won": 0.0, "nose": 0.1, "short head": 0.15, "head": 0.2}
        return lookup[lowered]
    return parse_float(text)


def parse_price(value: Any) -> float | None:
    price = parse_float(value)
    if price is None or price <= 0:
        return None
    return round(price, 4)


def parse_money(value: Any) -> float | None:
    return parse_float(value)


def parse_list_numbers(value: Any) -> list[float]:
    text = clean_text(value)
    if text is None:
        return []
    parts = re.split(r"[|,/;\-\s]+", text)
    numbers = []
    for part in parts:
        number = parse_float(part)
        if number is not None:
            numbers.append(number)
    return numbers


def average(values: list[float | int]) -> float | None:
    clean_values = [float(value) for value in values if value is not None]
    if not clean_values:
        return None
    return round(sum(clean_values) / len(clean_values), 4)


def maybe_json_loads(value: str) -> Any | None:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None


def first_present(row: pd.Series, aliases: list[str]) -> Any:
    for alias in aliases:
        if alias in row.index:
            value = row[alias]
            if value is not None and not (isinstance(value, float) and math.isnan(value)):
                if clean_text(value) is not None:
                    return value
    return None


def read_csv_flex(path: Path) -> pd.DataFrame:
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, low_memory=False, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, low_memory=False)


def read_json_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        records = []
        for line in path.read_text(encoding="utf-8").splitlines():
            payload = maybe_json_loads(line)
            if payload is not None:
                records.extend(flatten_payload_to_records(payload))
        return records

    payload = maybe_json_loads(path.read_text(encoding="utf-8"))
    return flatten_payload_to_records(payload)


def flatten_payload_to_records(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        if "payLoad" in payload:
            return flatten_payload_to_records(payload["payLoad"])
        if all(not isinstance(value, (list, dict)) for value in payload.values()):
            return [payload]
        records: list[dict[str, Any]] = []
        for value in payload.values():
            records.extend(flatten_payload_to_records(value))
        return records
    if isinstance(payload, list):
        records = []
        for item in payload:
            records.extend(flatten_payload_to_records(item))
        return records
    return []


def distance_bucket(distance: Any) -> str:
    metres = parse_distance(distance)
    if metres is None:
        return "unknown"
    if metres < 1200:
        return "sprint"
    if metres < 1800:
        return "middle"
    return "staying"


def to_float_or_zero(value: Any) -> float:
    parsed = parse_float(value)
    return parsed if parsed is not None else 0.0


def compute_max_drawdown(bank_history: list[float]) -> float:
    if not bank_history:
        return 0.0
    peak = bank_history[0]
    max_drawdown = 0.0
    for bank in bank_history:
        peak = max(peak, bank)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - bank) / peak)
    return round(max_drawdown, 4)


def assign_market_rank(frame: pd.DataFrame, odds_column: str = "closing_price") -> pd.DataFrame:
    ranked = frame.copy()
    ranked["_odds_for_rank"] = pd.to_numeric(ranked.get(odds_column), errors="coerce")
    ranked["market_rank"] = (
        ranked.groupby(["race_date", "track_norm", "race_number"], dropna=False)["_odds_for_rank"]
        .rank(method="dense", ascending=True)
    )
    ranked.drop(columns=["_odds_for_rank"], inplace=True)
    return ranked


def derive_price_movement_features(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    enriched["open_to_close_change"] = (
        pd.to_numeric(enriched.get("opening_price"), errors="coerce")
        - pd.to_numeric(enriched.get("closing_price"), errors="coerce")
    )
    enriched["60_to_close_change"] = (
        pd.to_numeric(enriched.get("price_60m"), errors="coerce")
        - pd.to_numeric(enriched.get("closing_price"), errors="coerce")
    )
    enriched["10_to_close_change"] = (
        pd.to_numeric(enriched.get("price_10m"), errors="coerce")
        - pd.to_numeric(enriched.get("closing_price"), errors="coerce")
    )
    enriched["shortened_flag"] = enriched["open_to_close_change"] > 0
    enriched["drifted_flag"] = enriched["open_to_close_change"] < 0
    return enriched


def estimate_runner_probabilities(frame: pd.DataFrame, score_column: str) -> pd.DataFrame:
    enriched = frame.copy()
    scores = pd.to_numeric(enriched.get(score_column), errors="coerce").fillna(0.0)
    enriched["_score_for_prob"] = scores.clip(lower=0.0001)
    score_totals = enriched.groupby(
        ["race_date", "track_norm", "race_number"], dropna=False
    )["_score_for_prob"].transform("sum")
    enriched["estimated_probability"] = (
        enriched["_score_for_prob"] / score_totals.replace({0.0: pd.NA})
    ).fillna(0.0)
    enriched.drop(columns=["_score_for_prob"], inplace=True)
    return enriched


def compute_edge_and_clv_columns(
    frame: pd.DataFrame,
    *,
    odds_column: str = "price_10m",
    closing_column: str = "closing_price",
    commission_rate: float = 0.08,
) -> pd.DataFrame:
    enriched = frame.copy()
    odds = pd.to_numeric(enriched.get(odds_column), errors="coerce")
    closing = pd.to_numeric(enriched.get(closing_column), errors="coerce")
    estimated_probability = pd.to_numeric(
        enriched.get("estimated_probability"), errors="coerce"
    ).fillna(0.0)

    market_probability = odds.apply(
        lambda value: commission_adjusted_market_probability(value, commission_rate)
    )
    enriched["market_probability_adj"] = market_probability
    enriched["edge"] = estimated_probability - market_probability.fillna(0.0)

    clv_metrics = [
        closing_line_metrics(odds_taken, closing_odds)
        for odds_taken, closing_odds in zip(odds, closing)
    ]
    enriched["clv_percent"] = [metric["clv_percent"] for metric in clv_metrics]
    return enriched


def build_group_summary(frame: pd.DataFrame, label_column: str) -> pd.DataFrame:
    working = frame.copy()
    working["won_flag"] = pd.to_numeric(working.get("won_flag"), errors="coerce").fillna(0)
    working["profit_loss"] = pd.to_numeric(working.get("profit_loss"), errors="coerce").fillna(0.0)
    working["stake"] = pd.to_numeric(working.get("stake"), errors="coerce").fillna(0.0)
    working["odds_used"] = pd.to_numeric(working.get("odds_used"), errors="coerce")
    working["edge"] = pd.to_numeric(working.get("edge"), errors="coerce")
    working["clv_percent"] = pd.to_numeric(working.get("clv_percent"), errors="coerce")

    grouped = working.groupby(label_column, dropna=False)
    summary = grouped.agg(
        bets=("won_flag", "size"),
        wins=("won_flag", "sum"),
        profit_loss=("profit_loss", "sum"),
        total_staked=("stake", "sum"),
        average_odds=("odds_used", "mean"),
        average_edge=("edge", "mean"),
        average_clv=("clv_percent", "mean"),
    ).reset_index()
    summary["losses"] = summary["bets"] - summary["wins"]
    summary["win_rate"] = summary["wins"] / summary["bets"].replace({0: pd.NA})
    summary["roi"] = summary["profit_loss"] / summary["total_staked"].replace({0.0: pd.NA})
    return summary.fillna(0.0)


def save_dataframe(frame: pd.DataFrame, path: Path) -> None:
    ensure_research_dirs()
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def json_dump(payload: Any, path: Path) -> None:
    ensure_research_dirs()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def classify_outcome(finish_position: Any) -> int:
    parsed = parse_finish_position(finish_position)
    return 1 if parsed == 1 else 0


def attach_common_labels(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    track_series = enriched["track"] if "track" in enriched.columns else pd.Series([None] * len(enriched), index=enriched.index)
    horse_series = enriched["horse_name"] if "horse_name" in enriched.columns else pd.Series([None] * len(enriched), index=enriched.index)
    distance_series = enriched["distance"] if "distance" in enriched.columns else pd.Series([None] * len(enriched), index=enriched.index)

    enriched["track_norm"] = track_series.map(normalize_track_name)
    enriched["horse_name_clean"] = horse_series.map(clean_horse_name)
    enriched["horse_name_norm"] = enriched["horse_name_clean"].map(
        lambda value: normalize_horse_name(value) if value else None
    )
    odds_series = None
    if "closing_price" in enriched.columns:
        odds_series = enriched["closing_price"]
    elif "price_10m" in enriched.columns:
        odds_series = enriched["price_10m"]
    elif "odds_for_matching" in enriched.columns:
        odds_series = enriched["odds_for_matching"]
    elif "traded_price" in enriched.columns:
        odds_series = enriched["traded_price"]
    else:
        odds_series = pd.Series([None] * len(enriched), index=enriched.index)
    enriched["odds_bucket"] = odds_series.map(odds_bucket_label)

    edge_series = enriched["edge"] if "edge" in enriched.columns else pd.Series([None] * len(enriched), index=enriched.index)
    enriched["edge_bucket"] = edge_series.map(edge_bucket_label)
    enriched["distance_bucket"] = distance_series.map(distance_bucket)
    return enriched
