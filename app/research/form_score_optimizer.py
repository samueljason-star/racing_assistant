from __future__ import annotations

from itertools import product
from pathlib import Path

import pandas as pd

from app.betting.market_helpers import commission_adjusted_market_probability
from app.research.utils import (
    RESEARCH_ARTIFACTS_DIR,
    RESEARCH_DATA_DIR,
    attach_common_labels,
    average,
    estimate_runner_probabilities,
    json_dump,
    parse_float,
    parse_int,
)

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
OUTPUT_PATH = RESEARCH_ARTIFACTS_DIR / "best_form_score_config.json"


def prepare_form_features(frame: pd.DataFrame) -> pd.DataFrame:
    working = attach_common_labels(frame.copy())
    working["last_start_finish"] = working.get("last_start_finish").map(parse_int)
    working["barrier"] = working.get("barrier").map(parse_int)
    working["distance"] = working.get("distance").map(parse_int)
    working["finish_position"] = working.get("finish_position").map(parse_int)
    working["margin"] = working.get("margin").map(parse_float)
    working["won_flag"] = working["finish_position"].map(lambda value: 1 if value == 1 else 0)

    def _parse_pipe_numbers(value: object) -> list[float]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        return [parse_float(part) for part in str(value).split("|") if parse_float(part) is not None]

    finishes = working.get("last_3_finishes", pd.Series(dtype=object)).map(_parse_pipe_numbers)
    margins = working.get("last_3_margins", pd.Series(dtype=object)).map(_parse_pipe_numbers)

    working["average_last_3_finish"] = finishes.map(lambda values: average(values))
    working["best_last_3_finish"] = finishes.map(lambda values: min(values) if values else None)
    working["average_margin_last_3"] = margins.map(lambda values: average(values))
    working["last_start_margin"] = margins.map(lambda values: values[0] if values else None)

    working["distance_change"] = 0.0
    working["similar_distance_flag"] = 1.0
    working["track_condition_match"] = working.get("track_condition").notna().astype(float)
    working["class_change"] = 0.0
    working["jockey_stat"] = 0.0
    working["trainer_stat"] = 0.0
    return working


def _scale_inverse(series: pd.Series, default: float = 0.5) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    max_value = numeric.max()
    min_value = numeric.min()
    if pd.isna(max_value) or pd.isna(min_value) or max_value == min_value:
        return pd.Series([default] * len(series), index=series.index)
    return 1.0 - ((numeric - min_value) / (max_value - min_value))


def _scale_positive(series: pd.Series, default: float = 0.5) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    max_value = numeric.max()
    min_value = numeric.min()
    if pd.isna(max_value) or pd.isna(min_value) or max_value == min_value:
        return pd.Series([default] * len(series), index=series.index)
    return (numeric - min_value) / (max_value - min_value)


def apply_form_formula(frame: pd.DataFrame, config: dict[str, float]) -> pd.DataFrame:
    working = frame.copy()
    finish_component = _scale_inverse(
        working["average_last_3_finish"].fillna(working["last_start_finish"])
    )
    margin_component = _scale_inverse(
        working["average_margin_last_3"].fillna(working["last_start_margin"]).fillna(0.0)
    )
    distance_component = pd.Series(
        [
            1.0 if bool(value) else 0.5
            for value in working["similar_distance_flag"].fillna(0.0).tolist()
        ],
        index=working.index,
    )
    class_component = _scale_inverse(working["class_change"].abs().fillna(0.0))
    barrier_component = _scale_inverse(working["barrier"].fillna(0.0))
    trainer_component = _scale_positive(working["trainer_stat"].fillna(0.0), default=0.0)
    jockey_component = _scale_positive(working["jockey_stat"].fillna(0.0), default=0.0)

    total_weight = sum(config.values()) or 1.0
    working["form_score"] = (
        finish_component * config["finish_weight"]
        + margin_component * config["margin_weight"]
        + distance_component * config["distance_weight"]
        + class_component * config["class_weight"]
        + barrier_component * config["barrier_weight"]
        + trainer_component * config.get("trainer_weight", 0.0)
        + jockey_component * config.get("jockey_weight", 0.0)
    ) / total_weight
    return working


def evaluate_form_formula(frame: pd.DataFrame, config: dict[str, float]) -> dict[str, float | int]:
    scored = apply_form_formula(frame, config)
    scored = estimate_runner_probabilities(scored, "form_score")
    odds = pd.to_numeric(scored.get("price_10m"), errors="coerce").combine_first(
        pd.to_numeric(scored.get("closing_price"), errors="coerce")
    )
    market_probability = odds.apply(lambda value: commission_adjusted_market_probability(value, 0.08))
    scored["edge"] = scored["estimated_probability"] - market_probability.fillna(0.0)

    candidates = scored[(odds >= 3.0) & (odds <= 15.0) & (scored["edge"] >= 0.02)].copy()
    candidates["stake"] = 1.0
    candidates["odds_used"] = pd.to_numeric(candidates.get("price_10m"), errors="coerce").combine_first(
        pd.to_numeric(candidates.get("closing_price"), errors="coerce")
    )
    candidates["profit_loss"] = candidates.apply(
        lambda row: ((float(row["odds_used"]) if pd.notna(row["odds_used"]) else 0.0) - 1.0) * 0.92
        if row["won_flag"] == 1
        else -1.0,
        axis=1,
    )

    brier = ((scored["estimated_probability"] - scored["won_flag"]) ** 2).mean()
    correlation = scored["form_score"].corr(scored["won_flag"])
    total_staked = candidates["stake"].sum()
    profit_loss = candidates["profit_loss"].sum()

    return {
        **config,
        "correlation": round(float(correlation) if pd.notna(correlation) else 0.0, 4),
        "brier_score": round(float(brier) if pd.notna(brier) else 0.0, 4),
        "roi": round(float(profit_loss / total_staked) if total_staked else 0.0, 4),
        "bets": int(len(candidates)),
        "strike_rate": round(float(candidates["won_flag"].mean()) if len(candidates) else 0.0, 4),
        "average_odds": round(float(odds.loc[candidates.index].mean()) if len(candidates) else 0.0, 4),
        "average_edge": round(float(candidates["edge"].mean()) if len(candidates) else 0.0, 4),
        "score": round(
            (
                (float(correlation) if pd.notna(correlation) else 0.0) * 0.35
                + (float(profit_loss / total_staked) if total_staked else 0.0) * 0.45
                - (float(brier) if pd.notna(brier) else 0.0) * 0.2
            ),
            4,
        ),
    }


def optimize_form_score(
    matched_path: Path = MATCHED_PATH,
    output_path: Path = OUTPUT_PATH,
) -> pd.DataFrame:
    if not matched_path.exists() or matched_path.stat().st_size == 0:
        raise RuntimeError(
            "Matched runner dataset is missing or empty. Run import_betfair_history and match_races after placing Betfair history files in data/betfair_history/."
        )
    frame = pd.read_csv(matched_path)
    if frame.empty:
        raise RuntimeError(
            "Matched runner dataset has 0 rows. Add Betfair history files to data/betfair_history/, rerun import_betfair_history, then rerun match_races."
        )
    prepared = prepare_form_features(frame)

    configs = []
    for finish_weight, margin_weight, distance_weight, class_weight, barrier_weight in product(
        (1.0, 1.5, 2.0),
        (0.5, 1.0, 1.5),
        (0.25, 0.5, 1.0),
        (0.0, 0.25, 0.5),
        (0.25, 0.5, 1.0),
    ):
        config = {
            "finish_weight": finish_weight,
            "margin_weight": margin_weight,
            "distance_weight": distance_weight,
            "class_weight": class_weight,
            "barrier_weight": barrier_weight,
            "trainer_weight": 0.0,
            "jockey_weight": 0.0,
        }
        configs.append(evaluate_form_formula(prepared, config))

    results = pd.DataFrame(configs).sort_values(
        ["score", "roi", "bets"], ascending=[False, False, False]
    )
    best = results.iloc[0].to_dict()
    json_dump(best, output_path)

    print("Top 20 Form Score Configurations")
    print(results.head(20).to_string(index=False))
    return results


def main() -> None:
    optimize_form_score()


if __name__ == "__main__":
    main()
