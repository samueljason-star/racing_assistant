from __future__ import annotations

from itertools import product
from pathlib import Path

import pandas as pd

from app.research.market_pattern_analysis import build_analysis_frame
from app.research.utils import RESEARCH_ARTIFACTS_DIR, RESEARCH_DATA_DIR, compute_max_drawdown, json_dump

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
OUTPUT_PATH = RESEARCH_ARTIFACTS_DIR / "best_strategy_config.json"


def _simulate_config(frame: pd.DataFrame, config: dict[str, object]) -> dict[str, float | int | str]:
    working = frame.copy()
    working = working[
        (working["price_10m"].fillna(working["closing_price"]) >= config["min_odds"])
        & (working["price_10m"].fillna(working["closing_price"]) <= config["max_odds"])
        & (working["edge"] >= config["min_edge"])
        & (working["form_score"] >= config["min_form_score"])
    ].copy()

    if config["required_movement"] is not None:
        working = working[working["open_to_close_change"] >= config["required_movement"]]

    working["race_day"] = pd.to_datetime(working["race_date"], errors="coerce")
    working = working.sort_values(["race_day", "combined_rank"])
    working["daily_rank"] = working.groupby("race_date").cumcount() + 1
    working = working[working["daily_rank"] <= config["max_bets_per_day"]]

    bank = 10000.0
    bank_history = [bank]
    profits = []
    for _, row in working.iterrows():
        stake = round(bank * 0.01, 2)
        odds_used = row["price_10m"] if pd.notna(row["price_10m"]) else row["closing_price"]
        profit = ((odds_used - 1.0) * 0.92 * stake) if row["won_flag"] == 1 else -stake
        bank = round(bank + profit, 2)
        bank_history.append(bank)
        profits.append(profit)

    total_bets = len(working)
    wins = int(working["won_flag"].sum()) if total_bets else 0
    total_staked = sum(round(value * 0.01, 2) for value in bank_history[:-1]) if total_bets else 0.0
    roi = (sum(profits) / total_staked) if total_staked else 0.0
    drawdown = compute_max_drawdown(bank_history)
    track_share = (
        float(working["track"].value_counts(normalize=True).iloc[0])
        if total_bets and working["track"].notna().any()
        else 0.0
    )
    score = (
        roi
        - drawdown * 0.75
        - (0.2 if total_bets < 20 else 0.0)
        - (0.15 if track_share > 0.5 else 0.0)
    )

    return {
        **config,
        "bets": total_bets,
        "wins": wins,
        "roi": round(float(roi), 4),
        "drawdown": round(drawdown, 4),
        "final_bank": bank,
        "track_concentration": round(track_share, 4),
        "score": round(score, 4),
    }


def optimize_strategy(
    matched_path: Path = MATCHED_PATH,
    output_path: Path = OUTPUT_PATH,
) -> pd.DataFrame:
    frame = build_analysis_frame(matched_path)
    frame["combined_rank"] = (
        frame.groupby(["race_date", "track_norm", "race_number"], dropna=False)["form_score"]
        .rank(method="dense", ascending=False)
    )

    ordered_dates = sorted(pd.to_datetime(frame["race_date"], errors="coerce").dropna().unique())
    if ordered_dates:
        cutoff = ordered_dates[max(int(len(ordered_dates) * 0.7) - 1, 0)]
        train_frame = frame[pd.to_datetime(frame["race_date"], errors="coerce") <= cutoff].copy()
        validation_frame = frame[pd.to_datetime(frame["race_date"], errors="coerce") > cutoff].copy()
        if validation_frame.empty:
            validation_frame = train_frame.copy()
    else:
        train_frame = frame.copy()
        validation_frame = frame.copy()

    configs = []
    for min_odds, max_odds, min_edge, min_form_score, required_movement, max_bets_per_day in product(
        (3.0, 4.0, 5.0),
        (10.0, 12.0, 15.0),
        (0.01, 0.02, 0.035, 0.05),
        (0.45, 0.55, 0.65, 0.75),
        (None, 0.0, 0.25, 0.5),
        (3, 5, 7),
    ):
        if min_odds >= max_odds:
            continue
        config = {
            "min_odds": min_odds,
            "max_odds": max_odds,
            "min_edge": min_edge,
            "min_form_score": min_form_score,
            "required_movement": required_movement,
            "max_bets_per_day": max_bets_per_day,
        }
        train_result = _simulate_config(train_frame, config)
        validation_result = _simulate_config(validation_frame, config)
        stability_penalty = abs(train_result["roi"] - validation_result["roi"]) * 0.5
        configs.append(
            {
                **config,
                "train_roi": train_result["roi"],
                "validation_roi": validation_result["roi"],
                "train_drawdown": train_result["drawdown"],
                "validation_drawdown": validation_result["drawdown"],
                "train_bets": train_result["bets"],
                "validation_bets": validation_result["bets"],
                "score": round(validation_result["score"] - stability_penalty, 4),
            }
        )

    results = pd.DataFrame(configs).sort_values("score", ascending=False)
    best_safe = results.sort_values(
        ["validation_drawdown", "score", "validation_bets"], ascending=[True, False, False]
    ).iloc[0].to_dict()
    best_aggressive = results.iloc[0].to_dict()
    payload = {
        "recommended_safe_strategy": best_safe,
        "recommended_aggressive_strategy": best_aggressive,
    }
    json_dump(payload, output_path)

    print("Top 20 Strategies")
    print(results.head(20).to_string(index=False))
    print("Recommended safe strategy")
    print(best_safe)
    print("Recommended aggressive strategy")
    print(best_aggressive)
    return results


def main() -> None:
    optimize_strategy()


if __name__ == "__main__":
    main()
