from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.betting.market_helpers import edge_bucket_label, odds_bucket_label
from app.research.market_pattern_analysis import build_analysis_frame
from app.research.utils import (
    RESEARCH_DATA_DIR,
    RESEARCH_REPORTS_DIR,
    compute_max_drawdown,
    distance_bucket,
    save_dataframe,
)

MATCHED_PATH = RESEARCH_DATA_DIR / "matched_runner_data.csv"
SUMMARY_PATH = RESEARCH_REPORTS_DIR / "backtest_summary.csv"
BETS_PATH = RESEARCH_REPORTS_DIR / "backtest_bets.csv"


def _base_strategy_mask(frame: pd.DataFrame, strategy_name: str) -> pd.Series:
    if strategy_name == "model_edge_only":
        return frame["edge"] >= 0.03
    if strategy_name == "form_score_only":
        return frame["form_score"] >= 0.65
    if strategy_name == "form_score_plus_market_edge":
        return (frame["form_score"] >= 0.60) & (frame["edge"] >= 0.02)
    if strategy_name == "form_score_plus_shortening":
        return (frame["form_score"] >= 0.60) & (frame["shortened_flag"] == True)
    if strategy_name == "favourite_near_favourite_value":
        return (frame["market_rank"] <= 3) & (frame["edge"] >= 0.01)
    raise ValueError(f"Unknown strategy: {strategy_name}")


def simulate_strategy(
    frame: pd.DataFrame,
    strategy_name: str,
    *,
    starting_bank: float = 10000.0,
    stake_method: str = "pct",
    stake_pct: float = 0.01,
    flat_stake: float = 100.0,
) -> tuple[dict[str, object], pd.DataFrame]:
    working = frame.copy()
    working["strategy_version"] = strategy_name
    working = working[_base_strategy_mask(working, strategy_name)].copy()
    working["race_day"] = pd.to_datetime(working["race_date"], errors="coerce")
    working = working.sort_values(["race_day", "race_number", "horse_name"])

    bank = starting_bank
    bank_history = [bank]
    bet_rows: list[dict[str, object]] = []

    for _, row in working.iterrows():
        stake = flat_stake if stake_method == "flat" else round(bank * stake_pct, 2)
        odds_used = row["price_10m"] if pd.notna(row["price_10m"]) else row["closing_price"]
        if pd.isna(odds_used) or odds_used <= 1:
            continue
        won_flag = int(row["won_flag"])
        profit_loss = round(((odds_used - 1.0) * 0.92 * stake) if won_flag else -stake, 2)
        bank = round(bank + profit_loss, 2)
        bank_history.append(bank)
        bet_rows.append(
            {
                **row.to_dict(),
                "strategy_version": strategy_name,
                "stake": stake,
                "odds_used": odds_used,
                "profit_loss": profit_loss,
                "bank_after_bet": bank,
            }
        )

    bets = pd.DataFrame(bet_rows)
    total_bets = len(bets)
    wins = int(bets["won_flag"].sum()) if total_bets else 0
    losses = total_bets - wins
    total_staked = float(bets["stake"].sum()) if total_bets else 0.0
    profit_loss = round(float(bets["profit_loss"].sum()) if total_bets else 0.0, 2)

    summary = {
        "strategy_version": strategy_name,
        "total_bets": total_bets,
        "wins": wins,
        "losses": losses,
        "strike_rate": round(wins / total_bets, 4) if total_bets else 0.0,
        "profit_loss": profit_loss,
        "roi": round(profit_loss / total_staked, 4) if total_staked else 0.0,
        "max_drawdown": compute_max_drawdown(bank_history),
        "final_bank": bank,
        "average_odds": round(float(bets["odds_used"].mean()) if total_bets else 0.0, 4),
        "average_edge": round(float(bets["edge"].mean()) if total_bets else 0.0, 4),
        "average_clv": round(float(bets["clv_percent"].mean()) if total_bets else 0.0, 4),
    }
    return summary, bets


def run_backtests(
    matched_path: Path = MATCHED_PATH,
    *,
    starting_bank: float = 10000.0,
    stake_method: str = "pct",
    stake_pct: float = 0.01,
    flat_stake: float = 100.0,
) -> dict[str, pd.DataFrame]:
    frame = build_analysis_frame(matched_path)
    strategies = [
        "model_edge_only",
        "form_score_only",
        "form_score_plus_market_edge",
        "form_score_plus_shortening",
        "favourite_near_favourite_value",
    ]

    summaries: list[dict[str, object]] = []
    bet_frames: list[pd.DataFrame] = []
    segment_frames: list[pd.DataFrame] = []

    for strategy_name in strategies:
        summary, bets = simulate_strategy(
            frame,
            strategy_name,
            starting_bank=starting_bank,
            stake_method=stake_method,
            stake_pct=stake_pct,
            flat_stake=flat_stake,
        )
        summaries.append(summary)
        bet_frames.append(bets)
        if not bets.empty:
            segmented = bets.copy()
            segmented["odds_bucket"] = segmented["odds_used"].map(odds_bucket_label)
            segmented["edge_bucket"] = segmented["edge"].map(edge_bucket_label)
            segmented["distance_bucket"] = segmented["distance"].map(distance_bucket)
            segment_frames.append(segmented)

    summary_frame = pd.DataFrame(summaries).sort_values(
        ["roi", "profit_loss", "total_bets"], ascending=[False, False, False]
    )
    bets_frame = pd.concat(bet_frames, ignore_index=True) if bet_frames else pd.DataFrame()

    if segment_frames:
        segments = pd.concat(segment_frames, ignore_index=True)
        segment_reports = []
        for segment_type in ("odds_bucket", "track", "distance_bucket", "class_name", "strategy_version"):
            grouped = (
                segments.groupby(["strategy_version", segment_type], dropna=False)
                .agg(
                    total_bets=("won_flag", "size"),
                    wins=("won_flag", "sum"),
                    profit_loss=("profit_loss", "sum"),
                    total_staked=("stake", "sum"),
                    average_odds=("odds_used", "mean"),
                    average_edge=("edge", "mean"),
                )
                .reset_index()
            )
            grouped["segment_type"] = segment_type
            grouped["roi"] = grouped["profit_loss"] / grouped["total_staked"].replace({0.0: pd.NA})
            segment_reports.append(grouped.fillna(0.0))
        segment_frame = pd.concat(segment_reports, ignore_index=True)
        summary_frame = pd.concat([summary_frame, segment_frame], ignore_index=True, sort=False)

    save_dataframe(summary_frame, SUMMARY_PATH)
    save_dataframe(bets_frame, BETS_PATH)

    print("Best Backtest Strategies")
    print(summary_frame.head(20).to_string(index=False))
    return {"summary": summary_frame, "bets": bets_frame}


def main() -> None:
    run_backtests()


if __name__ == "__main__":
    main()
