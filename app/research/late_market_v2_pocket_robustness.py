from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.betting.market_helpers import closing_line_metrics
from app.research.late_market_v2_backtest import MATCHED_PATH, ODDS_TIME_SERIES_PATH, _load_frame, _score_runners
from app.research.utils import RESEARCH_REPORTS_DIR, compute_max_drawdown, save_dataframe

OUTPUT_PATH = RESEARCH_REPORTS_DIR / "late_v2_pocket_robustness.csv"
FLAT_STAKE = 100.0


def _add_clv(frame: pd.DataFrame) -> pd.DataFrame:
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


def _safe_mean(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    value = pd.to_numeric(frame[column], errors="coerce").mean()
    return round(float(value), 4) if pd.notna(value) else 0.0


def _simulate_flat(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float | int]]:
    working = frame.copy().sort_values(["race_date", "track", "race_number", "horse_name"])
    if working.empty:
        return working, {
            "bets": 0,
            "wins": 0,
            "losses": 0,
            "strike_rate": 0.0,
            "profit_loss": 0.0,
            "roi": 0.0,
            "max_drawdown": 0.0,
            "average_odds": 0.0,
            "average_movement_score": 0.0,
            "average_form_score": 0.0,
            "average_edge": 0.0,
            "average_clv": 0.0,
        }

    working["stake"] = FLAT_STAKE
    working["profit_loss"] = working.apply(
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
    for profit in working["profit_loss"].tolist():
        bank = round(bank + float(profit), 2)
        bank_history.append(bank)

    bets = len(working)
    wins = int(working["won_flag"].sum())
    losses = bets - wins
    profit_loss = round(float(working["profit_loss"].sum()), 2)
    total_staked = bets * FLAT_STAKE
    summary = {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "strike_rate": round(wins / bets, 4) if bets else 0.0,
        "profit_loss": profit_loss,
        "roi": round(profit_loss / total_staked, 4) if total_staked else 0.0,
        "max_drawdown": compute_max_drawdown(bank_history),
        "average_odds": _safe_mean(working, "latest_odds"),
        "average_movement_score": _safe_mean(working, "movement_score"),
        "average_form_score": _safe_mean(working, "form_score"),
        "average_edge": _safe_mean(working, "edge"),
        "average_clv": _safe_mean(working, "clv_percent"),
    }
    return working, summary


def _build_base_scored() -> pd.DataFrame:
    frame, _ = _load_frame(MATCHED_PATH, ODDS_TIME_SERIES_PATH)
    scored = _add_clv(_score_runners(frame))
    scored["race_date"] = pd.to_datetime(scored["race_date"], errors="coerce")
    scored["week_start"] = scored["race_date"] - pd.to_timedelta(scored["race_date"].dt.weekday, unit="D")
    return scored


def _apply_variant(scored: pd.DataFrame, *, min_odds: float, max_odds: float, min_movement: float, rank_values: set[int], label: str) -> pd.DataFrame:
    working = scored.copy()
    rank_series = pd.to_numeric(working["market_rank"], errors="coerce")
    mask = (
        working["latest_odds"].between(min_odds, max_odds, inclusive="both")
        & rank_series.isin(rank_values)
        & (working["movement_score"] >= min_movement)
        & (working["form_score"] >= 0.30)
        & working["has_history"]
    )
    selected = working[mask].copy()
    selected["variant_label"] = label
    return selected


def _summary_row(section: str, bucket: str, variant: str, summary: dict[str, float | int]) -> dict[str, object]:
    return {
        "report_section": section,
        "bucket": bucket,
        "variant": variant,
        **summary,
    }


def run_pocket_robustness() -> pd.DataFrame:
    scored = _build_base_scored()
    variants = {
        "A_odds3_5_rank4_5_move0_75": dict(min_odds=3.0, max_odds=5.0, min_movement=0.75, rank_values={4, 5}),
        "B_odds3_5_rank4_5_move0_70": dict(min_odds=3.0, max_odds=5.0, min_movement=0.70, rank_values={4, 5}),
        "C_odds3_6_rank4_5_move0_75": dict(min_odds=3.0, max_odds=6.0, min_movement=0.75, rank_values={4, 5}),
        "D_odds3_5_rank3_5_move0_75": dict(min_odds=3.0, max_odds=5.0, min_movement=0.75, rank_values={3, 4, 5}),
    }

    rows: list[dict[str, object]] = []
    variant_bets: dict[str, pd.DataFrame] = {}

    for label, settings in variants.items():
        selected = _apply_variant(scored, label=label, **settings)
        bets, summary = _simulate_flat(selected)
        variant_bets[label] = bets
        rows.append(_summary_row("full_month", "all", label, summary))

        minus_best = bets.sort_values("profit_loss", ascending=False).iloc[1:].copy() if len(bets) >= 1 else bets.copy()
        _, minus_best_summary = _simulate_flat(minus_best)
        rows.append(_summary_row("leave_winner_out", "remove_best_winner", label, minus_best_summary))

        minus_top2 = bets.sort_values("profit_loss", ascending=False).iloc[2:].copy() if len(bets) >= 2 else bets.iloc[0:0].copy()
        _, minus_top2_summary = _simulate_flat(minus_top2)
        rows.append(_summary_row("leave_winner_out", "remove_top_2_winners", label, minus_top2_summary))

        worst_loser = bets.sort_values("profit_loss", ascending=True)
        if not worst_loser.empty and float(worst_loser.iloc[0]["profit_loss"]) < 0:
            minus_worst_loser = worst_loser.iloc[1:].copy()
        else:
            minus_worst_loser = bets.copy()
        _, minus_worst_loser_summary = _simulate_flat(minus_worst_loser)
        rows.append(_summary_row("leave_loser_out", "remove_worst_loser", label, minus_worst_loser_summary))

        for week_start, week_frame in bets.groupby("week_start", dropna=False):
            _, week_summary = _simulate_flat(week_frame)
            rows.append(
                _summary_row(
                    "weekly",
                    str(pd.to_datetime(week_start).date()) if pd.notna(week_start) else "unknown",
                    label,
                    week_summary,
                )
            )

        for track, track_frame in bets.groupby("track", dropna=False):
            _, track_summary = _simulate_flat(track_frame)
            rows.append(_summary_row("track", str(track), label, track_summary))

        for rank_value, rank_frame in bets.groupby("market_rank", dropna=False):
            _, rank_summary = _simulate_flat(rank_frame)
            rows.append(_summary_row("rank_split", f"rank_{int(rank_value)}", label, rank_summary))

    report = pd.DataFrame(rows)
    save_dataframe(report, OUTPUT_PATH)

    main_variant = report[
        (report["report_section"] == "full_month")
        & (report["variant"] == "A_odds3_5_rank4_5_move0_75")
    ].iloc[0]
    remove_best = report[
        (report["report_section"] == "leave_winner_out")
        & (report["bucket"] == "remove_best_winner")
        & (report["variant"] == "A_odds3_5_rank4_5_move0_75")
    ].iloc[0]
    remove_top2 = report[
        (report["report_section"] == "leave_winner_out")
        & (report["bucket"] == "remove_top_2_winners")
        & (report["variant"] == "A_odds3_5_rank4_5_move0_75")
    ].iloc[0]
    survives = bool(
        main_variant["roi"] > 0
        and remove_best["roi"] > 0
        and remove_top2["roi"] > 0
    )

    print("Late V2 Pocket Robustness")
    print(report[report["report_section"] == "full_month"].to_string(index=False))
    print("\nLeave Winner Out")
    print(
        report[
            report["report_section"].isin(["leave_winner_out", "leave_loser_out"])
        ].to_string(index=False)
    )
    print("\nWeekly Split")
    print(report[report["report_section"] == "weekly"].to_string(index=False))
    print("\nTrack Split")
    print(report[report["report_section"] == "track"].to_string(index=False))
    print("\nRank Split")
    print(report[report["report_section"] == "rank_split"].to_string(index=False))
    print(f"\nPOCKET SURVIVES ROBUSTNESS CHECKS: {survives}")
    return report


def main() -> None:
    run_pocket_robustness()


if __name__ == "__main__":
    main()
