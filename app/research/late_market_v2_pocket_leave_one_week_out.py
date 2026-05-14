from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.betting.market_helpers import closing_line_metrics
from app.research.late_market_v2_backtest import MATCHED_PATH, ODDS_TIME_SERIES_PATH, _load_frame, _score_runners
from app.research.utils import RESEARCH_REPORTS_DIR, compute_max_drawdown, save_dataframe

OUTPUT_PATH = RESEARCH_REPORTS_DIR / "late_v2_pocket_leave_one_week_out.csv"
FLAT_STAKE = 100.0
VARIANT_LABEL = "D_odds3_5_rank3_5_move0_75"


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


def _build_variant_d_frame() -> pd.DataFrame:
    frame, _ = _load_frame(MATCHED_PATH, ODDS_TIME_SERIES_PATH)
    scored = _add_clv(_score_runners(frame))
    scored["race_date"] = pd.to_datetime(scored["race_date"], errors="coerce")
    scored["week_start"] = scored["race_date"] - pd.to_timedelta(scored["race_date"].dt.weekday, unit="D")
    rank_series = pd.to_numeric(scored["market_rank"], errors="coerce")
    mask = (
        scored["latest_odds"].between(3.0, 5.0, inclusive="both")
        & rank_series.isin({3, 4, 5})
        & (scored["movement_score"] >= 0.75)
        & (scored["form_score"] >= 0.30)
        & scored["has_history"]
    )
    selected = scored[mask].copy()
    selected["variant"] = VARIANT_LABEL
    return selected.sort_values(["race_date", "track", "race_number", "horse_name"])


def _simulate_flat(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float | int]]:
    working = frame.copy()
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


def run_leave_one_week_out() -> pd.DataFrame:
    variant_frame = _build_variant_d_frame()
    unique_weeks = sorted(week for week in variant_frame["week_start"].dropna().unique())
    rows: list[dict[str, object]] = []

    _, full_summary = _simulate_flat(variant_frame)
    rows.append(
        {
            "report_section": "full_month",
            "week_bucket": "all_weeks",
            "removed_week": "",
            "variant": VARIANT_LABEL,
            **full_summary,
            "sample_warning": "ONE_MONTH_SMALL_SAMPLE" if int(full_summary["bets"]) < 100 else "",
        }
    )

    for week in unique_weeks:
        week_label = str(pd.to_datetime(week).date())
        week_only = variant_frame[variant_frame["week_start"] == week].copy()
        _, week_summary = _simulate_flat(week_only)
        rows.append(
            {
                "report_section": "individual_week",
                "week_bucket": week_label,
                "removed_week": "",
                "variant": VARIANT_LABEL,
                **week_summary,
                "sample_warning": "ONE_MONTH_SMALL_SAMPLE" if int(week_summary["bets"]) < 100 else "",
            }
        )

        remaining = variant_frame[variant_frame["week_start"] != week].copy()
        _, remaining_summary = _simulate_flat(remaining)
        rows.append(
            {
                "report_section": "leave_one_week_out",
                "week_bucket": "remaining_weeks",
                "removed_week": week_label,
                "variant": VARIANT_LABEL,
                **remaining_summary,
                "sample_warning": "ONE_MONTH_SMALL_SAMPLE" if int(remaining_summary["bets"]) < 100 else "",
            }
        )

    report = pd.DataFrame(rows)
    save_dataframe(report, OUTPUT_PATH)

    loo_rows = report[report["report_section"] == "leave_one_week_out"].copy()
    survives = bool((loo_rows["roi"] > 0).all()) if not loo_rows.empty else False

    print("Late V2 Pocket Leave-One-Week-Out")
    print(report[report["report_section"] == "full_month"].to_string(index=False))
    print("\nIndividual Weeks")
    print(report[report["report_section"] == "individual_week"].to_string(index=False))
    print("\nLeave-One-Week-Out")
    print(loo_rows.to_string(index=False))
    print(f"\nVARIANT D SURVIVES LEAVE-ONE-WEEK-OUT VALIDATION: {survives}")
    return report


def main() -> None:
    run_leave_one_week_out()


if __name__ == "__main__":
    main()
