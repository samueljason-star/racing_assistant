from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.bet_details import enrich_paper_bets
from app.betting.paper_bank import (
    get_all_strategy_bank_summary,
    get_combined_bank,
    get_total_roi,
)
from app.config import ACTIVE_DECISION_VERSION
from app.db import SessionLocal, init_db
from app.models import PaperBet
from app.reports.performance import (
    build_edge_bucket_breakdown,
    build_odds_bucket_breakdown,
    build_performance_stats,
    build_version_breakdown,
)

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")


def _today_date():
    return datetime.now(BRISBANE_TZ).date()


def _to_brisbane(value):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(BRISBANE_TZ)


def _is_same_brisbane_day(value, target_date) -> bool:
    converted = _to_brisbane(value)
    return bool(converted and converted.date() == target_date)


def generate_daily_summary_text() -> str:
    init_db()
    db = SessionLocal()

    try:
        today = _today_date()
        strategy_summary = get_all_strategy_bank_summary(db)
        summary_by_version = {
            item["decision_version"]: item
            for item in strategy_summary
        }
        combined_bank = get_combined_bank(db)
        total_roi = get_total_roi(db)

        all_bets = db.query(PaperBet).order_by(PaperBet.id.desc()).all()
        placed_today_rows = [bet for bet in all_bets if _is_same_brisbane_day(bet.placed_at, today)]
        settled_today_rows = [
            bet for bet in all_bets
            if bool(bet.settled_flag) and _is_same_brisbane_day(getattr(bet, "settled_at", None), today)
        ]
        placed_today_bets = enrich_paper_bets(db, placed_today_rows)
        settled_today_bets = enrich_paper_bets(db, settled_today_rows)
        open_today_bets = [bet for bet in placed_today_bets if not bet["settled_flag"]]
        placed_today_stats = build_performance_stats(placed_today_bets)
        settled_today_stats = build_performance_stats(settled_today_bets)
        version_breakdown = build_version_breakdown(placed_today_bets)
        odds_breakdown = build_odds_bucket_breakdown(placed_today_bets)
        edge_breakdown = build_edge_bucket_breakdown(placed_today_bets)
        v2_summary = summary_by_version.get(ACTIVE_DECISION_VERSION)

        best_bet = max(
            settled_today_bets,
            key=lambda bet: bet["profit_loss"] or 0.0,
            default=None,
        )
        worst_bet = min(
            settled_today_bets,
            key=lambda bet: bet["profit_loss"] or 0.0,
            default=None,
        )

        lines = [
            f"Daily Summary | {today.isoformat()}",
            f"Combined Bank: ${combined_bank:.2f}",
            f"Combined ROI: {total_roi:.2%}",
            f"Daily P/L: ${settled_today_stats['profit_loss']:.2f}",
            f"Bets Today: {placed_today_stats['total_bets']}",
            f"Wins/Losses: {settled_today_stats['wins']}/{settled_today_stats['losses']}",
            f"Open Bets: {len(open_today_bets)}",
        ]

        if v2_summary:
            lines.append(
                "model_edge_v2: "
                f"start=${v2_summary['starting_bank']:.2f} | "
                f"bank=${v2_summary['current_bank']:.2f} | "
                f"P/L=${v2_summary['profit_loss']:.2f} | "
                f"ROI={v2_summary['roi']:.2%}"
            )

        lines.append("Strategy Banks:")
        for item in strategy_summary:
            lines.append(
                f"- {item['decision_version']}: "
                f"start=${item['starting_bank']:.2f} | "
                f"bank=${item['current_bank']:.2f} | "
                f"P/L=${item['profit_loss']:.2f} | "
                f"ROI={item['roi']:.2%} | "
                f"open={item['open_bets']} | settled={item['settled_bets']}"
            )

        if version_breakdown:
            lines.append("Today By Version:")
            for version, stats in version_breakdown.items():
                lines.append(
                    f"- {version}: bets={stats['total_bets']} | "
                    f"wins={stats['wins']} | losses={stats['losses']} | "
                    f"ROI={stats['roi']:.2%} | avg_clv={stats['avg_clv']:+.2f}%"
                )

        if odds_breakdown:
            lines.append("Today By Odds Bucket:")
            for bucket, stats in odds_breakdown.items():
                lines.append(
                    f"- {bucket}: bets={stats['total_bets']} | "
                    f"P/L=${stats['profit_loss']:.2f} | ROI={stats['roi']:.2%}"
                )

        if edge_breakdown:
            lines.append("Today By Edge Bucket:")
            for bucket, stats in edge_breakdown.items():
                lines.append(
                    f"- {bucket}: bets={stats['total_bets']} | "
                    f"P/L=${stats['profit_loss']:.2f} | ROI={stats['roi']:.2%}"
                )

        if settled_today_stats["clv_samples"]:
            lines.append(
                f"Average CLV: {settled_today_stats['avg_clv']:+.2f}% "
                f"across {settled_today_stats['clv_samples']} settled bets"
            )

        if best_bet:
            lines.append(
                f"Best Bet: {best_bet['horse_name']} | "
                f"{best_bet['decision_version']} | "
                f"P/L ${best_bet['profit_loss']:.2f}"
            )
        else:
            lines.append("Best Bet: None")

        if worst_bet:
            lines.append(
                f"Worst Bet: {worst_bet['horse_name']} | "
                f"{worst_bet['decision_version']} | "
                f"P/L ${worst_bet['profit_loss']:.2f}"
            )
        else:
            lines.append("Worst Bet: None")

        return "\n".join(lines)
    finally:
        db.close()


def print_daily_summary() -> None:
    print(generate_daily_summary_text())


if __name__ == "__main__":
    print_daily_summary()
