from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.bet_details import enrich_paper_bets
from app.betting.paper_bank import (
    STARTING_BANK,
    get_bank_since_reset,
    get_current_bank,
    get_lifetime_bank,
    get_total_roi,
)
from app.db import SessionLocal, init_db
from app.models import Meeting, PaperBet, Race
from app.reports.calibration_utils import collect_calibration_rows, summarize_calibration
from app.reports.performance import (
    build_label_breakdown,
    build_odds_bucket_breakdown,
    build_performance_stats,
    build_status_breakdown,
    build_version_breakdown,
)

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")


def _today_iso() -> str:
    return datetime.now(BRISBANE_TZ).date().isoformat()


def _race_date_map(db, race_ids):
    if not race_ids:
        return {}

    races = db.query(Race).filter(Race.id.in_(sorted(set(race_ids)))).all()
    meeting_ids = [race.meeting_id for race in races if race.meeting_id is not None]
    meetings = db.query(Meeting).filter(Meeting.id.in_(sorted(set(meeting_ids)))).all()
    meeting_map = {meeting.id: meeting.date for meeting in meetings}

    return {race.id: meeting_map.get(race.meeting_id) for race in races}


def generate_daily_summary_text() -> str:
    """Return a Telegram-friendly summary of today's paper betting performance."""
    init_db()
    db = SessionLocal()

    try:
        today = _today_iso()
        current_bank = get_current_bank(db)
        bank_since_reset = get_bank_since_reset(db)
        lifetime_bank = get_lifetime_bank(db)
        total_roi = get_total_roi(db)

        all_bets = db.query(PaperBet).order_by(PaperBet.id.desc()).all()
        race_dates = _race_date_map(db, [bet.race_id for bet in all_bets])

        todays_bets = [bet for bet in all_bets if race_dates.get(bet.race_id) == today]
        settled_today = [
            bet for bet in todays_bets if bet.settled_flag and bet.profit_loss is not None
        ]
        open_today = [bet for bet in todays_bets if not bet.settled_flag]
        enriched_settled_today = enrich_paper_bets(db, settled_today)
        enriched_all_bets = enrich_paper_bets(db, all_bets)
        open_bets = sum(1 for bet in all_bets if not bet.settled_flag)
        overall_stats = build_performance_stats(settled_today)
        version_breakdown = build_version_breakdown(settled_today)
        odds_breakdown = build_odds_bucket_breakdown(settled_today)
        status_breakdown = build_status_breakdown(open_today, settled_today)
        track_labels = {
            bet["id"]: bet["track"] or "Unknown"
            for bet in enriched_all_bets
        }
        race_type_labels = {
            bet["id"]: bet["race_type"] or bet["meeting_type"] or "Unknown"
            for bet in enriched_all_bets
        }
        track_breakdown = build_label_breakdown(settled_today, track_labels, limit=5)
        race_type_breakdown = build_label_breakdown(settled_today, race_type_labels, limit=5)
        calibration = summarize_calibration(collect_calibration_rows(db))

        best_bet = max(
            enriched_settled_today,
            key=lambda bet: bet["profit_loss"] or 0.0,
            default=None,
        )
        worst_bet = min(
            enriched_settled_today,
            key=lambda bet: bet["profit_loss"] or 0.0,
            default=None,
        )

        lines = [
            f"Daily Summary | {today}",
            f"Starting Bank: ${STARTING_BANK:.2f}",
            f"Current Bank: ${current_bank:.2f}",
            f"Lifetime Bank: ${lifetime_bank:.2f}",
            f"Bank Since Reset: ${bank_since_reset:.2f}",
            f"Daily P/L: ${overall_stats['profit_loss']:.2f}",
            f"Total Bets Today: {len(todays_bets)}",
            f"Open Bets Total: {open_bets}",
            f"Total ROI: {total_roi:.2%}",
            (
                "Status Split: "
                f"open={status_breakdown['open']['total_bets']} | "
                f"settled={status_breakdown['settled']['total_bets']} | "
                f"open_exposure=${status_breakdown['open']['stake_exposure']:.2f}"
            ),
            (
                "Settled Today: "
                f"bets={overall_stats['total_bets']} | "
                f"wins={overall_stats['wins']} | "
                f"losses={overall_stats['losses']} | "
                f"P/L=${overall_stats['profit_loss']:.2f} | "
                f"ROI={overall_stats['roi']:.2%}"
            ),
        ]

        if overall_stats["clv_samples"]:
            lines.append(
                "CLV: "
                f"samples={overall_stats['clv_samples']} | "
                f"avg_diff={overall_stats['avg_clv_diff']:+.4f} | "
                f"beat_rate={overall_stats['beat_clv_rate']:.2%}"
            )

        if version_breakdown:
            lines.append("By Strategy Version:")
            for version, stats in version_breakdown.items():
                lines.append(
                    f"- {version}: bets={stats['total_bets']} | "
                    f"wins={stats['wins']} | losses={stats['losses']} | "
                    f"P/L=${stats['profit_loss']:.2f} | ROI={stats['roi']:.2%}"
                )

        if odds_breakdown:
            lines.append("By Odds Bucket:")
            for bucket, stats in odds_breakdown.items():
                lines.append(
                    f"- {bucket}: bets={stats['total_bets']} | "
                    f"wins={stats['wins']} | losses={stats['losses']} | "
                    f"P/L=${stats['profit_loss']:.2f} | ROI={stats['roi']:.2%}"
                )

        if track_breakdown:
            lines.append("Top Tracks:")
            for track, stats in track_breakdown.items():
                lines.append(
                    f"- {track}: bets={stats['total_bets']} | "
                    f"P/L=${stats['profit_loss']:.2f} | ROI={stats['roi']:.2%}"
                )

        if race_type_breakdown:
            lines.append("Top Race Types:")
            for race_type, stats in race_type_breakdown.items():
                lines.append(
                    f"- {race_type}: bets={stats['total_bets']} | "
                    f"P/L=${stats['profit_loss']:.2f} | ROI={stats['roi']:.2%}"
                )

        if calibration["brier_score"] is not None:
            lines.append(
                f"Calibration: Brier={calibration['brier_score']:.4f} | "
                f"Buckets={len(calibration['bucket_summaries'])}"
            )

        if best_bet:
            lines.append(
                "Best Bet: "
                f"{best_bet['horse_name']} | "
                f"race {best_bet['race_id']} | "
                f"odds {best_bet['odds_taken']:.2f} | "
                f"P/L ${best_bet['profit_loss']:.2f}"
            )
        else:
            lines.append("Best Bet: None")

        if worst_bet:
            lines.append(
                "Worst Bet: "
                f"{worst_bet['horse_name']} | "
                f"race {worst_bet['race_id']} | "
                f"odds {worst_bet['odds_taken']:.2f} | "
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
