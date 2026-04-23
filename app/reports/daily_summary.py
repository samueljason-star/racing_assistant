from __future__ import annotations

from datetime import datetime

from app.betting.bet_details import enrich_paper_bets
from app.betting.paper_bank import STARTING_BANK, get_current_bank, get_total_roi
from app.db import SessionLocal
from app.models import PaperBet


def _get_today_date():
    return datetime.now().date()


def _supports_created_at(bets):
    return any(hasattr(bet, "created_at") and getattr(bet, "created_at") is not None for bet in bets)


def _is_today(bet):
    created_at = getattr(bet, "created_at", None)
    if created_at is None:
        return False
    return created_at.date() == _get_today_date()


def generate_daily_summary_text() -> str:
    db = SessionLocal()

    try:
        today = _get_today_date()
        current_bank = get_current_bank(db)
        total_roi = get_total_roi(db)

        all_bets = db.query(PaperBet).all()
        if _supports_created_at(all_bets):
            todays_bets = [bet for bet in all_bets if _is_today(bet)]
            scope_label = "today"
        else:
            todays_bets = all_bets
            scope_label = "current dataset"
        settled_today = [bet for bet in todays_bets if bet.settled_flag and bet.profit_loss is not None]
        enriched_settled_today = enrich_paper_bets(db, settled_today)

        daily_profit_loss = round(sum(bet.profit_loss for bet in settled_today), 2)
        wins_today = sum(1 for bet in settled_today if bet.result == "WIN")
        losses_today = sum(1 for bet in settled_today if bet.result == "LOSE")
        open_bets = sum(1 for bet in all_bets if not bet.settled_flag)

        best_bet = max(enriched_settled_today, key=lambda bet: bet["profit_loss"], default=None)
        worst_bet = min(enriched_settled_today, key=lambda bet: bet["profit_loss"], default=None)

        lines = [
            f"Daily Paper Betting Summary ({today.isoformat()})",
            f"Starting bank: ${STARTING_BANK:.2f}",
            f"Current bank: ${current_bank:.2f}",
            f"Daily profit/loss: ${daily_profit_loss:.2f}",
            f"Total bets {scope_label}: {len(todays_bets)}",
            f"Wins {scope_label}: {wins_today}",
            f"Losses {scope_label}: {losses_today}",
            f"Open bets: {open_bets}",
            f"Total ROI: {total_roi:.2%}",
        ]

        if best_bet:
            lines.append(
                f"Best settled bet {scope_label}: "
                f"{best_bet['horse_name']} | race {best_bet['race_id']} | "
                f"odds {best_bet['odds_taken']:.2f} | "
                f"P/L ${best_bet['profit_loss']:.2f}"
            )
        else:
            lines.append(f"Best settled bet {scope_label}: None")

        if worst_bet:
            lines.append(
                f"Worst settled bet {scope_label}: "
                f"{worst_bet['horse_name']} | race {worst_bet['race_id']} | "
                f"odds {worst_bet['odds_taken']:.2f} | "
                f"P/L ${worst_bet['profit_loss']:.2f}"
            )
        else:
            lines.append(f"Worst settled bet {scope_label}: None")

        return "\n".join(lines)
    finally:
        db.close()


def print_daily_summary():
    print(generate_daily_summary_text())


if __name__ == "__main__":
    print_daily_summary()
