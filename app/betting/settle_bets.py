import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.bet_details import enrich_paper_bets
from app.betting.market_helpers import closing_line_metrics
from app.betting.paper_bank import get_strategy_bank
from app.db import SessionLocal, init_db
from app.models import OddsSnapshot, PaperBet, Result, Runner
from app.notifier.telegram import send_telegram_message


def _get_final_observed_odds(db, bet: PaperBet):
    latest_snapshot = (
        db.query(OddsSnapshot)
        .filter(
            OddsSnapshot.race_id == bet.race_id,
            OddsSnapshot.runner_id == bet.runner_id,
        )
        .order_by(OddsSnapshot.timestamp.desc())
        .first()
    )
    if not latest_snapshot or latest_snapshot.odds is None or latest_snapshot.odds <= 0:
        return None
    return latest_snapshot.odds


def _is_placeholder_result_set(db, race_id: int) -> bool:
    result_rows = db.query(Result).filter(
        Result.race_id == race_id,
        Result.finish_position.isnot(None),
    ).all()
    runner_count = db.query(Runner).filter(Runner.race_id == race_id).count()
    if runner_count <= 0 or len(result_rows) != runner_count:
        return False

    ordered = sorted(result_rows, key=lambda item: item.finish_position or 0)
    if [row.finish_position for row in ordered] != list(range(1, runner_count + 1)):
        return False

    for index, row in enumerate(ordered, start=1):
        expected_margin = 0.0 if index == 1 else round((index - 1) * 0.5, 2)
        if row.starting_price is not None:
            return False
        if row.margin != expected_margin:
            return False

    return True


def settle_bets():
    init_db()
    db = SessionLocal()

    try:
        bets_settled = 0
        wins = 0
        losses = 0
        bets_skipped_no_real_result = 0

        unsettled_bets = db.query(PaperBet).filter(PaperBet.settled_flag == False).all()

        for bet in unsettled_bets:
            result_row = db.query(Result).filter(
                Result.race_id == bet.race_id,
                Result.runner_id == bet.runner_id,
            ).first()

            if not result_row or result_row.finish_position is None:
                bets_skipped_no_real_result += 1
                continue

            if _is_placeholder_result_set(db, bet.race_id):
                bets_skipped_no_real_result += 1
                continue

            if result_row.finish_position == 1:
                bet.profit_loss = bet.stake * (bet.odds_taken - 1)
                bet.result = "WIN"
                wins += 1
            else:
                bet.profit_loss = -bet.stake
                bet.result = "LOSE"
                losses += 1

            final_observed_odds = _get_final_observed_odds(db, bet)
            clv = closing_line_metrics(bet.odds_taken, final_observed_odds)
            bet.closing_odds = final_observed_odds
            bet.final_observed_odds = final_observed_odds
            bet.closing_line_difference = clv["closing_line_difference"]
            bet.closing_line_pct = clv["closing_line_pct"]
            bet.clv_percent = clv["clv_percent"]
            bet.beat_closing_line = clv["beat_closing_line"]
            bet.settled_flag = True
            bet.settled_at = datetime.utcnow()
            bets_settled += 1

            bet_detail = enrich_paper_bets(db, [bet])[0]
            strategy_bank = get_strategy_bank(db, bet.decision_version or "unknown")
            message_lines = [
                "BET SETTLED",
                f"Horse: {bet_detail['horse_name']}",
                f"Track: {bet_detail['track'] or 'Unknown'}",
                f"Race Number: {bet_detail['race_number'] or 'Unknown'}",
                f"Race ID: {bet_detail['race_id']}",
                f"Result: {bet_detail['result']}",
                f"Finish Position: {result_row.finish_position}",
                f"Odds Taken: {bet_detail['odds_taken']:.2f}",
            ]
            if bet_detail["final_observed_odds"] is not None:
                message_lines.append(
                    f"Final Odds: {bet_detail['final_observed_odds']:.2f}"
                )
            if bet_detail["clv_percent"] is not None:
                message_lines.append(
                    f"CLV Percent: {bet_detail['clv_percent']:+.2f}%"
                )
            message_lines.extend(
                [
                    f"Stake: ${bet_detail['stake']:.2f}",
                    f"Profit/Loss: ${bet_detail['profit_loss']:.2f}",
                    f"Strategy Bank: ${strategy_bank:.2f}",
                ]
            )
            if send_telegram_message("\n".join(message_lines)):
                bet.settlement_notified_at = datetime.utcnow()

        db.commit()

        print(f"BETS SKIPPED DUE TO NO REAL RESULT: {bets_skipped_no_real_result}")
        print(f"BETS SETTLED: {bets_settled}")
        print(f"WINS: {wins}")
        print(f"LOSSES: {losses}")
    finally:
        db.close()


if __name__ == "__main__":
    settle_bets()
