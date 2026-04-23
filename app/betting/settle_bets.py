import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.bet_details import enrich_paper_bets
from app.betting.paper_bank import get_current_bank
from app.db import SessionLocal
from app.models import PaperBet, Result
from app.notifier.telegram import send_telegram_message


def settle_bets():
    db = SessionLocal()

    try:
        bets_settled = 0
        wins = 0
        losses = 0
        current_bank = get_current_bank(db)

        unsettled_bets = db.query(PaperBet).filter(PaperBet.settled_flag == False).all()

        for bet in unsettled_bets:
            result_row = db.query(Result).filter(
                Result.race_id == bet.race_id,
                Result.runner_id == bet.runner_id,
            ).first()

            if not result_row:
                continue

            if result_row.finish_position == 1:
                bet.profit_loss = bet.stake * (bet.odds_taken - 1)
                bet.result = "WIN"
                wins += 1
            else:
                bet.profit_loss = -bet.stake
                bet.result = "LOSE"
                losses += 1

            bet.settled_flag = True
            bets_settled += 1
            current_bank = round(current_bank + (bet.profit_loss or 0.0), 2)

            bet_detail = enrich_paper_bets(db, [bet])[0]
            message = (
                "Paper Bet Settled\n"
                f"Horse: {bet_detail['horse_name']}\n"
                f"Track: {bet_detail['track'] or 'Unknown'}\n"
                f"Race Number: {bet_detail['race_number'] or 'Unknown'}\n"
                f"Race ID: {bet_detail['race_id']}\n"
                f"Result: {bet_detail['result']}\n"
                f"Odds Taken: {bet_detail['odds_taken']:.2f}\n"
                f"Stake: ${bet_detail['stake']:.2f}\n"
                f"Profit/Loss: ${bet_detail['profit_loss']:.2f}\n"
                f"Current Bank: ${current_bank:.2f}"
            )
            send_telegram_message(message)

        db.commit()

        print(f"BETS SETTLED: {bets_settled}")
        print(f"WINS: {wins}")
        print(f"LOSSES: {losses}")
    finally:
        db.close()


if __name__ == "__main__":
    settle_bets()
