from app.models import PaperBet


STARTING_BANK = 10000.0
STAKE_PERCENT = 0.01


def get_current_bank(db) -> float:
    """Return the current paper bank after settled profit and loss."""
    settled_bets = db.query(PaperBet).filter(PaperBet.profit_loss.isnot(None)).all()
    profit_loss_total = sum(bet.profit_loss for bet in settled_bets)
    return round(STARTING_BANK + profit_loss_total, 2)


def get_next_stake(db) -> float:
    """Return the next stake using 1% of the current paper bank."""
    current_bank = get_current_bank(db)
    return round(current_bank * STAKE_PERCENT, 2)


def get_total_roi(db) -> float:
    """Return total ROI as settled profit/loss divided by total settled stake."""
    settled_bets = db.query(PaperBet).filter(PaperBet.profit_loss.isnot(None)).all()
    total_staked = sum(bet.stake for bet in settled_bets)
    if total_staked <= 0:
        return 0.0

    total_profit_loss = sum(bet.profit_loss for bet in settled_bets)
    return round(total_profit_loss / total_staked, 4)
