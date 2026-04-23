from collections import defaultdict

from app.db import SessionLocal
from app.models import Race, Runner, OddsSnapshot, PaperBet


STARTING_BANK = 10000.0
STAKE_PCT = 0.01


def get_latest_and_first_odds(db, runner_id):
    snaps = (
        db.query(OddsSnapshot)
        .filter(OddsSnapshot.runner_id == runner_id)
        .order_by(OddsSnapshot.timestamp.asc())
        .all()
    )

    if not snaps:
        return None, None

    first_odds = snaps[0].odds
    latest_odds = snaps[-1].odds
    return first_odds, latest_odds


def get_current_bank(db):
    bets = db.query(PaperBet).all()
    if not bets:
        return STARTING_BANK

    settled = [b for b in bets if b.profit_loss is not None]
    bank = STARTING_BANK + sum(b.profit_loss for b in settled)
    return round(bank, 2)


def create_value_bets():
    db = SessionLocal()

    races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
    current_bank = get_current_bank(db)
    stake = round(current_bank * STAKE_PCT, 2)

    created = 0

    for race in races:
        runners = db.query(Runner).filter(Runner.race_id == race.id).all()

        runner_data = []
        for runner in runners:
            first_odds, latest_odds = get_latest_and_first_odds(db, runner.id)

            if first_odds is None or latest_odds is None:
                continue

            runner_data.append({
                "runner": runner,
                "first_odds": first_odds,
                "latest_odds": latest_odds,
            })

        if len(runner_data) < 6:
            continue

        runner_data.sort(key=lambda x: x["latest_odds"])

        # assign ranks
        for i, item in enumerate(runner_data, start=1):
            item["rank"] = i

        favourite_odds = runner_data[0]["latest_odds"]

        if favourite_odds >= 2.80:
            continue

        chosen = None

        for item in runner_data:
            latest_odds = item["latest_odds"]
            first_odds = item["first_odds"]
            rank = item["rank"]

            if rank not in (2, 3, 4):
                continue

            if not (3.0 <= latest_odds <= 10.0):
                continue

            if first_odds <= 0:
                continue

            shortening_pct = ((first_odds - latest_odds) / first_odds) * 100

            if shortening_pct < 5:
                continue

            chosen = item
            break

        if not chosen:
            continue

        runner = chosen["runner"]
        latest_odds = chosen["latest_odds"]
        market_probability = 1 / latest_odds

        already_exists = (
            db.query(PaperBet)
            .filter(PaperBet.race_id == race.id)
            .first()
        )
        if already_exists:
            continue

        db.add(
            PaperBet(
                race_id=race.id,
                runner_id=runner.id,
                odds_taken=latest_odds,
                market_probability=market_probability,
                model_probability=market_probability,  # placeholder for now
                edge=0.0,
                stake=stake,
                decision_reason=(
                    f"Rank {chosen['rank']}, odds shortened from "
                    f"{chosen['first_odds']} to {latest_odds}"
                ),
                result=None,
                profit_loss=None,
                settled_flag=False,
                decision_version="value_v1",
            )
        )
        created += 1

    db.commit()
    db.close()

    print(f"PAPER BETS CREATED: {created}")