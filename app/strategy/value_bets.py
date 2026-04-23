from app.betting.paper_bank import get_current_bank, get_next_stake
from app.betting.bet_details import enrich_paper_bets
from app.db import SessionLocal
from app.models import Feature, OddsSnapshot, PaperBet, Prediction, Race, Runner
from app.notifier.telegram import send_telegram_message

MIN_FIELD_SIZE = 6
MIN_RUNNER_ODDS = 3.0
MAX_RUNNER_ODDS = 12.0
MIN_EDGE = 0.05


def race_already_has_bet(db, race_id):
    existing_bet = db.query(PaperBet).filter(PaperBet.race_id == race_id).first()
    return existing_bet is not None


def get_latest_odds(db, runner_id):
    snapshot = (
        db.query(OddsSnapshot)
        .filter(OddsSnapshot.runner_id == runner_id)
        .order_by(OddsSnapshot.timestamp.desc())
        .first()
    )
    if not snapshot or snapshot.odds is None or snapshot.odds <= 0:
        return None
    return snapshot.odds


def build_runner_signal(db, runner):
    latest_odds = get_latest_odds(db, runner.id)
    if latest_odds is None:
        return None

    prediction = db.query(Prediction).filter(
        Prediction.race_id == runner.race_id,
        Prediction.runner_id == runner.id,
    ).first()
    if not prediction or prediction.model_probability is None:
        return None

    feature = db.query(Feature).filter(
        Feature.race_id == runner.race_id,
        Feature.runner_id == runner.id,
    ).first()

    market_probability = 1 / latest_odds
    model_probability = prediction.model_probability
    edge = model_probability - market_probability

    return {
        "runner": runner,
        "feature": feature,
        "prediction": prediction,
        "latest_odds": latest_odds,
        "market_probability": market_probability,
        "model_probability": model_probability,
        "edge": edge,
    }


def get_race_candidates(db, race):
    runners = db.query(Runner).filter(Runner.race_id == race.id).all()
    if len(runners) < MIN_FIELD_SIZE:
        return None, []

    candidates = []
    for runner in runners:
        signal = build_runner_signal(db, runner)
        if not signal:
            continue

        if not (MIN_RUNNER_ODDS <= signal["latest_odds"] <= MAX_RUNNER_ODDS):
            continue

        if signal["edge"] < MIN_EDGE:
            continue

        candidates.append(signal)

    candidates.sort(key=lambda item: item["edge"], reverse=True)
    return len(runners), candidates


def create_paper_bet(db, race, chosen_runner, stake):
    decision_version = "model_edge_v1"
    paper_bet = PaperBet(
        race_id=race.id,
        runner_id=chosen_runner["runner"].id,
        odds_taken=chosen_runner["latest_odds"],
        market_probability=chosen_runner["market_probability"],
        model_probability=chosen_runner["model_probability"],
        edge=chosen_runner["edge"],
        stake=stake,
        decision_reason=(
            f"Model edge {chosen_runner['edge']:.4f} | "
            f"model={chosen_runner['model_probability']:.4f} | "
            f"market={chosen_runner['market_probability']:.4f}"
        ),
        result=None,
        profit_loss=None,
        settled_flag=False,
        decision_version=decision_version,
    )
    db.add(paper_bet)
    return paper_bet


def create_value_bets():
    db = SessionLocal()

    try:
        races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
        current_bank = get_current_bank(db)
        stake = get_next_stake(db)

        races_checked = 0
        bets_created = 0
        created_edges = []

        for race in races:
            races_checked += 1

            if race_already_has_bet(db, race.id):
                continue

            field_size, candidates = get_race_candidates(db, race)

            if field_size is None:
                continue

            if not candidates:
                continue

            chosen = candidates[0]
            paper_bet = create_paper_bet(db, race, chosen, stake)
            bets_created += 1
            created_edges.append(chosen["edge"])

            bet_detail = enrich_paper_bets(db, [paper_bet])[0]
            message = (
                "New Paper Bet\n"
                f"Horse: {bet_detail['horse_name']}\n"
                f"Track: {bet_detail['track'] or 'Unknown'}\n"
                f"Race Number: {bet_detail['race_number'] or 'Unknown'}\n"
                f"Race ID: {bet_detail['race_id']}\n"
                f"Odds Taken: {bet_detail['odds_taken']:.2f}\n"
                f"Stake: ${bet_detail['stake']:.2f}\n"
                f"Market Prob: {bet_detail['market_probability']:.4f}\n"
                f"Model Prob: {bet_detail['model_probability']:.4f}\n"
                f"Edge: {bet_detail['edge']:.4f}\n"
                f"Decision: {bet_detail['decision_reason']}\n"
                f"Version: {bet_detail['decision_version']}"
            )
            send_telegram_message(message)

            print(
                f"{bet_detail['track'] or 'Unknown'} R{bet_detail['race_number'] or '?'}: BET | "
                f"{chosen['runner'].horse_name} | "
                f"odds={chosen['latest_odds']:.2f} | "
                f"model={chosen['model_probability']:.4f} | "
                f"market={chosen['market_probability']:.4f} | "
                f"edge={chosen['edge']:.4f} | "
                f"stake=${stake:.2f}"
            )

        db.commit()

        avg_edge = sum(created_edges) / len(created_edges) if created_edges else 0.0
        print(f"RACES CHECKED: {races_checked}")
        print(f"PAPER BETS CREATED: {bets_created}")
        print(f"AVG EDGE OF CREATED BETS: {avg_edge:.4f}")
    finally:
        db.close()


if __name__ == "__main__":
    create_value_bets()
