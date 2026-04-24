from app.betting.bet_details import enrich_paper_bets
from app.betting.market_helpers import calculate_edge, commission_adjusted_market_probability, raw_market_probability
from app.betting.paper_bank import get_latest_reset, get_strategy_bank, get_strategy_next_stake
from app.config import (
    ACTIVE_DECISION_VERSION,
    BETFAIR_COMMISSION_RATE,
    PAPER_MAX_MODEL_PROBABILITY,
    PAPER_MAX_ODDS,
    PAPER_MIN_EDGE,
    PAPER_MIN_ODDS,
)
from app.db import SessionLocal, init_db
from app.models import Feature, OddsSnapshot, PaperBet, Prediction, Race, Runner
from app.notifier.telegram import send_telegram_message

MIN_FIELD_SIZE = 6
MIN_RUNNER_ODDS = PAPER_MIN_ODDS
MAX_RUNNER_ODDS = PAPER_MAX_ODDS
MIN_EDGE = PAPER_MIN_EDGE
MAX_MODEL_PROBABILITY = PAPER_MAX_MODEL_PROBABILITY
COMMISSION_RATE = BETFAIR_COMMISSION_RATE
DECISION_VERSION = "model_edge_v2"


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
        return None, "missing_odds"

    prediction = db.query(Prediction).filter(
        Prediction.race_id == runner.race_id,
        Prediction.runner_id == runner.id,
    ).first()
    if not prediction or prediction.model_probability is None:
        return None, "missing_prediction"

    if prediction.model_probability > MAX_MODEL_PROBABILITY:
        return None, "model_probability_cap"

    feature = db.query(Feature).filter(
        Feature.race_id == runner.race_id,
        Feature.runner_id == runner.id,
    ).first()

    raw_probability = raw_market_probability(latest_odds)
    market_probability = commission_adjusted_market_probability(
        latest_odds,
        COMMISSION_RATE,
    )
    model_probability = prediction.model_probability
    edge = calculate_edge(model_probability, market_probability)

    if market_probability is None or edge is None:
        return None, "invalid_market_probability"

    return {
        "runner": runner,
        "feature": feature,
        "prediction": prediction,
        "latest_odds": latest_odds,
        "raw_market_probability": raw_probability,
        "market_probability": market_probability,
        "model_probability": model_probability,
        "edge": edge,
    }, None


def get_race_candidates(db, race, counters, all_candidates):
    runners = db.query(Runner).filter(Runner.race_id == race.id).all()
    if len(runners) < MIN_FIELD_SIZE:
        counters["races_skipped_field_size"] += 1
        return None, []

    candidates = []
    for runner in runners:
        signal, skip_reason = build_runner_signal(db, runner)
        if not signal:
            if skip_reason == "missing_prediction":
                counters["runners_skipped_missing_prediction"] += 1
            elif skip_reason == "missing_odds":
                counters["runners_skipped_missing_odds"] += 1
            elif skip_reason == "model_probability_cap":
                counters["runners_skipped_model_probability_cap"] += 1
            elif skip_reason == "invalid_market_probability":
                counters["runners_skipped_invalid_market_probability"] += 1
            continue

        all_candidates.append(signal)

        if not (MIN_RUNNER_ODDS <= signal["latest_odds"] <= MAX_RUNNER_ODDS):
            counters["runners_skipped_odds_band"] += 1
            continue

        if signal["edge"] < MIN_EDGE:
            counters["runners_skipped_edge_threshold"] += 1
            continue

        candidates.append(signal)

    candidates.sort(key=lambda item: item["edge"], reverse=True)
    return len(runners), candidates


def create_paper_bet(db, race, chosen_runner, stake):
    latest_reset = get_latest_reset(db, DECISION_VERSION)
    paper_bet = PaperBet(
        race_id=race.id,
        runner_id=chosen_runner["runner"].id,
        odds_taken=chosen_runner["latest_odds"],
        market_probability=chosen_runner["market_probability"],
        model_probability=chosen_runner["model_probability"],
        edge=chosen_runner["edge"],
        stake=stake,
        commission_rate=COMMISSION_RATE,
        decision_reason=(
            f"odds_taken={chosen_runner['latest_odds']:.2f} | "
            f"model_probability={chosen_runner['model_probability']:.4f} | "
            f"market_probability_adj={chosen_runner['market_probability']:.4f} | "
            f"edge={chosen_runner['edge']:.4f} | "
            f"strategy_version={DECISION_VERSION}"
        ),
        result=None,
        profit_loss=None,
        settled_flag=False,
        decision_version=DECISION_VERSION,
        paper_bank_reset_id=latest_reset.id if latest_reset else None,
    )
    db.add(paper_bet)
    return paper_bet


def create_value_bets():
    init_db()
    db = SessionLocal()

    try:
        races = db.query(Race).filter(Race.betfair_market_id.isnot(None)).all()
        current_bank = get_strategy_bank(db, DECISION_VERSION)

        races_checked = 0
        bets_created = 0
        created_edges = []
        counters = {
            "races_skipped_field_size": 0,
            "runners_skipped_missing_prediction": 0,
            "runners_skipped_missing_odds": 0,
            "runners_skipped_invalid_market_probability": 0,
            "runners_skipped_model_probability_cap": 0,
            "runners_skipped_odds_band": 0,
            "runners_skipped_edge_threshold": 0,
        }
        all_candidates = []

        for race in races:
            races_checked += 1

            if race_already_has_bet(db, race.id):
                continue

            field_size, candidates = get_race_candidates(db, race, counters, all_candidates)

            if field_size is None or not candidates:
                continue

            chosen = candidates[0]
            stake = get_strategy_next_stake(db, DECISION_VERSION)
            paper_bet = create_paper_bet(db, race, chosen, stake)
            bets_created += 1
            created_edges.append(chosen["edge"])

            bet_detail = enrich_paper_bets(db, [paper_bet])[0]
            strategy_bank = get_strategy_bank(db, DECISION_VERSION)
            message = (
                "New Paper Bet\n"
                f"Horse: {bet_detail['horse_name']}\n"
                f"Track: {bet_detail['track'] or 'Unknown'}\n"
                f"Race ID: {bet_detail['race_id']}\n"
                f"Odds Taken: {bet_detail['odds_taken']:.2f}\n"
                f"Stake: ${bet_detail['stake']:.2f}\n"
                f"Model Prob: {bet_detail['model_probability']:.4f}\n"
                f"Adj Market Prob: {bet_detail['market_probability']:.4f}\n"
                f"Edge: {bet_detail['edge']:.4f}\n"
                f"Version: {bet_detail['decision_version']}\n"
                f"Strategy Bank: ${strategy_bank:.2f}"
            )
            send_telegram_message(message)

            print(
                f"{bet_detail['track'] or 'Unknown'} R{bet_detail['race_number'] or '?'}: BET | "
                f"{chosen['runner'].horse_name} | "
                f"odds={chosen['latest_odds']:.2f} | "
                f"model={chosen['model_probability']:.4f} | "
                f"market_raw={chosen['raw_market_probability']:.4f} | "
                f"market_adj={chosen['market_probability']:.4f} | "
                f"edge={chosen['edge']:.4f} | "
                f"stake=${stake:.2f}"
            )

        db.commit()

        avg_edge = sum(created_edges) / len(created_edges) if created_edges else 0.0
        print(f"STRATEGY BANK {DECISION_VERSION}: ${current_bank:.2f}")
        print(f"RACES CHECKED: {races_checked}")
        print(f"PAPER BETS CREATED: {bets_created}")
        print(f"AVG EDGE OF CREATED BETS: {avg_edge:.4f}")
        print(
            f"RUNNERS SKIPPED DUE TO MISSING PREDICTION: "
            f"{counters['runners_skipped_missing_prediction']}"
        )
        print(
            f"RUNNERS SKIPPED DUE TO ODDS BAND: "
            f"{counters['runners_skipped_odds_band']}"
        )
        print(
            f"RUNNERS SKIPPED DUE TO EDGE THRESHOLD: "
            f"{counters['runners_skipped_edge_threshold']}"
        )
        print(
            "RUNNERS SKIPPED DUE TO MODEL PROBABILITY CAP: "
            f"{counters['runners_skipped_model_probability_cap']}"
        )
        print(
            "RUNNERS SKIPPED DUE TO INVALID ADJUSTED MARKET PROBABILITY: "
            f"{counters['runners_skipped_invalid_market_probability']}"
        )
        print(
            f"RUNNERS SKIPPED DUE TO MISSING ODDS: "
            f"{counters['runners_skipped_missing_odds']}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    create_value_bets()
