from app.betting.market_helpers import calculate_edge, commission_adjusted_market_probability
from app.betting.paper_bank import get_latest_reset, get_next_stake
from app.config import ACTIVE_DECISION_VERSION, BETFAIR_COMMISSION_RATE, PAPER_MIN_EDGE
from app.db import SessionLocal, init_db
from app.models import Feature, OddsSnapshot, PaperBet, Prediction

EDGE_THRESHOLD = PAPER_MIN_EDGE
MIN_CONFIDENCE = 0.60


def create_paper_bets() -> None:
    init_db()
    db = SessionLocal()
    try:
        predictions = db.query(Prediction).all()
        latest_reset = get_latest_reset(db)

        for pred in predictions:
            feature = db.query(Feature).filter(
                Feature.race_id == pred.race_id,
                Feature.runner_id == pred.runner_id,
            ).first()

            latest_odds = (
                db.query(OddsSnapshot)
                .filter(
                    OddsSnapshot.race_id == pred.race_id,
                    OddsSnapshot.runner_id == pred.runner_id,
                )
                .order_by(OddsSnapshot.timestamp.desc())
                .first()
            )

            if not feature or not latest_odds:
                continue

            market_probability = commission_adjusted_market_probability(
                latest_odds.odds,
                BETFAIR_COMMISSION_RATE,
            )
            model_probability = pred.model_probability or 0
            edge = calculate_edge(model_probability, market_probability)
            if market_probability is None or edge is None:
                continue

            already_exists = db.query(PaperBet).filter(
                PaperBet.race_id == pred.race_id,
                PaperBet.runner_id == pred.runner_id,
            ).first()

            if edge >= EDGE_THRESHOLD and pred.confidence_score >= MIN_CONFIDENCE and not already_exists:
                bet = PaperBet(
                    race_id=pred.race_id,
                    runner_id=pred.runner_id,
                    odds_taken=latest_odds.odds,
                    market_probability=market_probability,
                    model_probability=model_probability,
                    edge=edge,
                    stake=get_next_stake(db),
                    commission_rate=BETFAIR_COMMISSION_RATE,
                    decision_reason=(
                        "Model edge over commission-adjusted market | "
                        f"market_adj={market_probability:.4f} | "
                        f"commission={BETFAIR_COMMISSION_RATE:.2%}"
                    ),
                    decision_version=ACTIVE_DECISION_VERSION,
                    paper_bank_reset_id=latest_reset.id if latest_reset else None,
                )
                db.add(bet)

        db.commit()
        print("Paper bets created.")
    finally:
        db.close()
