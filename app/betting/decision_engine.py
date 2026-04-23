from app.db import SessionLocal
from app.models import Prediction, Feature, OddsSnapshot, PaperBet

EDGE_THRESHOLD = 0.05
MIN_CONFIDENCE = 0.60
FIXED_STAKE = 100.0


def create_paper_bets() -> None:
    db = SessionLocal()
    try:
        predictions = db.query(Prediction).all()

        for pred in predictions:
            feature = db.query(Feature).filter(
                Feature.race_id == pred.race_id,
                Feature.runner_id == pred.runner_id,
            ).first()

            latest_odds = db.query(OddsSnapshot).filter(
                OddsSnapshot.race_id == pred.race_id,
                OddsSnapshot.runner_id == pred.runner_id,
            ).order_by(OddsSnapshot.timestamp.desc()).first()

            if not feature or not latest_odds:
                continue

            market_probability = feature.market_probability or 0
            model_probability = pred.model_probability or 0
            edge = model_probability - market_probability

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
                    stake=FIXED_STAKE,
                    decision_reason="Model edge over market",
                    decision_version="v1",
                )
                db.add(bet)

        db.commit()
        print("Paper bets created.")
    finally:
        db.close()
