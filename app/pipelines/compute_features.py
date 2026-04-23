from app.db import SessionLocal
from app.models import Runner, OddsSnapshot, Feature


def compute_market_probability(odds: float) -> float:
    if odds <= 0:
        return 0.0
    return 1 / odds


def compute_features() -> None:
    db = SessionLocal()
    try:
        runners = db.query(Runner).all()

        for runner in runners:
            latest_odds = db.query(OddsSnapshot).filter(
                OddsSnapshot.runner_id == runner.id,
            ).order_by(OddsSnapshot.timestamp.desc()).first()

            if not latest_odds:
                continue

            market_probability = compute_market_probability(latest_odds.odds)
            barrier_score = max(0, 10 - runner.barrier) if runner.barrier else 0
            form_score = 5.0
            trainer_score = 5.0
            jockey_score = 5.0
            distance_score = 5.0
            track_score = 5.0

            existing = db.query(Feature).filter(
                Feature.race_id == runner.race_id,
                Feature.runner_id == runner.id,
            ).first()

            if existing:
                existing.market_probability = market_probability
                existing.barrier_score = barrier_score
                existing.form_score = form_score
                existing.trainer_score = trainer_score
                existing.jockey_score = jockey_score
                existing.distance_score = distance_score
                existing.track_score = track_score
            else:
                feature = Feature(
                    race_id=runner.race_id,
                    runner_id=runner.id,
                    market_probability=market_probability,
                    barrier_score=barrier_score,
                    form_score=form_score,
                    trainer_score=trainer_score,
                    jockey_score=jockey_score,
                    distance_score=distance_score,
                    track_score=track_score,
                    odds_rank=0,
                    feature_version="v1",
                )
                db.add(feature)

        db.commit()
        print("Features computed.")
    finally:
        db.close()
