from __future__ import annotations

import joblib
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import SessionLocal
from app.models import Feature, Prediction
from app.predictions.model_io import MODEL_PATH, build_feature_frame


def _load_model_payload():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Trained model not found at {MODEL_PATH}. Run train_model.py first."
        )
    return joblib.load(MODEL_PATH)


def predict_races() -> None:
    payload = _load_model_payload()
    model = payload["model"]
    model_version = payload["model_version"]

    db = SessionLocal()
    try:
        features = db.query(Feature).all()
        if not features:
            print("No feature rows found. Compute features before predicting.")
            return

        feature_rows = []
        for feature in features:
            feature_rows.append(
                {
                    "race_id": feature.race_id,
                    "runner_id": feature.runner_id,
                    "market_probability": feature.market_probability,
                    "odds_rank": feature.odds_rank,
                    "barrier_score": feature.barrier_score,
                    "form_score": feature.form_score,
                    "trainer_score": feature.trainer_score,
                    "jockey_score": feature.jockey_score,
                    "distance_score": feature.distance_score,
                    "track_score": feature.track_score,
                }
            )

        X = build_feature_frame(feature_rows)
        probabilities = model.predict_proba(X)[:, 1]

        race_groups = {}
        for row, probability in zip(feature_rows, probabilities, strict=True):
            row["model_probability"] = float(probability)
            race_groups.setdefault(row["race_id"], []).append(row)

        for race_id, rows in race_groups.items():
            rows.sort(key=lambda item: item["model_probability"], reverse=True)
            for rank, row in enumerate(rows, start=1):
                existing = db.query(Prediction).filter(
                    Prediction.race_id == row["race_id"],
                    Prediction.runner_id == row["runner_id"],
                ).first()
                confidence_score = max(
                    row["model_probability"],
                    1.0 - row["model_probability"],
                )

                if existing:
                    existing.model_probability = row["model_probability"]
                    existing.model_rank = rank
                    existing.confidence_score = confidence_score
                    existing.model_version = model_version
                else:
                    db.add(
                        Prediction(
                            race_id=row["race_id"],
                            runner_id=row["runner_id"],
                            model_probability=row["model_probability"],
                            model_rank=rank,
                            confidence_score=confidence_score,
                            model_version=model_version,
                        )
                    )

        db.commit()
        print(
            f"Predictions created with {model_version}. "
            f"scored_rows={len(feature_rows)}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    predict_races()
