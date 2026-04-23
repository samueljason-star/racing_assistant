from __future__ import annotations

from datetime import datetime, timezone
import sys
from pathlib import Path

import joblib
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import SessionLocal
from app.models import Feature, Result
from app.predictions.model_io import FEATURE_COLUMNS, MODEL_PATH, MODEL_VERSION, build_feature_frame


def _choose_calibration_folds(y: list[int]) -> int | None:
    positives = sum(y)
    negatives = len(y) - positives
    minority_class = min(positives, negatives)
    if minority_class < 2:
        return None
    return min(5, minority_class)


def _load_training_rows(db):
    rows = (
        db.query(Feature, Result)
        .join(
            Result,
            (Feature.race_id == Result.race_id)
            & (Feature.runner_id == Result.runner_id),
        )
        .filter(Result.finish_position.isnot(None))
        .all()
    )

    training_rows = []
    labels = []
    for feature, result in rows:
        training_rows.append(
            {
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
        labels.append(1 if result.finish_position == 1 else 0)

    return training_rows, labels


def train_model() -> None:
    db = SessionLocal()
    try:
        training_rows, labels = _load_training_rows(db)
    finally:
        db.close()

    if not training_rows:
        raise ValueError(
            "No labeled training rows found. Load historical results so features can be "
            "joined to results before training."
        )

    if len(set(labels)) < 2:
        raise ValueError(
            "Training data must contain both winners and non-winners for logistic regression."
        )

    X = build_feature_frame(training_rows)
    y = labels

    base_estimator = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "logreg",
                LogisticRegression(
                    max_iter=2000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )

    calibration_folds = _choose_calibration_folds(y)
    if calibration_folds:
        model = CalibratedClassifierCV(
            estimator=base_estimator,
            method="sigmoid",
            cv=calibration_folds,
        )
        calibration_method = "sigmoid"
    else:
        model = base_estimator
        calibration_method = "builtin"

    model.fit(X, y)
    train_probabilities = model.predict_proba(X)[:, 1]

    payload = {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "model_version": MODEL_VERSION,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "training_rows": len(training_rows),
        "positive_rows": int(sum(y)),
        "negative_rows": int(len(y) - sum(y)),
        "calibration_method": calibration_method,
        "train_brier_score": float(brier_score_loss(y, train_probabilities)),
    }

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, MODEL_PATH)

    print(
        f"Model trained: rows={payload['training_rows']} "
        f"winners={payload['positive_rows']} "
        f"non_winners={payload['negative_rows']} "
        f"brier={payload['train_brier_score']:.4f} "
        f"saved_to={MODEL_PATH}"
    )


if __name__ == "__main__":
    train_model()
