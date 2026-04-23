from __future__ import annotations

from pathlib import Path

import pandas as pd

FEATURE_COLUMNS = [
    "market_probability",
    "odds_rank",
    "barrier_score",
    "form_score",
    "trainer_score",
    "jockey_score",
    "distance_score",
    "track_score",
]

MODEL_VERSION = "logreg_v1"
MODEL_PATH = (
    Path(__file__).resolve().parent / "artifacts" / "logistic_regression.joblib"
)


def build_feature_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    for column in FEATURE_COLUMNS:
        if column not in frame:
            frame[column] = 0.0
    return frame[FEATURE_COLUMNS].fillna(0.0)
