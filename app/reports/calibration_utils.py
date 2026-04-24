from __future__ import annotations

from collections import defaultdict

from app.betting.market_helpers import calculate_edge, commission_adjusted_market_probability, odds_bucket_label
from app.config import BETFAIR_COMMISSION_RATE
from app.models import Feature, OddsSnapshot, Prediction, Result


def _latest_odds_map(db) -> dict[tuple[int, int], float]:
    snapshots = db.query(OddsSnapshot).order_by(OddsSnapshot.timestamp.desc()).all()
    latest_by_runner = {}

    for snapshot in snapshots:
        key = (snapshot.race_id, snapshot.runner_id)
        if key not in latest_by_runner and snapshot.odds and snapshot.odds > 0:
            latest_by_runner[key] = snapshot.odds

    return latest_by_runner


def _feature_map(db) -> dict[tuple[int, int], Feature]:
    features = db.query(Feature).all()
    return {(feature.race_id, feature.runner_id): feature for feature in features}


def _result_map(db) -> dict[tuple[int, int], Result]:
    results = db.query(Result).all()
    return {(result.race_id, result.runner_id): result for result in results}


def collect_calibration_rows(db):
    latest_odds = _latest_odds_map(db)
    feature_map = _feature_map(db)
    result_map = _result_map(db)
    rows = []

    for prediction in db.query(Prediction).all():
        key = (prediction.race_id, prediction.runner_id)
        result = result_map.get(key)
        if not result or prediction.model_probability is None:
            continue

        feature = feature_map.get(key)
        market_odds = latest_odds.get(key)
        if market_odds is None and feature and feature.market_probability and feature.market_probability > 0:
            market_odds = 1.0 / feature.market_probability

        market_probability = None
        if market_odds is not None:
            market_probability = commission_adjusted_market_probability(
                market_odds,
                BETFAIR_COMMISSION_RATE,
            )
        elif feature:
            market_probability = feature.market_probability

        rows.append(
            {
                "race_id": prediction.race_id,
                "runner_id": prediction.runner_id,
                "odds": market_odds,
                "bucket": odds_bucket_label(market_odds),
                "predicted_probability": prediction.model_probability,
                "market_probability": market_probability,
                "actual_win": 1.0 if result.finish_position == 1 else 0.0,
                "edge": calculate_edge(prediction.model_probability, market_probability),
            }
        )

    return rows


def summarize_calibration(rows):
    bucket_rows = defaultdict(list)
    for row in rows:
        bucket_rows[row["bucket"]].append(row)

    bucket_order = ["0-3", "3-5", "5-8", "8-10", "10-15", "15+", "unknown"]
    summaries = []

    for bucket in bucket_order:
        values = bucket_rows.get(bucket)
        if not values:
            continue

        count = len(values)
        avg_pred = sum(row["predicted_probability"] for row in values) / count
        avg_actual = sum(row["actual_win"] for row in values) / count
        market_values = [row["market_probability"] for row in values if row["market_probability"] is not None]
        edge_values = [row["edge"] for row in values if row["edge"] is not None]
        summaries.append(
            {
                "bucket": bucket,
                "count": count,
                "avg_predicted_probability": avg_pred,
                "actual_win_rate": avg_actual,
                "avg_market_probability": (
                    sum(market_values) / len(market_values) if market_values else None
                ),
                "avg_edge": sum(edge_values) / len(edge_values) if edge_values else None,
                "brier_score": sum(
                    (row["predicted_probability"] - row["actual_win"]) ** 2
                    for row in values
                ) / count,
            }
        )

    brier_score = None
    if rows:
        brier_score = sum(
            (row["predicted_probability"] - row["actual_win"]) ** 2
            for row in rows
        ) / len(rows)

    return {
        "rows": rows,
        "bucket_summaries": summaries,
        "brier_score": brier_score,
    }
