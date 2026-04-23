from __future__ import annotations


DEFAULT_ODDS_BUCKETS = (
    (0.0, 3.0, "0-3"),
    (3.0, 5.0, "3-5"),
    (5.0, 8.0, "5-8"),
    (8.0, 12.0, "8-12"),
    (12.0, 20.0, "12-20"),
    (20.0, None, "20+"),
)


def commission_adjusted_market_probability(odds: float, commission_rate: float) -> float | None:
    """Return the back-bet break-even probability after exchange commission."""
    if odds is None or odds <= 1:
        return None

    bounded_commission = min(max(commission_rate or 0.0, 0.0), 0.99)
    net_profit_multiple = (odds - 1.0) * (1.0 - bounded_commission)
    return 1.0 / (1.0 + net_profit_multiple)


def calculate_edge(model_probability: float, market_probability: float | None) -> float | None:
    if model_probability is None or market_probability is None:
        return None
    return model_probability - market_probability


def odds_bucket_label(odds: float | None) -> str:
    if odds is None or odds <= 0:
        return "unknown"

    for lower, upper, label in DEFAULT_ODDS_BUCKETS:
        if upper is None and odds >= lower:
            return label
        if lower <= odds < upper:
            return label

    return "unknown"


def closing_line_metrics(odds_taken: float | None, closing_odds: float | None) -> dict[str, float | bool | None]:
    if (
        odds_taken is None
        or closing_odds is None
        or odds_taken <= 0
        or closing_odds <= 0
    ):
        return {
            "closing_odds": closing_odds,
            "closing_line_difference": None,
            "closing_line_pct": None,
            "beat_closing_line": None,
        }

    difference = round(closing_odds - odds_taken, 4)
    improvement_pct = round((odds_taken - closing_odds) / odds_taken, 4)
    return {
        "closing_odds": closing_odds,
        "closing_line_difference": difference,
        "closing_line_pct": improvement_pct,
        "beat_closing_line": closing_odds < odds_taken,
    }
