from __future__ import annotations

from collections import defaultdict

from app.betting.market_helpers import odds_bucket_label


def build_performance_stats(bets) -> dict[str, float | int]:
    total_bets = len(bets)
    wins = sum(1 for bet in bets if bet.result == "WIN")
    losses = sum(1 for bet in bets if bet.result == "LOSE")
    profit_loss = round(sum((bet.profit_loss or 0.0) for bet in bets), 2)
    total_staked = sum((bet.stake or 0.0) for bet in bets)
    roi = round(profit_loss / total_staked, 4) if total_staked > 0 else 0.0

    clv_values = [
        bet.closing_line_difference
        for bet in bets
        if getattr(bet, "closing_line_difference", None) is not None
    ]
    beat_clv_count = sum(1 for bet in bets if getattr(bet, "beat_closing_line", None) is True)
    beat_clv_rate = round(beat_clv_count / len(clv_values), 4) if clv_values else 0.0

    return {
        "total_bets": total_bets,
        "wins": wins,
        "losses": losses,
        "profit_loss": profit_loss,
        "roi": roi,
        "avg_clv_diff": round(sum(clv_values) / len(clv_values), 4) if clv_values else 0.0,
        "beat_clv_rate": beat_clv_rate,
        "clv_samples": len(clv_values),
    }


def build_version_breakdown(bets) -> dict[str, dict[str, float | int]]:
    version_map = defaultdict(list)
    for bet in bets:
        version_map[bet.decision_version or "unknown"].append(bet)

    return {
        version: build_performance_stats(version_bets)
        for version, version_bets in sorted(version_map.items())
    }


def build_odds_bucket_breakdown(bets) -> dict[str, dict[str, float | int]]:
    odds_map = defaultdict(list)
    for bet in bets:
        odds_map[odds_bucket_label(getattr(bet, "odds_taken", None))].append(bet)

    bucket_order = ["0-3", "3-5", "5-8", "8-12", "12-20", "20+", "unknown"]
    return {
        bucket: build_performance_stats(odds_map[bucket])
        for bucket in bucket_order
        if odds_map.get(bucket)
    }


def build_status_breakdown(open_bets, settled_bets) -> dict[str, dict[str, float | int]]:
    return {
        "open": {
            "total_bets": len(open_bets),
            "stake_exposure": round(sum((bet.stake or 0.0) for bet in open_bets), 2),
        },
        "settled": build_performance_stats(settled_bets),
    }


def build_label_breakdown(bets, labels_by_bet_id, limit: int | None = None) -> dict[str, dict[str, float | int]]:
    grouped = defaultdict(list)
    for bet in bets:
        label = labels_by_bet_id.get(getattr(bet, "id", None)) or "unknown"
        grouped[label].append(bet)

    ordered_items = sorted(
        grouped.items(),
        key=lambda item: (
            -len(item[1]),
            item[0],
        ),
    )
    if limit is not None:
        ordered_items = ordered_items[:limit]

    return {
        label: build_performance_stats(group_bets)
        for label, group_bets in ordered_items
    }
