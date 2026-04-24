from __future__ import annotations

from collections import defaultdict

from app.betting.market_helpers import edge_bucket_label, odds_bucket_label


def _value(item, key, default=None):
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def build_performance_stats(bets) -> dict[str, float | int]:
    total_bets = len(bets)
    settled_bets = [bet for bet in bets if bool(_value(bet, "settled_flag", False))]
    open_bets = [bet for bet in bets if not bool(_value(bet, "settled_flag", False))]
    wins = sum(1 for bet in settled_bets if _value(bet, "result") == "WIN")
    losses = sum(1 for bet in settled_bets if _value(bet, "result") == "LOSE")
    profit_loss = round(sum((_value(bet, "profit_loss", 0.0) or 0.0) for bet in settled_bets), 2)
    total_staked = sum((_value(bet, "stake", 0.0) or 0.0) for bet in settled_bets)
    roi = round(profit_loss / total_staked, 4) if total_staked > 0 else 0.0
    strike_rate = round(wins / len(settled_bets), 4) if settled_bets else 0.0

    avg_odds = round(
        sum((_value(bet, "odds_taken", 0.0) or 0.0) for bet in bets) / total_bets,
        4,
    ) if total_bets else 0.0
    avg_edge = round(
        sum((_value(bet, "edge", 0.0) or 0.0) for bet in bets) / total_bets,
        4,
    ) if total_bets else 0.0
    clv_values = [
        _value(bet, "clv_percent")
        for bet in settled_bets
        if _value(bet, "clv_percent") is not None
    ]

    return {
        "total_bets": total_bets,
        "settled_bets": len(settled_bets),
        "open_bets": len(open_bets),
        "wins": wins,
        "losses": losses,
        "strike_rate": strike_rate,
        "profit_loss": profit_loss,
        "roi": roi,
        "avg_odds": avg_odds,
        "avg_edge": avg_edge,
        "avg_clv": round(sum(clv_values) / len(clv_values), 2) if clv_values else 0.0,
        "clv_samples": len(clv_values),
    }


def _group_and_build(bets, label_fn, ordered_labels: list[str] | None = None):
    grouped = defaultdict(list)
    for bet in bets:
        grouped[label_fn(bet)].append(bet)

    if ordered_labels is not None:
        return {
            label: build_performance_stats(grouped[label])
            for label in ordered_labels
            if grouped.get(label)
        }

    return {
        label: build_performance_stats(group_bets)
        for label, group_bets in sorted(grouped.items())
    }


def build_version_breakdown(bets) -> dict[str, dict[str, float | int]]:
    return _group_and_build(bets, lambda bet: _value(bet, "decision_version", "unknown") or "unknown")


def build_odds_bucket_breakdown(bets) -> dict[str, dict[str, float | int]]:
    bucket_order = ["0-3", "3-5", "5-8", "8-10", "10-15", "15+", "unknown"]
    return _group_and_build(
        bets,
        lambda bet: odds_bucket_label(_value(bet, "odds_taken")),
        ordered_labels=bucket_order,
    )


def build_edge_bucket_breakdown(bets) -> dict[str, dict[str, float | int]]:
    bucket_order = ["0-0.03", "0.03-0.06", "0.06-0.10", "0.10+", "unknown"]
    return _group_and_build(
        bets,
        lambda bet: edge_bucket_label(_value(bet, "edge")),
        ordered_labels=bucket_order,
    )


def build_status_breakdown(open_bets, settled_bets) -> dict[str, dict[str, float | int]]:
    return {
        "open": build_performance_stats(open_bets),
        "settled": build_performance_stats(settled_bets),
    }


def build_label_breakdown(bets, labels_by_bet_id, limit: int | None = None) -> dict[str, dict[str, float | int]]:
    grouped = defaultdict(list)
    for bet in bets:
        label = labels_by_bet_id.get(_value(bet, "id")) or "unknown"
        grouped[label].append(bet)

    ordered_items = sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
    if limit is not None:
        ordered_items = ordered_items[:limit]

    return {
        label: build_performance_stats(group_bets)
        for label, group_bets in ordered_items
    }
