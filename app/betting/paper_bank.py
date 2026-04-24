from __future__ import annotations

from app.config import ACTIVE_DECISION_VERSION, PAPER_STARTING_BANK, PAPER_STAKE_PCT
from app.models import PaperBet, PaperBetArchive, PaperBankReset

STARTING_BANK = PAPER_STARTING_BANK
STAKE_PERCENT = PAPER_STAKE_PCT
KNOWN_DECISION_VERSIONS = ("value_v1", "model_edge_v1", "model_edge_v2")


def get_known_decision_versions(db) -> list[str]:
    versions = set(KNOWN_DECISION_VERSIONS)

    versions.update(
        version
        for (version,) in db.query(PaperBet.decision_version).distinct().all()
        if version
    )
    versions.update(
        version
        for (version,) in db.query(PaperBetArchive.decision_version).distinct().all()
        if version
    )
    versions.update(
        version
        for (version,) in db.query(PaperBankReset.decision_version).distinct().all()
        if version
    )
    versions.add(ACTIVE_DECISION_VERSION)

    return sorted(versions)


def get_latest_reset(db, decision_version: str | None = None) -> PaperBankReset | None:
    query = db.query(PaperBankReset)
    if decision_version is not None:
        query = query.filter(PaperBankReset.decision_version == decision_version)
    return query.order_by(PaperBankReset.id.desc()).first()


def get_strategy_starting_bank(db, decision_version: str) -> float:
    latest_reset = get_latest_reset(db, decision_version)
    if latest_reset and latest_reset.baseline_bank is not None:
        return float(latest_reset.baseline_bank)
    return float(STARTING_BANK)


def _strategy_bets_query(db, decision_version: str):
    return db.query(PaperBet).filter(PaperBet.decision_version == decision_version)


def get_strategy_bank(db, decision_version: str) -> float:
    settled_bets = _strategy_bets_query(db, decision_version).filter(
        PaperBet.profit_loss.isnot(None)
    ).all()
    total_profit_loss = sum((bet.profit_loss or 0.0) for bet in settled_bets)
    return round(get_strategy_starting_bank(db, decision_version) + total_profit_loss, 2)


def get_strategy_profit_loss(db, decision_version: str) -> float:
    return round(
        get_strategy_bank(db, decision_version) - get_strategy_starting_bank(db, decision_version),
        2,
    )


def get_strategy_roi(db, decision_version: str) -> float:
    settled_bets = _strategy_bets_query(db, decision_version).filter(
        PaperBet.profit_loss.isnot(None)
    ).all()
    total_staked = sum((bet.stake or 0.0) for bet in settled_bets)
    if total_staked <= 0:
        return 0.0

    total_profit_loss = sum((bet.profit_loss or 0.0) for bet in settled_bets)
    return round(total_profit_loss / total_staked, 4)


def get_strategy_next_stake(db, decision_version: str) -> float:
    return round(get_strategy_bank(db, decision_version) * STAKE_PERCENT, 2)


def get_all_strategy_bank_summary(db) -> list[dict[str, float | int | str]]:
    summary = []
    for decision_version in get_known_decision_versions(db):
        all_bets = _strategy_bets_query(db, decision_version).all()
        open_bets = [bet for bet in all_bets if not bet.settled_flag]
        settled_bets = [bet for bet in all_bets if bet.settled_flag and bet.profit_loss is not None]
        starting_bank = get_strategy_starting_bank(db, decision_version)
        current_bank = get_strategy_bank(db, decision_version)
        profit_loss = round(current_bank - starting_bank, 2)

        summary.append(
            {
                "decision_version": decision_version,
                "starting_bank": round(starting_bank, 2),
                "current_bank": round(current_bank, 2),
                "profit_loss": profit_loss,
                "roi": get_strategy_roi(db, decision_version),
                "open_bets": len(open_bets),
                "settled_bets": len(settled_bets),
                "total_bets": len(all_bets),
                "stake_exposure": round(sum((bet.stake or 0.0) for bet in open_bets), 2),
                "next_stake": get_strategy_next_stake(db, decision_version),
            }
        )

    return summary


def get_combined_bank(db) -> float:
    return round(sum(item["current_bank"] for item in get_all_strategy_bank_summary(db)), 2)


def get_current_bank(db) -> float:
    """Return the combined bank across all strategy versions."""
    return get_combined_bank(db)


def get_bank_since_reset(db) -> float:
    return round(
        sum(item["profit_loss"] for item in get_all_strategy_bank_summary(db)),
        2,
    )


def get_lifetime_bank(db) -> float:
    active_profit_loss = sum(
        (bet.profit_loss or 0.0)
        for bet in db.query(PaperBet).filter(PaperBet.profit_loss.isnot(None)).all()
    )
    archived_profit_loss = sum(
        (bet.profit_loss or 0.0)
        for bet in db.query(PaperBetArchive).filter(PaperBetArchive.profit_loss.isnot(None)).all()
    )
    version_count = len(get_known_decision_versions(db))
    return round((STARTING_BANK * version_count) + active_profit_loss + archived_profit_loss, 2)


def get_next_stake(db) -> float:
    """Backward-compatible helper that follows the active strategy version."""
    return get_strategy_next_stake(db, ACTIVE_DECISION_VERSION)


def get_total_roi(db) -> float:
    summaries = get_all_strategy_bank_summary(db)
    total_profit_loss = sum(item["profit_loss"] for item in summaries)
    total_staked = sum(
        (bet.stake or 0.0)
        for bet in db.query(PaperBet).filter(PaperBet.profit_loss.isnot(None)).all()
    )
    if total_staked <= 0:
        return 0.0
    return round(total_profit_loss / total_staked, 4)
