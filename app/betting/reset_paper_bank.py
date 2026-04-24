from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.paper_bank import STARTING_BANK, get_known_decision_versions
from app.db import SessionLocal, init_db
from app.models import PaperBet, PaperBetArchive, PaperBankReset


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reset paper betting results safely.\n\n"
            "Examples:\n"
            "  python3 -m app.betting.reset_paper_bank --decision-version model_edge_v2\n"
            "  python3 -m app.betting.reset_paper_bank --all\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--decision-version",
        help="Reset only the selected decision_version, for example model_edge_v2.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Reset all decision versions.",
    )
    parser.add_argument(
        "--delete-existing",
        action="store_true",
        help="Delete selected paper bets instead of archiving them first.",
    )
    parser.add_argument(
        "--baseline",
        type=float,
        default=STARTING_BANK,
        help="New starting bank for the selected strategy version(s).",
    )
    parser.add_argument(
        "--note",
        default="paper bank reset",
        help="Optional note stored with the reset marker.",
    )
    return parser


def _archive_or_delete_bets(db, bets, delete_existing: bool, reset_row_id: int) -> int:
    affected = 0
    for bet in bets:
        if not delete_existing:
            db.add(
                PaperBetArchive(
                    original_paper_bet_id=bet.id,
                    race_id=bet.race_id,
                    runner_id=bet.runner_id,
                    odds_taken=bet.odds_taken,
                    market_probability=bet.market_probability,
                    model_probability=bet.model_probability,
                    edge=bet.edge,
                    stake=bet.stake,
                    commission_rate=bet.commission_rate or 0.08,
                    decision_reason=bet.decision_reason,
                    result=bet.result,
                    profit_loss=bet.profit_loss,
                    settled_flag=bet.settled_flag,
                    decision_version=bet.decision_version,
                    paper_bank_reset_id=bet.paper_bank_reset_id,
                    closing_odds=bet.closing_odds,
                    final_observed_odds=bet.final_observed_odds,
                    closing_line_difference=bet.closing_line_difference,
                    closing_line_pct=bet.closing_line_pct,
                    clv_percent=getattr(bet, "clv_percent", None),
                    beat_closing_line=bet.beat_closing_line,
                    placed_at=bet.placed_at,
                    settled_at=bet.settled_at,
                    archived_reason=f"bank_reset:{reset_row_id}",
                )
            )
        db.delete(bet)
        affected += 1
    return affected


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    init_db()
    db = SessionLocal()
    try:
        decision_versions = (
            get_known_decision_versions(db)
            if args.all
            else [args.decision_version]
        )

        total_cleared = 0
        for decision_version in decision_versions:
            existing_bets = (
                db.query(PaperBet)
                .filter(PaperBet.decision_version == decision_version)
                .order_by(PaperBet.id.asc())
                .all()
            )
            archived_profit_loss = round(
                sum((bet.profit_loss or 0.0) for bet in existing_bets),
                2,
            )

            reset_row = PaperBankReset(
                decision_version=decision_version,
                baseline_bank=args.baseline,
                archived_bet_count=len(existing_bets),
                archived_profit_loss=archived_profit_loss,
                note=args.note,
            )
            db.add(reset_row)
            db.flush()

            cleared = _archive_or_delete_bets(
                db,
                existing_bets,
                delete_existing=args.delete_existing,
                reset_row_id=reset_row.id,
            )
            total_cleared += cleared

            print(
                f"Decision version reset: {decision_version} | "
                f"PaperBet rows cleared: {cleared} | "
                f"New starting bank: ${args.baseline:.2f}"
            )

        db.commit()
        print(f"Total PaperBet rows cleared: {total_cleared}")
        print(
            "Usage examples: "
            "python3 -m app.betting.reset_paper_bank --decision-version model_edge_v2 | "
            "python3 -m app.betting.reset_paper_bank --all"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
