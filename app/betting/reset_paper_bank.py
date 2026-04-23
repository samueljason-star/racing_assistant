from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betting.paper_bank import STARTING_BANK
from app.db import SessionLocal, init_db
from app.models import PaperBet, PaperBetArchive, PaperBankReset


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset the paper betting bank safely.")
    parser.add_argument(
        "--delete-existing",
        action="store_true",
        help="Delete current paper bets instead of archiving them first.",
    )
    parser.add_argument(
        "--baseline",
        type=float,
        default=STARTING_BANK,
        help="Baseline bank for the new paper betting cycle.",
    )
    parser.add_argument(
        "--note",
        default="v2 reset",
        help="Optional note stored with the reset marker.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    init_db()
    db = SessionLocal()
    try:
        existing_bets = db.query(PaperBet).order_by(PaperBet.id.asc()).all()
        archived_count = 0
        archived_profit_loss = round(
            sum((bet.profit_loss or 0.0) for bet in existing_bets),
            2,
        )

        reset_row = PaperBankReset(
            baseline_bank=args.baseline,
            archived_bet_count=len(existing_bets),
            archived_profit_loss=archived_profit_loss,
            note=args.note,
        )
        db.add(reset_row)
        db.flush()

        if args.delete_existing:
            for bet in existing_bets:
                db.delete(bet)
        else:
            for bet in existing_bets:
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
                        beat_closing_line=bet.beat_closing_line,
                        placed_at=bet.placed_at,
                        settled_at=bet.settled_at,
                        archived_reason=f"bank_reset:{reset_row.id}",
                    )
                )
                db.delete(bet)
                archived_count += 1

        db.commit()

        action = "deleted" if args.delete_existing else "archived"
        print("PAPER BANK RESET COMPLETE")
        print(f"Reset ID: {reset_row.id}")
        print(f"Reset Note: {reset_row.note}")
        print(f"New Baseline: ${args.baseline:.2f}")
        print(f"Existing Bets Found: {len(existing_bets)}")
        print(f"Existing Bets {action.title()}: {len(existing_bets) if args.delete_existing else archived_count}")
        print(f"Archived Profit/Loss Snapshot: ${archived_profit_loss:.2f}")
        print("Preserved Tables: races, runners, odds snapshots, results, features, predictions")
        print("Active paper_bets table is now empty and ready for a fresh cycle.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
