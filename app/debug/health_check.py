from __future__ import annotations

import io
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

import joblib

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db import SessionLocal
from app.models import Feature, Meeting, OddsSnapshot, PaperBet, Prediction, Race, Result, Runner
from app.predictions.model_io import MODEL_PATH


def print_header(title: str) -> None:
    print(f"\n=== {title} ===")


def check_database():
    """Check database connectivity and core table counts."""
    print_header("Database")
    counts = {}
    warnings = []
    ok = True

    db = SessionLocal()
    try:
        counts["Meeting"] = db.query(Meeting).count()
        counts["Race"] = db.query(Race).count()
        counts["Runner"] = db.query(Runner).count()
        counts["OddsSnapshot"] = db.query(OddsSnapshot).count()
        counts["Feature"] = db.query(Feature).count()
        counts["Prediction"] = db.query(Prediction).count()
        counts["Result"] = db.query(Result).count()
        counts["PaperBet"] = db.query(PaperBet).count()

        for name, count in counts.items():
            print(f"{name}: {count}")

        race_with_runners = (
            db.query(Race)
            .join(Runner, Runner.race_id == Race.id)
            .first()
        )
        if race_with_runners:
            print("Integrity: at least one race has runners")
        else:
            ok = False
            warnings.append("WARNING: No races with runners found")

        race_with_odds = (
            db.query(Race)
            .join(OddsSnapshot, OddsSnapshot.race_id == Race.id)
            .first()
        )
        if race_with_odds:
            print("Integrity: odds snapshots exist for at least one race")
        else:
            ok = False
            warnings.append("WARNING: No odds snapshots found for races")

        if counts["Feature"] > 0:
            print("Integrity: feature rows exist")
        else:
            ok = False
            warnings.append("WARNING: No feature rows found")

        if counts["Prediction"] > 0:
            print("Integrity: prediction rows exist")
        else:
            ok = False
            warnings.append("WARNING: No prediction rows found")
    except Exception as exc:
        ok = False
        warnings.append(f"WARNING: Database check failed: {exc}")
    finally:
        db.close()

    for warning in warnings:
        print(warning)

    return ok, counts, warnings


def check_model():
    """Check model artifact readiness and inspect metadata."""
    print_header("Model")
    ok = True
    details = {}

    try:
        print(f"Model artifact path: {MODEL_PATH}")
        if not MODEL_PATH.exists():
            raise FileNotFoundError("Model artifact file does not exist")

        payload = joblib.load(MODEL_PATH)
        model = payload.get("model")
        feature_columns = payload.get("feature_columns")
        details["model_type"] = type(model).__name__ if model is not None else None
        details["feature_columns"] = feature_columns

        print(f"Model type: {details['model_type']}")
        print(f"Feature columns: {details['feature_columns']}")
    except Exception as exc:
        ok = False
        print(f"MODEL CHECK FAILED: {exc}")

    return ok, details


def _parse_int_from_output(output: str, label: str):
    prefix = f"{label}:"
    for line in output.splitlines():
        if line.startswith(prefix):
            try:
                return int(line.split(":", 1)[1].strip())
            except ValueError:
                return None
    return None


def check_pipeline():
    """Run the pipeline-once hook if it exists and summarize results."""
    print_header("Pipeline")
    ok = True
    summary = {
        "markets_processed": None,
        "odds_snapshots_saved": None,
        "bets_created": None,
    }

    try:
        from app.scheduler.run_once import run_pipeline_once  # type: ignore
    except Exception as exc:
        ok = False
        print(f"PIPELINE CHECK FAILED: app.scheduler.run_once missing or invalid: {exc}")
        return ok, summary

    try:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            run_pipeline_once()
        output = buffer.getvalue()
        print(output.strip() or "Pipeline ran with no stdout output")

        summary["markets_processed"] = _parse_int_from_output(output, "MARKETS PROCESSED")
        summary["odds_snapshots_saved"] = _parse_int_from_output(output, "ODDS SNAPSHOTS SAVED")
        summary["bets_created"] = _parse_int_from_output(output, "PAPER BETS CREATED")

        print(f"Markets processed: {summary['markets_processed']}")
        print(f"Odds snapshots saved: {summary['odds_snapshots_saved']}")
        print(f"Bets created: {summary['bets_created']}")
    except Exception as exc:
        ok = False
        print(f"PIPELINE CHECK FAILED: {exc}")

    return ok, summary


def check_settlement():
    """Run bet settlement and summarize the outcome."""
    print_header("Settlement")
    ok = True
    summary = {"bets_settled": None, "wins": None, "losses": None}

    try:
        from app.betting.settle_bets import settle_bets

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            settle_bets()
        output = buffer.getvalue()
        print(output.strip() or "Settlement ran with no stdout output")

        summary["bets_settled"] = _parse_int_from_output(output, "BETS SETTLED")
        summary["wins"] = _parse_int_from_output(output, "WINS")
        summary["losses"] = _parse_int_from_output(output, "LOSSES")

        print(f"Bets settled: {summary['bets_settled']}")
        print(f"Wins: {summary['wins']}")
        print(f"Losses: {summary['losses']}")
    except Exception as exc:
        ok = False
        print(f"SETTLEMENT CHECK FAILED: {exc}")

    return ok, summary


def check_telegram():
    """Attempt a Telegram connectivity test if a sender is available."""
    print_header("Telegram")
    ok = True

    try:
        from app.notifier.telegram import send_telegram_message
    except Exception:
        send_telegram_message = None

    if send_telegram_message is None:
        ok = False
        print("TELEGRAM CHECK FAILED: Telegram sender not found at app.notifier.telegram.send_telegram_message")
        print("Expected test message: TEST: Racing assistant is connected")
        return ok

    try:
        sent = send_telegram_message("TEST: Racing assistant is connected")
        if not sent:
            raise RuntimeError("send_telegram_message returned False")
        print("Telegram test message sent successfully")
    except Exception as exc:
        ok = False
        print(f"TELEGRAM CHECK FAILED: {exc}")

    return ok


def main():
    database_ok, _, _ = check_database()
    model_ok, _ = check_model()
    pipeline_ok, _ = check_pipeline()
    _, _ = check_settlement()
    telegram_ok = check_telegram()

    print_header("Final Status")
    print(f"DATABASE {'OK' if database_ok else 'FAIL'}")
    print(f"MODEL {'OK' if model_ok else 'FAIL'}")
    print(f"PIPELINE {'OK' if pipeline_ok else 'FAIL'}")
    print(f"TELEGRAM {'OK' if telegram_ok else 'FAIL'}")


if __name__ == "__main__":
    main()
