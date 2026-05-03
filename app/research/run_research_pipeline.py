from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from app.notifier.telegram import send_telegram_message
from app.research.backtest_engine import run_backtests
from app.research.form_score_optimizer import optimize_form_score
from app.research.import_betfair_history import import_betfair_history
from app.research.import_punting_form import import_punting_form
from app.research.market_pattern_analysis import analyze_market_patterns
from app.research.match_races import match_races
from app.research.testing_model import develop_testing_model
from app.research.strategy_optimizer import optimize_strategy
from app.research.utils import BETFAIR_HISTORY_INPUT_DIR, RESEARCH_ARTIFACTS_DIR, RESEARCH_DATA_DIR


def _section(title: str) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def _artifact_json(path: Path) -> dict[str, object]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _send_research_notification(text: str, *, enabled: bool) -> None:
    if not enabled:
        return
    sent = send_telegram_message(text)
    print(f"RESEARCH TELEGRAM SENT: {sent}")


def _build_completion_message(matched_frame: pd.DataFrame) -> str:
    strategy_payload = _artifact_json(RESEARCH_ARTIFACTS_DIR / "best_strategy_config.json")
    form_payload = _artifact_json(RESEARCH_ARTIFACTS_DIR / "best_form_score_config.json")
    safe_strategy = strategy_payload.get("recommended_safe_strategy", {})
    aggressive_strategy = strategy_payload.get("recommended_aggressive_strategy", {})

    lines = [
        "RESEARCH COMPLETE",
        f"Matched runner rows: {len(matched_frame)}",
    ]

    if form_payload:
        lines.append(
            "Best form config: "
            f"score={form_payload.get('score')} "
            f"roi={form_payload.get('roi')} "
            f"bets={form_payload.get('bets')}"
        )

    if safe_strategy:
        lines.append(
            "Safe strategy: "
            f"roi={safe_strategy.get('validation_roi')} "
            f"drawdown={safe_strategy.get('validation_drawdown')} "
            f"bets={safe_strategy.get('validation_bets')} "
            f"odds={safe_strategy.get('min_odds')}-{safe_strategy.get('max_odds')} "
            f"edge>={safe_strategy.get('min_edge')} "
            f"form>={safe_strategy.get('min_form_score')}"
        )

    if aggressive_strategy:
        lines.append(
            "Aggressive strategy: "
            f"roi={aggressive_strategy.get('validation_roi')} "
            f"drawdown={aggressive_strategy.get('validation_drawdown')} "
            f"bets={aggressive_strategy.get('validation_bets')} "
            f"odds={aggressive_strategy.get('min_odds')}-{aggressive_strategy.get('max_odds')} "
            f"edge>={aggressive_strategy.get('min_edge')} "
            f"form>={aggressive_strategy.get('min_form_score')}"
        )

    lines.append("No live strategy settings were changed automatically.")
    return "\n".join(lines)


def run_research_pipeline(*, notify_telegram: bool = False) -> None:
    _section("1. Import Punting Form")
    import_punting_form()

    _section("2. Import Betfair History")
    betfair_frame = import_betfair_history()

    _section("3. Match Datasets")
    matched_frame = match_races()

    if betfair_frame.empty:
        print()
        print("Research pipeline stopped early.")
        print(f"No Betfair history files were found under: {BETFAIR_HISTORY_INPUT_DIR}")
        print("Place CSV, JSON, or JSONL Betfair historical files there, then rerun the pipeline.")
        _send_research_notification(
            (
                "RESEARCH STOPPED EARLY\n"
                "No Betfair history rows were imported.\n"
                f"Input dir: {BETFAIR_HISTORY_INPUT_DIR}"
            ),
            enabled=notify_telegram,
        )
        return

    if matched_frame.empty:
        print()
        print("Research pipeline stopped early.")
        print(
            f"No matched runner rows were produced in: {RESEARCH_DATA_DIR / 'matched_runner_data.csv'}"
        )
        print("Check track/horse naming and Betfair history coverage, then rerun the pipeline.")
        _send_research_notification(
            (
                "RESEARCH STOPPED EARLY\n"
                "No matched runner rows were produced.\n"
                "Check Betfair coverage and track/horse matching."
            ),
            enabled=notify_telegram,
        )
        return

    _section("4. Optimise Form Score")
    optimize_form_score()

    _section("5. Analyse Market Patterns")
    analyze_market_patterns()

    _section("6. Develop Testing Model")
    develop_testing_model()

    _section("7. Run Backtests")
    run_backtests()

    _section("8. Optimise Strategy")
    optimize_strategy()

    _section("Research Complete")
    print("Recommended configurations were saved under app/research/artifacts/.")
    print("Nothing was applied to the live strategy automatically.")
    _send_research_notification(
        _build_completion_message(matched_frame),
        enabled=notify_telegram,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the offline racing research pipeline.")
    parser.add_argument(
        "--notify-telegram",
        action="store_true",
        help="Send a Telegram summary when the research pipeline stops early or completes.",
    )
    args = parser.parse_args()
    run_research_pipeline(notify_telegram=args.notify_telegram)


if __name__ == "__main__":
    main()
