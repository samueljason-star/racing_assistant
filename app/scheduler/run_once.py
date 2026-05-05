import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betfair.save_markets import save_markets
from app.betfair.save_odds import save_odds
from app.betting.settle_bets import settle_bets
from app.pipelines.compute_features import compute_features
from app.pipelines.update_results import update_results
from app.predictions.predict import predict_races
from app.racing_australia.load_horse_history import load_today_horse_history
from app.strategy.late_market_bets import create_late_market_bets
from app.strategy.late_market_v2_bets import create_late_market_v2_bets
from app.strategy.value_bets import create_value_bets

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")


def _timestamp() -> str:
    return datetime.now(BRISBANE_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _run_step(step_name: str, step_func) -> float:
    print(f"[{_timestamp()}] Starting step: {step_name}")
    started = time.perf_counter()
    try:
        step_func()
        duration = time.perf_counter() - started
        print(f"[{_timestamp()}] Finished step: {step_name} | duration={duration:.2f}s")
        return duration
    except Exception as exc:
        duration = time.perf_counter() - started
        print(f"[{_timestamp()}] Step failed: {step_name} | {exc}")
        print(f"[{_timestamp()}] Step duration before failure: {step_name} | duration={duration:.2f}s")
        traceback.print_exc()
        return duration


def _run_pipeline(label: str, steps: list[tuple[str, object]]) -> None:
    print(f"[{_timestamp()}] {label} START")
    pipeline_started = time.perf_counter()
    total_duration = 0.0
    for step_name, step_func in steps:
        total_duration += _run_step(step_name, step_func)
    pipeline_elapsed = time.perf_counter() - pipeline_started
    print(
        f"[{_timestamp()}] {label} FINISH duration={pipeline_elapsed:.2f}s "
        f"(sum_step_durations={total_duration:.2f}s)"
    )


def run_fast_pipeline_once() -> None:
    """Run the fast live betting pipeline once."""
    steps = [
        ("save_markets", save_markets),
        ("save_odds", save_odds),
        ("update_results", update_results),
        ("predict_races", predict_races),
        ("create_value_bets", create_value_bets),
        ("create_late_market_bets", create_late_market_bets),
        ("create_late_market_v2_bets", create_late_market_v2_bets),
        ("settle_bets", settle_bets),
    ]
    _run_pipeline("FAST PIPELINE", steps)


def run_slow_refresh_once() -> None:
    """Run the slow history/features refresh pipeline once."""
    steps = [
        ("load_today_horse_history", load_today_horse_history),
        ("compute_features", compute_features),
        ("predict_races", predict_races),
    ]
    _run_pipeline("SLOW REFRESH", steps)


def run_pipeline_once() -> None:
    """Backward-compatible alias for the fast live pipeline."""
    run_fast_pipeline_once()


if __name__ == "__main__":
    run_pipeline_once()
