import sys
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
from app.strategy.value_bets import create_value_bets

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")


def _timestamp() -> str:
    return datetime.now(BRISBANE_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _run_step(step_name: str, step_func) -> None:
    print(f"[{_timestamp()}] Starting step: {step_name}")
    try:
        step_func()
        print(f"[{_timestamp()}] Finished step: {step_name}")
    except Exception as exc:
        print(f"[{_timestamp()}] Step failed: {step_name} | {exc}")
        traceback.print_exc()


def run_pipeline_once() -> None:
    """Run the live pipeline once in the configured production order."""
    print(f"[{_timestamp()}] Pipeline run started")

    steps = [
        ("save_markets", save_markets),
        ("save_odds", save_odds),
        ("update_results", update_results),
        ("load_today_horse_history", load_today_horse_history),
        ("compute_features", compute_features),
        ("predict_races", predict_races),
        ("create_value_bets", create_value_bets),
        ("settle_bets", settle_bets),
    ]

    for step_name, step_func in steps:
        _run_step(step_name, step_func)

    print(f"[{_timestamp()}] Pipeline run finished")


if __name__ == "__main__":
    run_pipeline_once()
