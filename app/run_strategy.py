import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.betfair.save_markets import save_markets
from app.betfair.save_odds import save_odds
from app.pipelines.compute_features import compute_features
from app.pipelines.update_results import update_results
from app.predictions.predict import predict_races
from app.strategy.late_market_bets import create_late_market_bets
from app.strategy.value_bets import create_value_bets


def main():
    save_markets()
    save_odds()
    print("Running feature computation...")
    compute_features()
    print("Running predictions...")
    predict_races()
    print("Running results update...")
    update_results()
    create_value_bets()
    create_late_market_bets()


if __name__ == "__main__":
    main()
