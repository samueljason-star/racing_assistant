from app.pipelines.update_meetings import update_meetings
from app.pipelines.update_runners import update_runners
from app.pipelines.update_odds import update_odds
from app.pipelines.update_results import update_results
from app.pipelines.compute_features import compute_features
from app.predictions.predict import predict_races
from app.betting.decision_engine import create_paper_bets
from app.betting.settle_bets import settle_paper_bets
from app.reports.daily_summary import daily_summary


def run_full_update() -> None:
    update_meetings()
    update_runners()
    update_odds()
    compute_features()
    predict_races()
    create_paper_bets()
    update_results()
    settle_paper_bets()
    daily_summary()
