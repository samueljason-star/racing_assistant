# Research Module

This module is for offline research and backtesting only.

It does not write to the live production database by default.
It reads historical files and writes research outputs under:

- `data/research/`
- `data/research/reports/`
- `app/research/artifacts/`

## Input folders

Place Punting Form files in:

- `data/punting_form/`
- or use the raw API downloader output in:
- `data/raw/punting_form/`

Place Betfair historical files in:

- `data/betfair_history/`

## Expected Punting Form files

The importer is defensive and configurable-by-column-name, so it does not rely on one exact CSV layout.
It can also read the raw Punting Form API dump produced by the downloader under `data/raw/punting_form/`.

Useful Punting Form files include historical CSV exports that contain runner-level race data such as:

- race date
- track
- race number
- horse name
- barrier
- jockey
- trainer
- weight
- distance
- class
- track condition
- finish position
- margin
- starting price
- recent form columns if available

## Expected Betfair history files

Supported input formats:

- CSV
- JSON
- JSONL
- Betfair historical `.bz2` market stream files extracted from archive downloads

Useful fields include:

- race date
- track or venue
- race number if available
- market id
- selection id
- horse name
- timestamp
- traded price or best back price
- best lay price if available
- total matched if available
- market start time so `minutes_to_jump` can be derived

## How to run the full research pipeline

```bash
cd /Users/sam/Desktop/racing_assistant
python3 -m app.research.run_research_pipeline
```

To send a Telegram summary when the run stops early or completes:

```bash
cd /Users/sam/Desktop/racing_assistant
python3 -m app.research.run_research_pipeline --notify-telegram
```

For a long VM run that should survive disconnects:

```bash
cd /home/samueljasonhines/racing_assistant
source venv/bin/activate
mkdir -p logs
nohup python3 -u -m app.research.run_research_pipeline --notify-telegram > logs/research_pipeline.log 2>&1 &
```

You can also run each stage separately:

```bash
python3 -m app.research.import_punting_form
python3 -m app.research.import_betfair_history
python3 -m app.research.match_races
python3 -m app.research.form_score_optimizer
python3 -m app.research.market_pattern_analysis
python3 -m app.research.testing_model
python3 -m app.research.backtest_engine
python3 -m app.research.strategy_optimizer
```

## Outputs

Main cleaned datasets:

- `data/research/punting_form_clean.csv`
- `data/research/betfair_odds_clean.csv`
- `data/research/matched_runner_data.csv`
- `data/research/unmatched_punting_form.csv`
- `data/research/unmatched_betfair.csv`

Reports:

- `data/research/reports/market_patterns.csv`
- `data/research/reports/odds_bucket_report.csv`
- `data/research/reports/movement_report.csv`
- `data/research/reports/testing_model_results.csv`
- `data/research/reports/testing_model_backtest.csv`
- `data/research/reports/testing_model_feature_importance.csv`
- `data/research/reports/backtest_summary.csv`
- `data/research/reports/backtest_bets.csv`

Artifacts:

- `app/research/artifacts/best_form_score_config.json`
- `app/research/artifacts/best_testing_model_config.json`
- `app/research/artifacts/testing_model.joblib`
- `app/research/artifacts/best_strategy_config.json`

## How to interpret the outputs

`ROI`

Return on investment. Higher is better, but small sample sizes can mislead badly.

`Drawdown`

The biggest peak-to-trough fall in the simulated bank. Lower is safer.

`CLV`

Closing line value. Positive CLV usually means the bet beat the later market.

`Calibration`

Whether estimated probabilities behave like real probabilities over time. Poor calibration means a score may rank well but still be a bad probability estimate.

`Form score optimisation`

This tests different weighted form formulas and ranks them by a mix of signal quality and backtest behaviour.

`Testing model`

This trains offline candidate models on matched historical data, ranks the most useful features, and backtests model-driven selection rules to approximate and stress-test the live strategy logic.

## Overfitting warning

Do not push the best raw backtest config straight into live betting.

A strategy can look great because of:

- one hot track
- one short time period
- too few bets
- one unusual market regime

Use the saved config as a candidate, then validate it on newer unseen periods and apply changes to live strategy code manually.
