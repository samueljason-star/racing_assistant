# Research Labs

These labs are exploratory market-relative research tools for the `racing_assistant` repo.

They are not broad winner-prediction systems and they are not ROI optimizers. The working assumption is that the market is mostly efficient overall, so the point of the lab suite is to discover narrow structural weaknesses, candidate features, and market-behaviour patterns that might justify deeper focused research.

## Philosophy

- Structure over hype: a lab can be useful even if it finds no betting edge.
- CLV and market behaviour matter more than raw PnL.
- False positives are expected, especially in longshots and tiny samples.
- Commercial plausibility matters: strong-market runners and repeatable zones deserve more weight than spectacular outliers.

## Labs

### `pace_shape_lab`
- Purpose: test whether pace-shape proxies create systematic market mispricing.
- Run: `python -m app.research.labs.pace_shape_lab`
- Looks for: pressure regimes, leader congestion, pace-collapse style CLV pockets.

### `market_timing_lab`
- Purpose: understand when the market is most wrong.
- Run: `python -m app.research.labs.market_timing_lab`
- Looks for: early weakness, late steam/drift behaviour, volatility windows.

### `stable_intent_lab`
- Purpose: inspect trainer/jockey/stable intent patterns.
- Run: `python -m app.research.labs.stable_intent_lab`
- Looks for: prep-stage effects, trainer-jockey combinations, stable-intent proxies.

### `track_bias_lab`
- Purpose: inspect track-bias adaptation and barrier/lane effects.
- Run: `python -m app.research.labs.track_bias_lab`
- Looks for: inside/outside bias proxies, condition interactions, slow market adaptation.

### `sectional_efficiency_lab`
- Purpose: inspect sectional-efficiency and energy-distribution proxies.
- Run: `python -m app.research.labs.sectional_efficiency_lab`
- Looks for: strong-finisher underpricing, inefficient profiles, sectional-context effects.

### `favourite_longshot_lab`
- Purpose: quantify favourite-longshot distortion and disagreement clustering.
- Run: `python -m app.research.labs.favourite_longshot_lab`
- Looks for: odds-bucket calibration gaps, longshot noise, residual distortions.

### `field_shape_lab`
- Purpose: test field-size and race-shape effects.
- Run: `python -m app.research.labs.field_shape_lab`
- Looks for: compression effects, chaos regimes, CLV by field structure.

### `market_agreement_lab`
- Purpose: inspect where models and market later agree or disagree.
- Run: `python -m app.research.labs.market_agreement_lab`
- Looks for: shortening patterns, disagreement buckets, noisy vs useful signals.

### `regime_detection_lab`
- Purpose: detect shifting market regimes.
- Run: `python -m app.research.labs.regime_detection_lab`
- Looks for: month/track/class volatility, changing CLV behaviour, unstable regimes.

### `feature_discovery_lab`
- Purpose: discover new features and interactions.
- Run: `python -m app.research.labs.feature_discovery_lab`
- Looks for: mutual information, interaction strength, market-only vs non-market lift.

## Outputs

Each lab writes:
- one or more CSV reports to `data/research/reports/`
- a markdown summary explaining the structural read

Labs explicitly flag:
- low sample size
- high concentration
- likely false positives
- likely longshot noise
- commercial plausibility

## Suggested Use

1. Run labs to generate hypotheses.
2. Ignore spectacular outliers until they survive concentration and plausibility checks.
3. Promote only the most credible patterns into focused modelling modules.
4. Treat “nothing useful found” as a valid and valuable result.
