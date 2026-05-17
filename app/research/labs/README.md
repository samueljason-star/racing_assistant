# Racing Market Research Labs

This folder contains market-relative research labs for `racing_assistant`.

These labs are not broad winner-prediction systems and they are not raw ROI optimizers. The point is to understand market behaviour, discover candidate features, and identify narrow structural weaknesses that might later justify focused modelling.

## Core Philosophy

- The market is probably efficient overall.
- Broad “predict everything” approaches are weak by default.
- Longshot disagreement is currently distrusted unless it survives strong plausibility checks.
- CLV, shortening, and market-agreement behaviour matter more than spectacular one-off PnL pockets.
- A lab can be useful even if it finds no betting edge.

## Predictive vs Descriptive

### Leakage-safe predictive labs
These are intended to make ex-ante statements using only information available before the target window:

- `market_agreement_lab`
- `market_timing_lab`
- `feature_discovery_lab`
- `pace_shape_lab`
- `interaction_discovery_lab`
- `realistic_zone_feature_lab`
- `market_compression_lab`
- `pre60_context_lab`
- `interaction_strength_lab`
- `realistic_zone_market_agreement_lab`
- `leakage_stress_test_lab`
- `realistic_zone_feature_stability_lab`
- `compression_and_density_lab`
- `pace_context_engineering_lab`
- `market_rank_interaction_lab`
- `temporal_validation_lab`
- `clv_persistence_lab`
- `realistic_execution_lab`
- `pace_compression_feature_lab`
- `form_consistency_feature_lab`
- `execution_conversion_lab`
- `temporal_scaling_lab`
- `clv_to_value_conversion_lab`
- `executable_price_model_lab`
- `slippage_and_fill_lab`
- `shortening_vs_true_value_lab`
- `realistic_execution_simulation_lab`
- `post_shortening_value_lab`
- `realistic_zone_edge_filter_lab`
- `compression_conditioned_value_lab`
- `steam_size_value_lab`
- `executable_price_improvement_lab`
- `shortening_plus_compression_validation_lab`
- `post_shortening_overbet_lab`
- `execution_sensitivity_lab`
- `candidate_edge_audit_lab`
- `adverse_fill_avoidance_lab`
- `shortening_plus_compression_temporal_validation_lab`
- `paper_betting_simulation_lab`
- `post_shortening_value_preservation_lab`
- `executable_subset_discovery_lab`
- `live_readiness_hardening_lab`
- `realistic_zone_execution_decay_lab`
- `forward_month_extension_validation_lab`
- `forward_month_paper_execution_lab`
- `candidate_monthly_persistence_lab`
- `forward_execution_fragility_lab`

These labs explicitly audit leakage and exclude close-derived or post-race columns from predictive feature sets.

### Descriptive / structural labs
These are still useful, but should be interpreted as structural diagnostics rather than predictive proof:

- `market_microstructure_lab`
- `realistic_zone_microstructure_lab`
- `track_bias_lab`
- `stable_intent_lab`
- `sectional_efficiency_lab`
- `field_shape_lab`
- `favourite_longshot_lab`
- `regime_detection_lab`

## Current Trustworthy Conclusions

- Market-derived variables still dominate most predictive work.
- The current strongest leakage-safe non-market candidates are:
  - `pace_pressure_proxy`
  - `average_last_3_finish`
  - `form_signal`
- The strongest current realistic-zone / pre-60 candidates are:
  - `movement_open_to_60`
  - `favourite_density`
  - `odds_compression_index`
  - `form_rank_interaction`
  - `pace_rank_interaction`
- The strongest cleaned realistic-zone target is still `sixty_to_close`.
- The strongest current realistic-zone structural result is a leakage-stress-tested `gradient_boosting / non_market_only / sixty_to_close` row around `auc ~= 0.9307`.
- That result survived label randomization and target shifting checks in the expected way, and did not rely on duplicate-race or train/test race overlap.
- After leakage cleanup, realistic-zone `open_to_60` looks much closer to market-baseline behaviour than the earlier naive run suggested.
- The more credible current structural signal remains realistic-zone `sixty_to_close`, especially when non-market/context features are allowed to compete with pure market-only sets.
- The current execution question is no longer whether the signal exists at all, but whether realistic-zone CLV/shortening structure survives execution degradation and temporal scaling.
- CLV and executable value are not the same thing.
- A runner can shorten and still become fair-priced or overbet.
- A simulated executable pathway can look positive without proving live tradability; explicit fill/slippage assumptions still matter.
- Current execution-conversion work suggests there may be plausible monetisation pathways in the realistic zone, but they are still assumption-sensitive and not yet validated as live edge.
- Compression-conditioned candidates currently look more promising than generic strongest-shortener selection if the question is “does any value survive after the move?”
- `shortening_plus_compression` is still the best current paper-test candidate, and it survived simple outlier stress better than a pure lucky-spike story.
- The biggest current paper-test warning is still temporal weakness:
  - the strongest candidate is concentrated in a single month in the current sample
  - that means positive paper-execution rows are not yet proof of temporal robustness
- Strict forward-month validation is now implemented, and its first honest answer is:
  - no unseen month exists yet
  - current data only covers `2026-04`
  - so no true month-forward survival claim can be made yet
- Hardened live-readiness scoring currently collapses every candidate back toward zero once temporal weakness, concentration, slippage sensitivity, and overbet risk are penalized together.
- Adverse-fill avoidance looks more promising than exact executable-price prediction:
  - adverse-fill classification is tractable
  - exact fill-price regression is still weak
- Broad strongest-shortener buckets still often become slightly overbet after the move.
- Compression-conditioned filtering is still stronger than steam-size proxying alone.
- Paper-betting simulation can still show positive paths under explicit assumptions, but those remain simulation-only evidence rather than validated live execution.
- Strongest shorteners still often become slightly overbet after steam, especially in the broad top-shortener buckets.
- Small/medium steam remains interesting structurally, but the current ex-ante proxy candidates are still weaker than the best compression-conditioned slice.
- Live-readiness scoring is a diagnostic ranking, not an endorsement that a candidate is ready for money.
- Interaction effects remain small. The best current market-rank interaction is `market_rank_current x field_size`, with only modest incremental lift.
- Compression and favourite-density features look more useful as feature families than as standalone signals.
- Pace/context still looks more like a feature-engineering direction than a finished predictive signal.
- `runner_efficiency_proxy` looked strong in early discovery work, but it depends on finish position and is therefore post-race descriptive only. It should not be treated as a predictive feature.
- Longshot disagreement is still widely distrusted because it repeatedly clusters in noisy, weak-market zones.
- The framework is currently better at structural diagnostics and feature discovery than at finding validated betting edges.
- Known leakage risk to keep watching:
  - any `open_to_60` experiment that accidentally sees 60-second anchored market fields
  - any “non-market” bucket that still includes current/anchor market state
  - any descriptive feature whose name or construction hides post-race information

## Labs

### `market_agreement_lab`
- Researches: where pre-race and pre-60-second information predicts later market agreement.
- Outputs: predictive report, monotonicity, realistic-zone quality, feature importance, leakage audit.
- Use when: you want the cleanest CLV-first / agreement-first diagnostic.

### `market_timing_lab`
- Researches: which timing windows are more predictable without grouping on realized steam/drift labels.
- Outputs: prediction-by-window, timing window quality, persistence, leakage audit.
- Use when: you want to know whether open-to-60 or later windows are structurally weaker.

### `feature_discovery_lab`
- Researches: which leakage-safe features and feature groups add information beyond market state.
- Outputs: extended feature report, interaction strength, redundancy clusters, ablation, uniqueness summary.
- Use when: you want the highest-value next feature-engineering direction.

### `pace_shape_lab`
- Researches: whether pace-pressure proxies matter inside realistic liquid zones.
- Outputs: market agreement, CLV response, interaction effects, realistic-zone report.
- Use when: you want to test whether pace/context can add small incremental lift.

### `interaction_discovery_lab`
- Researches: whether contextual combinations beat standalone variables.
- Outputs: interaction report, CLV lift, AUC lift, realistic-zone quality.
- Use when: you suspect the market prices simple features but not combinations.

### `realistic_zone_feature_lab`
- Researches: whether non-market and interaction features add lift inside the most credible realistic zone.
- Outputs: feature report, ablation, monotonicity, leakage audit.
- Use when: you want the cleanest read on feature uniqueness vs market re-encoding in rank `1-5`, odds `2-10`, small/medium fields.

### `market_compression_lab`
- Researches: favourite density, market compression, and dispersion structure.
- Outputs: compression report, CLV by compression regime, interaction report, leakage audit.
- Use when: you want to know whether compressed favourite-heavy markets behave differently from spread-out books.

### `pre60_context_lab`
- Researches: whether open-to-60 state and pre-60 context predict later shortening and CLV.
- Outputs: predictive report, monotonicity, realistic-zone report, leakage audit.
- Use when: you want the clearest timing-context research around the strongest current window.

### `interaction_strength_lab`
- Researches: whether specific realistic interactions create measurable incremental lift.
- Outputs: interaction-strength report, AUC lift, CLV lift, realistic-zone quality, leakage audit.
- Use when: you want to prioritize which contextual combinations deserve proper feature engineering.

### `realistic_zone_market_agreement_lab`
- Researches: the cleanest strict realistic-zone market-agreement prediction setup across `open_to_60` and `sixty_to_close`.
- Outputs: market-agreement report, monotonicity, feature importance, leakage audit.
- Use when: you want the most commercially relevant predictive benchmark in the framework.

### `leakage_stress_test_lab`
- Researches: whether the strongest realistic-zone `sixty_to_close` result survives adversarial leakage and contamination checks.
- Outputs: stress-test report, feature availability audit, timestamp audit, target contamination audit.
- Use when: you want to know whether a strong result is actually robust or just accidentally contaminated.

### `realistic_zone_feature_stability_lab`
- Researches: whether realistic-zone non-market/context lift persists through time.
- Outputs: stability report, temporal consistency table, walk-forward-style report, leakage audit.
- Use when: you want to know whether a signal is persistent instead of one-off.

### `compression_and_density_lab`
- Researches: stronger opening-market compression and favourite-density features.
- Outputs: compression feature report, density analysis, CLV response, leakage audit.
- Use when: you want to turn market structure into better ex-ante features.

### `pace_context_engineering_lab`
- Researches: stronger pace/context features and pace-conditioned interactions.
- Outputs: pace-context report, interaction table, realistic-zone quality, leakage audit.
- Use when: you want to push pace/context from intuition toward usable features.

### `market_rank_interaction_lab`
- Researches: how market rank interacts with field size, track condition, density, pace, form, and early movement.
- Outputs: interaction report, lift table, CLV table, leakage audit.
- Use when: you want to find contextual combinations that matter more than standalone variables.

### `temporal_validation_lab`
- Researches: whether realistic-zone signals survive non-degenerate temporal validation.
- Outputs: temporal report, robustness table, fold-quality report, leakage audit.
- Use when: you want to separate stable structure from fold artefacts.

### `clv_persistence_lab`
- Researches: whether realistic-zone CLV survives outlier removal and basic temporal slicing.
- Outputs: CLV persistence report, monotonicity persistence, temporal stability, outlier stress, leakage audit.
- Use when: you want to know whether positive CLV is broad and persistent or just an outlier artefact.

### `realistic_execution_lab`
- Researches: whether realistic-zone agreement signals survive mild execution degradation.
- Outputs: execution report, fill degradation table, slippage analysis, leakage audit.
- Use when: you want to stress paper CLV against worse fills, stale pricing, and partial capture.

### `realistic_zone_microstructure_lab`
- Researches: realistic-zone market states, archetypes, and compression/volatility structure.
- Outputs: realistic-zone clusters and movement archetypes.
- Use when: you want descriptive structure for the realistic zone only.

### `pace_compression_feature_lab`
- Researches: whether pace only becomes useful when paired with market compression and favourite density.
- Outputs: pace-compression report, interaction-lift table, realistic-zone quality, leakage audit.
- Use when: you want to test conditional pace hypotheses rather than broad pace claims.

### `form_consistency_feature_lab`
- Researches: whether contextual form consistency beats raw form inputs.
- Outputs: form-consistency report, uniqueness table, realistic-zone quality, leakage audit.
- Use when: you want to push non-market form/context features beyond naive last-start style fields.

### `execution_conversion_lab`
- Researches: whether market-agreement structure has any plausible CLV-to-ROI conversion pathway.
- Outputs: execution conversion report, CLV-to-ROI table, shortening-to-ROI table, leakage audit.
- Use when: you want to test monetisation plausibility without turning the project into threshold farming.

### `temporal_scaling_lab`
- Researches: whether realistic-zone signals survive larger and less convenient temporal splits.
- Outputs: temporal scaling report, fold-level table, regime stability table, leakage audit.
- Use when: you want a harsher temporal robustness check than a single holdout.

### `clv_to_value_conversion_lab`
- Researches: whether predicted shortening survives into executable value after realistic fill assumptions.
- Outputs: conversion report, post-shortening value decay, executable value persistence, leakage audit.
- Use when: you want to know whether shortening still leaves value after execution.

### `executable_price_model_lab`
- Researches: simulated executable price, slippage, and executable CLV quality.
- Outputs: executable price predictions, slippage estimates, summary.
- Use when: you want a regression-style view of fill quality rather than just classification of shorteners.

### `slippage_and_fill_lab`
- Researches: how fragile the realistic-zone signal is to progressively worse fills.
- Outputs: slippage stress report, fill decay, execution fragility analysis, leakage audit.
- Use when: you want to know how quickly an attractive paper signal dies under worse execution.

### `shortening_vs_true_value_lab`
- Researches: whether shorteners are still value or whether they become overbet momentum runners.
- Outputs: shortening-vs-value report, post-shortening calibration, overbet analysis, leakage audit.
- Use when: you want to separate “market moved” from “runner still worth backing”.

### `realistic_execution_simulation_lab`
- Researches: end-to-end execution pathways under explicit scenario assumptions.
- Outputs: execution simulation table, realism summary, pathway analysis, leakage audit.
- Use when: you want the most direct answer to “is there any plausible executable pathway here?”.

### `post_shortening_value_lab`
- Researches: whether value remains after shortening actually occurs.
- Outputs: post-shortening value report, edge decay, calibration, leakage audit.
- Use when: you want to know whether steam preserves or destroys value.

### `realistic_zone_edge_filter_lab`
- Researches: a small fixed set of realistic executable candidate filters.
- Outputs: edge-filter report, executable edge candidates, leakage audit.
- Use when: you want a conservative executable-candidate screen without brute-force search.

### `compression_conditioned_value_lab`
- Researches: whether shortening preserves value only in specific compression regimes.
- Outputs: compression-conditioned value report, compression buckets, post-shortening edge table, leakage audit.
- Use when: you want to know whether compression separates value-preserving steam from overbet momentum.

### `steam_size_value_lab`
- Researches: whether tiny/small/medium/large steam behave differently after the move.
- Outputs: steam-size value report, overbet curve, value persistence, leakage audit.
- Use when: you want to know whether steam size itself is useful structurally.

### `executable_price_improvement_lab`
- Researches: better executable-price, slippage, and fill-quality modelling under realistic-zone assumptions.
- Outputs: executable-price improvement report, error analysis, fill-quality model report.
- Use when: execution realism is the bottleneck and you need a better fill model before taking simulation results too seriously.

### `shortening_plus_compression_validation_lab`
- Researches: whether the current best compression-conditioned candidate survives outlier, concentration, and simple temporal stress.
- Outputs: validation report, stress tests, candidate bet list, leakage audit.
- Use when: you want to try to break the strongest current executable candidate.

### `post_shortening_overbet_lab`
- Researches: where steam overshoots fair value after the market move.
- Outputs: overbet report, overbet by steam size, overbet by compression, warning-feature table, leakage audit.
- Use when: you want to avoid backing shorteners that look like momentum rather than value.

### `execution_sensitivity_lab`
- Researches: how much execution degradation each realistic candidate can tolerate.
- Outputs: execution sensitivity report, break-even slippage table, candidate decay table, leakage audit.
- Use when: you want to map which candidates die fastest as fills worsen.

### `candidate_edge_audit_lab`
- Researches: one consolidated audit of the current candidate set, including a heuristic live-readiness score.
- Outputs: candidate audit report, live-readiness scores, leakage audit.
- Use when: you want one place to compare which candidates are closest to paper-test readiness.

### `adverse_fill_avoidance_lab`
- Researches: whether likely bad fills can be identified and avoided with ex-ante market-chaos and compression-instability features.
- Outputs: adverse-fill report, fill-quality feature importance, probability buckets, leakage audit.
- Use when: exact executable price is too noisy, but you still want to avoid obviously dangerous execution environments.

### `shortening_plus_compression_temporal_validation_lab`
- Researches: whether the best current paper-test candidate survives month and split-based temporal hardening.
- Outputs: temporal validation report, monthly breakdown, temporal stress table, leakage audit.
- Use when: you want the blunt answer to whether `shortening_plus_compression` is temporally real or still concentrated.

### `paper_betting_simulation_lab`
- Researches: bankroll-path behavior for a small fixed set of realistic-zone execution candidates under explicit fill scenarios.
- Outputs: paper simulation report, cumulative paper paths, bankroll simulation table, leakage audit.
- Use when: you want to test paper execution without pretending it is live tradability.

### `post_shortening_value_preservation_lab`
- Researches: which ex-ante conditions are associated with value surviving after the move.
- Outputs: value-preservation report, overbet-after-shortening table, persistence-conditions table, leakage audit.
- Use when: you want to separate “shortened” from “still worth backing after shortening.”

### `executable_subset_discovery_lab`
- Researches: a constrained set of realistic executable subsets rather than broad combinatorial strategy mining.
- Outputs: subset report, subset stability table, subset live-readiness table, leakage audit.
- Use when: you want to compare a small fixed set of executable hypotheses inside the realistic zone.

### `live_readiness_hardening_lab`
- Researches: a harsher readiness score that heavily penalizes temporal weakness, concentration, fill fragility, and overbet risk.
- Outputs: hardened readiness scores, penalty breakdown, leakage audit.
- Use when: you want a pessimistic screen for whether any current candidate is even close to live paper-betting readiness.

### `realistic_zone_execution_decay_lab`
- Researches: how candidate edges decay under progressively worse execution assumptions.
- Outputs: execution-decay table, break-even thresholds, leakage audit.
- Use when: you want to know which candidate dies first once delays, slippage, or partial fills worsen.

### `forward_month_extension_validation_lab`
- Researches: strict unseen-month candidate survival with frozen thresholds from past months only.
- Outputs: forward extension report, candidate comparison, decay analysis, leakage audit.
- Use when: you want a true forward-style answer instead of another historical split.

### `forward_month_paper_execution_lab`
- Researches: pseudo-live paper execution on the next unseen month only.
- Outputs: forward paper execution table, forward execution paths, leakage audit.
- Use when: you want the cleanest distinction between historical paper simulation and actual unseen-month paper evaluation.

### `candidate_monthly_persistence_lab`
- Researches: whether candidate performance is distributed across months or concentrated in one period.
- Outputs: monthly persistence table, rolling stability, monthly edge decay, leakage audit.
- Use when: you want a blunt month-concentration check before treating a candidate as robust.

### `forward_execution_fragility_lab`
- Researches: how frozen forward-month candidates degrade under worsening execution assumptions.
- Outputs: forward fragility table, forward break-even thresholds, leakage audit.
- Use when: you have enough months to test whether an unseen-month candidate still survives slippage and delay.

### `market_microstructure_lab`
- Researches: recurring market states and movement archetypes.
- Outputs: microstructure clusters, movement archetypes.
- Use when: you want structural clustering rather than predictive claims.

### `track_bias_lab`
- Researches: barrier/condition and track-response structure.

### `stable_intent_lab`
- Researches: trainer/jockey/stable intent proxies.

### `sectional_efficiency_lab`
- Researches: sectional/efficiency-style proxies and their market response.

### `favourite_longshot_lab`
- Researches: favourite-longshot distortion, calibration, and disagreement concentration.

### `field_shape_lab`
- Researches: field structure, compression, and race-shape behaviour.

### `regime_detection_lab`
- Researches: whether market behaviour varies materially over time, track, or class.

## How To Run

Each lab supports:

```bash
python -m app.research.labs.<lab_name> --target sixty_to_close
```

Examples:

```bash
python -m app.research.labs.market_agreement_lab --target sixty_to_close
python -m app.research.labs.feature_discovery_lab --target sixty_to_close
python -m app.research.labs.market_timing_lab --target sixty_to_close
python -m app.research.labs.realistic_zone_feature_lab --target sixty_to_close
python -m app.research.labs.pre60_context_lab --target sixty_to_close
python -m app.research.labs.realistic_zone_market_agreement_lab --target sixty_to_close
python -m app.research.labs.leakage_stress_test_lab --target sixty_to_close
python -m app.research.labs.realistic_zone_feature_stability_lab --target sixty_to_close
python -m app.research.labs.temporal_validation_lab --target sixty_to_close
python -m app.research.labs.clv_persistence_lab --target sixty_to_close
python -m app.research.labs.realistic_execution_lab --target sixty_to_close
python -m app.research.labs.pace_compression_feature_lab --target sixty_to_close
python -m app.research.labs.form_consistency_feature_lab --target sixty_to_close
python -m app.research.labs.execution_conversion_lab --target sixty_to_close
python -m app.research.labs.temporal_scaling_lab --target sixty_to_close
python -m app.research.labs.clv_to_value_conversion_lab --target sixty_to_close
python -m app.research.labs.executable_price_model_lab --target sixty_to_close
python -m app.research.labs.slippage_and_fill_lab --target sixty_to_close
python -m app.research.labs.shortening_vs_true_value_lab --target sixty_to_close
python -m app.research.labs.realistic_execution_simulation_lab --target sixty_to_close
python -m app.research.labs.post_shortening_value_lab --target sixty_to_close
python -m app.research.labs.realistic_zone_edge_filter_lab --target sixty_to_close
python -m app.research.labs.compression_conditioned_value_lab --target sixty_to_close
python -m app.research.labs.steam_size_value_lab --target sixty_to_close
python -m app.research.labs.executable_price_improvement_lab --target sixty_to_close
python -m app.research.labs.shortening_plus_compression_validation_lab --target sixty_to_close
python -m app.research.labs.post_shortening_overbet_lab --target sixty_to_close
python -m app.research.labs.execution_sensitivity_lab --target sixty_to_close
python -m app.research.labs.candidate_edge_audit_lab --target sixty_to_close
python -m app.research.labs.adverse_fill_avoidance_lab --target sixty_to_close
python -m app.research.labs.shortening_plus_compression_temporal_validation_lab --target sixty_to_close
python -m app.research.labs.paper_betting_simulation_lab --target sixty_to_close
python -m app.research.labs.post_shortening_value_preservation_lab --target sixty_to_close
python -m app.research.labs.executable_subset_discovery_lab --target sixty_to_close
python -m app.research.labs.live_readiness_hardening_lab --target sixty_to_close
python -m app.research.labs.realistic_zone_execution_decay_lab --target sixty_to_close
python -m app.research.labs.forward_month_extension_validation_lab --target sixty_to_close
python -m app.research.labs.forward_month_paper_execution_lab --target sixty_to_close
python -m app.research.labs.candidate_monthly_persistence_lab --target sixty_to_close
python -m app.research.labs.forward_execution_fragility_lab --target sixty_to_close
```

## Highest-Value Current Research Directions

- Better leakage-safe pace and shape features.
- Better historical form consistency and efficiency measures.
- Better pre-60 market context and compression metrics.
- Better opening-market versions of compression / favourite-density / pace context features for `open_to_60`.
- Better market-structure features that do not collapse back into plain market-state re-encoding.
- Better execution realism and CLV-persistence diagnostics for realistic-zone `sixty_to_close`.
- Better executable-price proxies:
  - spread
  - slippage
  - price acceleration
  - liquidity / fill-quality proxies
- Better compression-conditioned value filters:
  - value after steam
  - overbet warning structure
  - ex-ante candidate screening
- Better distinction between:
  - shortening prediction
  - CLV persistence
  - executable value
- Better paper-test candidate discipline:
  - candidates should keep positive executable edge
  - candidates should avoid obvious post-shortening overbet behaviour
  - candidates should survive slippage sensitivity checks
- Better temporal depth before making any forward-style claims:
  - unseen-month validation matters more than prettier in-sample paper metrics
  - month concentration should be reduced before any candidate is treated as paper-live worthy
- Interaction discovery inside realistic market zones:
  - market rank `1–5`
  - odds `2–10`
  - small/medium fields
- Strict realistic-zone feature comparisons:
  - `market_only` vs `all_features`
  - `non_market_only`
  - `interaction_only`

## What To Deprioritize

- Broad brute-force model farms.
- Giant strategy searches.
- Longshot disagreement pockets with weak market plausibility.
- Any result that only looks good because it groups directly on realized future movement.
- Any “open_to_60” result that depends on 60-second anchored state.
