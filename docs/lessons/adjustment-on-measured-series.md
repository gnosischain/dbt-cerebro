---
id: adjustment-on-measured-series
title: A hand-set adjustment multiplied onto a series that already measures the effect counts it twice
status: observed
scope: >-
  any model that multiplies a measured or vendor-published series by a hand-set factor
  table (seasonal, regional or "correction" multipliers); canonical instance: the ESG
  carbon intensity ensemble over Ember's monthly values
symptom: >-
  a published metric swings harder across the year than its source, moves the wrong
  way for some members, and its annual total sits a few percent away from the
  source-based total
last_verified: 2026-11-10
evidence:
  - 'models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql before 2026-11: carbon_intensity_mean = Ember monthly intensity x a continent x month CASE (Europe 1.18 Dec-Feb, 0.92 Jun-Aug, 1.08 Mar/Apr/Nov; Asia 1.12 / 1.08 / 1.05; North America 1.15 / 1.12 / 1.03; Oceania, South America and Africa similar; World 1.0)'
  - 'Ember monthly values already carry the season. Cerebro query 2026-11-10 on crawlers_data.ember_electricity_data, 2023-2025, winter mean / annual mean and summer mean / annual mean: AUT 1.569 / 0.515, FIN 1.375 / 0.648, FRA 1.351 / 0.842, EST 1.222 / 0.785, NLD 1.191 / 0.814, DEU 1.09 / 0.904. ESP, USA and CAN are dirtier in summer (0.999 / 1.062, 1.017 / 1.071, 0.969 / 1.067), so the winter uplift pointed the wrong way for them'
  - 'the factors are not mean-neutral over a year (annual mean: Europe 1.045, Asia 1.067, North America 1.0775, Oceania 1.042, South America 1.02, Africa 1.0075), so annual CO2 was also inflated by 1-8%'
  - 'impact on published node CO2 (cerebro query 2026-11-10: sum of int_esg_dynamic_power_consumption.daily_co2_kg_mean against the same sum divided by each country''s factor): +16.6% in Dec 2025, Jan 2026 and Feb 2026; +6.0% Mar; +6.9% Apr; +0.7% May; -3.7% Jun 2026'
  - 'docs/model_review/revisit/ESG.md ESG-C07 (2026 audit) flagged only that the uncertainty band was not scaled by the factor; the factor itself was never questioned'
  - 'fix in tree, pending deploy (branch fix/esg-ember-generation-monthly): carbon_intensity_mean is Ember''s value as published; the seasonal_adjustment column is dropped together with its semantic measure and metric seasonal_adjustment_value (a sum of factors, approved, with synonyms such as energy consumption and network footprint)'
  - '2026-11-10, local run: with int_esg_carbon_intensity_ensemble.seasonal_adjustment dropped and its measure still authored, build_registry.py --validate --max-warnings 0 exited 0 and target/semantic_validation_report.json did not mention it. The gate checks entity and relationship columns (entity_expr_unknown_column, relationship_key_unknown_column) but not measure expressions, so the metric would only have failed at query time'
---

## Symptom
A published series swings across the year more than its source does, moves the wrong
way for some members (here: countries whose grid is dirtier in summer), and its annual
total sits a few percent away from a total computed from the source alone.

## Root cause
A factor table built for an input that hides an effect (an annual average) was applied
to an input that already measures it. Ember computes each monthly intensity from that
month's generation mix, so the month's season is already in the value; multiplying by
a seasonal factor counts it twice. Hand-set factor tables also rarely average 1 over a
cycle, so they bias totals as well as shapes.

## Forbidden action
Don't multiply a measured series by a hand-set adjustment table unless the input is
coarser than the effect, and the factors are calibrated against the same source and
average 1 over the cycle. Don't add an "adjustment" before measuring the input's own
variation for the effect you are adjusting for.

## Detection
For each member, compare the source's own profile (season mean / annual mean) with
the factor applied to it, and check that the factors average 1 over a full cycle. A
published series whose ratio to the source value is not 1 on average is biased.

## Safe remediation
Use the source value as published. Drop the adjustment column together with every
semantic measure and metric on it (`grep -rn <column> semantic/` first): the registry
gate does not check measure expressions, so dropping the column alone stays green in
CI and breaks the metric at query time (lesson semantic-retirement-gate). An API
column needs the same care: keep it at a neutral value until its consumers move.

## Ground truth
The vendor's published value: for Ember, `Emissions intensity (gCO2e/kWh)` on
`Electricity source = 'Total generation'` for the area and month.

## Enforcement
None. `carbon_intensity_mean` equals `base_carbon_intensity` after the fix; no test
pins it, so that a future multi-source ensemble stays possible.
