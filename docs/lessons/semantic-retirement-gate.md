---
id: semantic-retirement-gate
title: Retiring/renaming a model with semantic authoring breaks the CI registry gate
status: enforced
scope: any dbt model referenced by semantic/authoring/**/semantic_models.yml
symptom: CI fails build_registry.py --validate with missing_measure /
  metric_missing_root_model after a model rename/retire that looked clean locally
last_verified: 2026-07-17
evidence:
  - scripts/semantic/build_registry.py:1461 ("missing_measure"), :1478 ("metric_missing_root_model"), :1160 ("approved_model_missing_measures"); --validate :1578, exit-1 over max-warnings :1684-1689
  - .github/workflows/build-and-release.yaml (~:157) — runs build_registry.py --validate --max-warnings 0; step comment: validation backlog zeroed, any new error OR warning fails the build
  - '2026-11-10, local run: with int_esg_carbon_intensity_ensemble.seasonal_adjustment dropped and its measure still authored, build_registry.py --validate --max-warnings 0 exited 0 and target/semantic_validation_report.json did not mention it. The gate checks entity and relationship columns (entity_expr_unknown_column, relationship_key_unknown_column) but not measure expressions, so the metric would only have failed at query time'
---

## Symptom
A model rename/retire passes `dbt build` locally but fails CI at the semantic-registry
step: metrics defined on the old model orphan.

## Root cause
Semantic authoring is deliberately decoupled from model files — metrics in
`semantic/authoring/` reference models by name. dbt has no knowledge of those
references, so nothing in a dbt-only workflow flags the break. The CI gate is strict
(`--max-warnings 0`; even a lone missing description fails).

## Forbidden action
Don't retire, rename, or delete a model based only on dbt-side usage (`dbt ls`,
`grep ref(`); semantic authoring is a second reference surface, and the
metrics-dashboard SQL is a third.

## Detection
Before retiring/renaming: `grep -r "<model_name>" semantic/` and run
`python scripts/semantic/build_registry.py --target-dir target --validate
--max-warnings 0`. Details land in `target/semantic_validation_report.json`.

## Safe remediation
Move/retire the semantic authoring block together with the model change; re-point
metrics to the successor model or delete them explicitly.

## Ground truth
target/semantic_validation_report.json after a local registry build.

## Enforcement
CI gate (strict, zero-warning) since the validation backlog was zeroed. It covers
model retire/rename only: a dropped COLUMN that a measure still reads passes the gate
(measure expressions are not checked against model columns) and fails at query
time. Before dropping a column, `grep -rn <column> semantic/` and remove its
measures and metrics in the same change.
