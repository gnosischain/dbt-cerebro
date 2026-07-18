---
id: ch-alias-shadows-where
title: ClickHouse output aliases shadow source columns in same-level WHERE
status: remediated
scope: any SELECT that aliases an expression to a name that also exists as a source
  column and filters on that name at the same level
symptom: a WHERE filter silently evaluates against the aliased output expression (often
  a constant) instead of the source column — wrong rows, no error
last_verified: 2026-07-17
evidence:
  - models/execution/gpay/intermediate/int_execution_gpay_roles_events.sql:63-68 (in-code explanation + subquery isolation)
  - models/execution/prices/intermediate/int_execution_prices_native_daily.sql:89 ("relabeling (ClickHouse alias-shadows-column-in-WHERE pitfall)")
  - models/execution/gpay/marts/fct_execution_gpay_user_balances_latest.sql:26-27; models/mixpanel_ga/marts/api_mixpanel_ga_gpay_card_spend_totals_weekly.sql:13 (same convention)
---

## Symptom
A filter like `WHERE event_name = 'X'` matches everything (or nothing) because the
SELECT also defines `'Y' AS event_name` — ClickHouse resolves the same-level WHERE
against the output alias, not the source column.

## Root cause
ClickHouse name resolution lets output aliases shadow source columns within the same
query level (unlike most SQL engines).

## Forbidden action
Don't relabel a column (especially to a constant) and filter on the original name at
the same level.

## Safe remediation / convention
Do the filtering in an inner scope where nothing shadows the column (subquery selecting
`*`), or relabel constants only in an **outer** subquery after filtering — the pattern
used by the models cited above.

## Detection
Hard to detect mechanically; review any SELECT that both aliases and filters the same
identifier. Row-count sanity checks on the filter.

## Enforcement
Convention documented in-code at the cited sites and here; no static gate.
