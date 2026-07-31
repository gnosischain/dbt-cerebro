---
id: stage-vars-scope-illusion
title: A meta.full_refresh stage var only scopes a run if the model SQL reads it — and on
  insert_overwrite it wipes instead
status: enforced
scope: models with meta.full_refresh stages carrying a dimension include filter
  (symbol/slice); macros/db/symbol_filter.sql; scripts/full_refresh/refresh.py
symptom: a per-token backfill either silently doubles every OTHER token in the window
  (append models) or silently deletes them (insert_overwrite models)
last_verified: 2026-07-31
evidence:
  - seeds/tokens_whitelist.csv (2026-07-31, commit da587794) — 6 Ripio wrapped-fiat
    stablecoins added with date_start 2026-02-19; the same commit added a `ripio_wfiat`
    stage to exactly the two models that read var('symbol') and take the append path
  - models/execution/tokens/intermediate/int_execution_tokens_address_diffs_daily.sql:4,41-42
    — literal incremental_strategy='insert_overwrite' with partition_by='toStartOfMonth(date)'
    that nevertheless honours var('symbol'); same shape in int_execution_tokens_transfers_daily.sql:5
  - "models/execution/tokens/intermediate/int_execution_tokens_balance_cohorts_daily.sql
    and int_execution_tokens_balances_by_sector_daily.sql (pre-2026-07-31) — meta.full_refresh
    present, zero var('symbol') reads and zero symbol_filter() calls, so a stage var added
    here would have been inert; both were given the wiring on 2026-07-31"
  - scripts/full_refresh/refresh.py:513-522 — vars are assembled as
    {start_month, end_month, **stage_vars} and passed to `dbt run --vars`; dbt does not
    error on a var no model reads
  - macros/db/symbol_filter.sql — the only dimension-filter mechanism in the repo
---

## Symptom
A backfill scoped to one new token "succeeds", and afterwards either every *other* token
in the touched months is duplicated, or every other token is gone. The run log looks
clean either way — the stage name and its `vars` are printed, so the run reads as scoped.

## Root cause
A `meta.full_refresh` stage's `vars` are inert metadata. `refresh.py` merges them into
`{start_month, end_month, **stage_vars}` and hands them to `dbt run --vars`. dbt does not
error on a var that no model reads, so scoping only happens if the model's SQL actually
reads it — `{% set symbol = var('symbol', none) %}` plus `{{ symbol_filter(...) }}`.

That gives two independent failure modes, with opposite polarity:

| Model shape | Stage passes `symbol:` | Result |
|---|---|---|
| SQL never reads `var('symbol')` | filter is inert | run covers ALL tokens; on an `append` strategy that is an exact second copy of every token in the window (`append-over-populated-duplicates`) |
| SQL reads it, strategy resolves to literal `insert_overwrite` | filter applies | the result set holds only the filtered tokens, so `REPLACE PARTITION` swaps each whole month to filtered-only content — every other token is wiped (`staged-insert-overwrite-wipe`, on the *dimension* axis instead of the time axis) |

Only the intersection is safe: the SQL reads the var **and** the strategy resolves to
`append` for the scoped window.

## Forbidden action
Never add a per-dimension stage (`symbol`, `slice`) to a model without first confirming
both halves:
1. the model body contains `var('<name>'` and applies it via `symbol_filter`;
2. the strategy resolves to `append` when `start_month` is set — i.e. the
   `('append' if start_month else …)` pattern, not a literal `insert_overwrite`.

Never "fix" a missing filter by dropping the stage var and running a bare
`start_month` window over already-populated months — that is the duplicate half.

## Detection
Before running: `grep -c "var('symbol'" <model>.sql` must be non-zero for any model whose
stage passes `symbol:`, and the compiled SQL under those vars must contain the
`symbol IN (…)` predicate.

After running, per-symbol row counts for the touched window against a pre-run baseline:
every pre-existing symbol must be **unchanged** — a decrease is the wipe, an exact
doubling is the duplicate.

## Safe remediation
Give the model the filter rather than working around its absence. For a stateless,
per-token-grain aggregate already on `('append' if start_month else …)`, adding the
`symbol`/`symbol_exclude` var reads plus `symbol_filter` calls (and passing `filters_sql`
into `apply_monthly_incremental_filter` so the watermark subquery is scoped too) makes a
new-token backfill purely additive — no partition drop needed, because new rows land at a
new `token_address` and cannot collide.

Where the strategy is a literal `insert_overwrite` and cannot be changed cheaply, do not
scope by dimension at all: re-run the affected months **unfiltered**. `REPLACE PARTITION`
is atomic and idempotent, so a whole-month recompute reproduces every existing token and
picks up the new one.

## Ground truth
Per-symbol row counts for the window, before and after, from a baseline captured *before*
the first write. `REPLACE PARTITION` has no undo.

## Enforcement
STATIC GATE (2026-07-31): `scripts/checks/no_delete_insert.py` rules
`stage_var_not_read` (a stage passes an include filter the model body never reads) and
`staged_scoped_include_overwrite` (a literal-`insert_overwrite` model carries a stage with
an include filter). Both operate on RAW model code — the manifest collapses the strategy
expression to its default branch and cannot see which vars the body reads. Neither rule is
allowlisted: no model violates either today, and the gate exists to keep it that way. Note
these are deliberately narrower than `staged_literal_overwrite`, whose grandfathered
entries in `no_delete_insert.allow` are defensible precisely because those stages carry
`symbol_exclude` (an exclude filter, run whole-month) rather than an include filter.
