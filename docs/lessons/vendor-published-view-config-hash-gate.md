---
id: vendor-published-view-config-hash-gate
title: A vendor "published" view gated on the current config hash hides all earlier history
status: remediated
scope: >-
  any dbt model reading a service's curated/"published" view instead of its base
  tables (canonical: the rpc_state_indexer v_token_scalars_published /
  v_token_balances_published views feeding int_rpc_state_indexer_gno_supply_daily)
symptom: >-
  an api mart collapses to the last day or two while the service's base tables still
  hold the full history; nothing in dbt changed
last_verified: 2026-09-09
evidence:
  - rpc-state-indexer migrations/007_views.sql (v_publications_eligible INNER JOINs v_config_registry_current ON config_hash)
  - rpc-state-indexer commit fa80947 (2026-09-08, TokenConfig.universe_aliases serialised into canonical_config_json → every token target's config_hash changed at deploy)
  - ClickHouse 2026-09-09 09:40 UTC — census_publications daily_gno_supply_scalar 2,208 days vs v_publications_eligible 1; daily_token_supply eligible 1 day; pool targets (config unchanged) unaffected
  - dbt cron 2026-09-09 09:08 UTC rebuilt int_rpc_state_indexer_gno_supply_daily with 6 rows (query_log written_rows=6); api_gno_supply_daily showed two dates on the dashboard
  - fix: models/rpc_state_indexer/staging/stg_rpc_state_indexer__publications.sql (selection in dbt over base tables, no config-hash gate) — PR #77 merged 2026-09-09 09:57 UTC, image e27c954 deployed 13:45 UTC (deployment, live pod, CronJob verified)
---

## Symptom
`api_gno_supply_daily` served two dates. The staging views over the indexer's
`v_*_published` views returned only the last two days for the GNO job; the indexer's
`census_publications` still held 2,208 days.

## Root cause
The indexer's `v_publications_eligible` keeps a publication only if its `config_hash`
equals the target's **current** registry hash. The canonical config JSON serialises the
whole target model, so adding a field to that model (a new optional list, default
empty) changed the hash of every token target at the next deploy. From that moment
every earlier publication was ineligible; only days censused by the new image remained.
Pool targets, whose model did not change, kept their history — the confirming detail.
The next dbt cron rebuilt the table-materialised int model from what it could see.

## Forbidden action
Never point dbt staging at a vendor's curated/"published" view when the base tables
are available: the selection policy (which attempt counts) then lives behind a service
deploy and a migration, and a service-side change can remove history from every mart
without any dbt change. Never treat a sudden "only recent days" symptom as a dbt
regression before checking the source view against its base tables.

## Detection
Compare the source view with its base table per job:
`uniqExact(snapshot_date)` in `rpc_state_indexer.v_publications_eligible` vs
`rpc_state_indexer.census_publications` for the same `chain_id, job_name`. A ratio far
below 1 with unchanged base counts is this class. Source freshness does not catch it
(the newest day is present).

## Safe remediation
Select the published attempt in dbt: `stg_rpc_state_indexer__publications` takes, per
(chain, job, target, day), the newest publication whose attempt is verified (all batches
verified, no failed observations, no terminal error) at the chain's canonical finalized
day anchor — no config-hash condition. The two staging views join the raw
`token_scalars` / `token_balances` (FINAL) to it by `attempt_id`; `sources.yml` declares
the base tables. Rebuild the int model afterwards (`dbt run -s +int_rpc_state_indexer_gno_supply_daily`).
`remediated` since 2026-09-09 (PR #77 deployed). `enforced` would need a CI rule that no dbt model declares a `v_*_published` source — follow-up.
