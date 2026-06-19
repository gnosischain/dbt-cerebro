# Future Validator And Gnosis Pay Modeling

This note preserves a deferred design for validator-owned Gnosis Pay wallet analysis.

It is intentionally not active in the current dbt project. The related dbt models, schema entries, and executable semantic definitions were reverted so the live project stays focused on the currently approved model surface.

## Status

- Not implemented in active dbt models
- Not exposed as active semantic metrics
- Preserved here for future implementation planning

## Why It Was Deferred

The design is still useful, but it introduces new modeling choices around:

- how validator ownership should be normalized from consensus-side data
- how Gnosis Pay wallet ownership should be represented when wallets can have multiple owners
- which owner-level and wallet-level facts should be considered canonical public marts
- which metrics should become approved semantic metrics versus remain internal

For now, those decisions should be made intentionally in a later implementation pass rather than living half-active in the main dbt model graph.

## Proposed Future Models

### `int_consensus_validator_owners_latest`

Purpose:
- normalize validator-related ownership into execution-style owner addresses
- provide a reusable owner-level bridge between validator activity and execution-side wallet analysis

Expected grain:
- one row per `owner_address`

Expected fields:
- `owner_address`
- `validator_count`
- owner-level validator flags or buckets needed for segmentation

### `fct_execution_gpay_wallet_profile_latest`

Purpose:
- provide a single latest-state profile per Gnosis Pay wallet
- combine wallet ownership, validator overlap, and lifetime wallet behavior

Expected grain:
- one row per `wallet_address`

Expected fields:
- `wallet_address`
- `owner_address`
- `validator_count`
- `validator_bucket`
- `user_segment`
- `first_activity_date`
- `last_activity_date`
- `active_months`
- `total_payment_count`
- `total_payment_volume_usd`
- `total_cashback_usd`

### `fct_execution_gpay_wallet_activity_daily`

Purpose:
- provide wallet-level daily behavior facts for trend, usage, and cohort reporting
- support daily analysis without forcing complex joins at query time

Expected grain:
- one row per wallet per day, or another explicitly documented daily wallet grain

Expected fields:
- `day`
- `wallet_address`
- `owner_address`
- `validator_count`
- `validator_bucket`
- `user_segment`
- daily payment counts
- daily payment volume
- daily cashback totals
- daily active or paying flags

### `fct_execution_gpay_wallet_activity_monthly`

Purpose:
- provide a monthly wallet behavior mart for month-based reporting
- avoid requiring downstream semantic rollups from daily to monthly grain

Expected grain:
- one row per wallet per month, or another explicitly documented monthly wallet grain

Expected fields:
- `month`
- `wallet_address`
- `owner_address`
- `validator_count`
- `validator_bucket`
- `user_segment`
- monthly payment counts
- monthly payment volume
- monthly cashback totals
- monthly active or paying flags

## Proposed Update To Existing Model

### `int_execution_gpay_wallet_owners`

Potential future enhancement:
- decode all wallet owners rather than only a single owner

Why this matters:
- multi-owner wallets change the overlap between validators and Gnosis Pay users
- downstream wallet segmentation becomes more accurate if ownership expansion happens in a canonical intermediate model

## Planned Semantic Layer On Top

If these dbt models are later implemented and approved, a future semantic layer could expose metrics such as:

- wallet count
- validator-owned wallet count
- total payment volume
- total payment count
- total cashback
- average wallet tenure
- average active months
- average validators per owner or wallet
- daily active wallets
- monthly active wallets

Likely public dimensions:

- `day`
- `month`
- `user_segment`
- `validator_bucket`
- wallet activity type or action type where appropriate

## Suggested Future Rollout

1. Implement or reintroduce the dbt models with clear documented grain.
2. Add schema docs and tests for the new marts.
3. Re-run `dbt docs generate`.
4. Rebuild semantic artifacts:
   - `python scripts/semantic/build_registry.py --validate --target-dir target`
   - `python scripts/semantic/build_semantic_docs.py --target-dir target`
5. Add only curated approved semantic models and metrics once the marts are stable.
6. Validate the local `cerebro-mcp` semantic flow against local `target/` artifacts before any remote publish.

## Design Principle

The dbt layer should do the hard modeling work first.

The semantic layer should sit on top of stable, intentional marts rather than compensating for unfinished ownership or overlap logic.
