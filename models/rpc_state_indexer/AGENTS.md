# models/rpc_state_indexer — how to work with the census-derived models

Scoped guide for the state-census layer: token holder balances and token supply read from
the rpc-state-indexer service instead of accumulated from transfer logs. Root rules in
[AGENTS.md](../../AGENTS.md) apply; this file adds what is specific here. Read it before
touching anything under `models/rpc_state_indexer/` or any consumer of these models.

## What the census is

The indexer reads `balanceOf(holder)` for every discovered holder and `totalSupply()` for
every configured token at one canonical block per UTC day (the *day anchor*), on Gnosis
(chain 100) and Ethereum (chain 1), and publishes a token-day only when the observation is
complete and, for the curated balances job, only when the holder sum equals `totalSupply`
(`integrity_mode: full_supply`, `universe: full_holders`). Jobs on chain 100:
`daily_curated_balances` (holder balances, the tokens dbt serves), `daily_token_supply`
(totalSupply for ~3,400 tokens), `daily_treasury`, `daily_pool_reserves`,
`daily_cl_liquidity`, `daily_atokens_full`, `daily_gno_supply_scalar`. Indexer repo:
`gnosischain/rpc-state-indexer`, config under `config/gnosis/{tokens,jobs}.yaml`.

Every day is an independent observation. There is no carry-forward, no running sum, no seed
day; a wrong day cannot poison the next one, and a repaired day needs no reseeding. That is
the whole reason this layer replaced the transfer-derived chain, which drifted from on-chain
state (measured 2026-09-14: SAFE −7.7%, BRLA −2.3%, WETH +1.7%; an `eth_call` at the anchor
block matched the census to six decimals) and wiped a month on 2026-09-15.

## Layout

- `staging/stg_rpc_state_indexer__publications.sql` — picks the single published attempt per
  (chain, job, target, day) from the **base tables**, never the service's `v_*_published`
  views (`docs/lessons/vendor-published-view-config-hash-gate.md`).
- `staging/stg_rpc_state_indexer__token_balances_published.sql`,
  `…__token_scalars_published.sql` — observations joined to the published attempt. No
  `decimals`, `symbol` or `token_class`; those come from the `tokens_whitelist` seed.
- `intermediate/int_rpc_state_indexer_token_balances_daily` — `(date, token_address, address)`,
  native units; replaces `int_execution_tokens_balances_native_daily`.
- `intermediate/int_rpc_state_indexer_token_balances_priced_daily` — adds `balance_usd`
  (LEFT join on `(date, upper(symbol))`); replaces `int_execution_tokens_balances_daily`.
- `intermediate/int_rpc_state_indexer_token_supply_daily` — `(date, token_address)`, with
  `supply_total` (totalSupply less the zero-address balance), `supply_holders` (sum of holder
  balances), `holders`; replaces `int_execution_tokens_supply_holders_daily`. For curated
  tokens `supply_total == supply_holders` exactly, because the census only publishes when
  they reconcile; consumers take `supply_total`.
- `intermediate/int_rpc_state_indexer_gno_supply_daily` and `marts/api_gno_supply_daily` — the
  four-label GNO supply endpoint (WL-036/037).

## Invariants (also in each model's `meta.agent`)

1. **Always filter `chain_id` and `job_name`.** The same (token, holder, day) exists under
   several jobs; mixing them double-counts. The balance models read
   `daily_curated_balances` only; the supply model reads its scalars from the curated job
   only — reading scalars from every job admits whitelist wrappers (`aGno*`, `sp*`) that carry
   a supply scalar but no holder census, 14 supply-only tokens a day
   (`docs/lessons/census-scalars-cross-job-scope.md`).
2. **Zero address dropped at the source.** The census records the real burned balance at
   `0x0`; it is published once, as the burn term in `supply_total`. The transfer-derived chain
   held MINUS the minted supply there, so every old consumer's `balance > 0` silently excluded
   it; a census row would pass that filter (`docs/lessons/zero-address-sign-flip-across-derivations.md`).
3. **Scope = `tokens_whitelist` ∩ the indexer's curated selector**, via INNER JOIN on the
   seed. `symbol`/`symbol_exclude` vars are not read here. A whitelist token missing from the
   census yields no rows, never a wrong number: check the selector and the token's
   `deployment_block` before suspecting dbt (`docs/lessons/indexer-deployment-block-truncates-history.md`).
4. **Incremental window comes from `apply_monthly_incremental_filter`**, never hand-rolled:
   whole months under `insert_overwrite`, strictly after `max(date)` under the runner's
   `incremental_end_date` (append). The first cron with a hand-rolled month window appended
   September three times (`docs/lessons/microbatch-append-window-must-be-strategy-aware.md`).
5. **Migrated contracts are windowed per address by the seed** (`date_start`/`date_end`), and
   on the indexer side a retired facade contract is *aliased* to the live ledger
   (`universe_aliases`), never end-dated (`docs/lessons/migrated-token-contract-alias-not-end-date.md`).
   Compare per address, never per symbol, when checking coverage.
6. Balance math in exact `Int256`; scale to `Float64` only at the aggregate boundary.

## Operating the layer

- **Daily**: the cron builds the three models under `tag:production` through the microbatch
  runner. The census legitimately lags to D-1; the coverage test excludes yesterday.
- **Add a token**: add it to `config/gnosis/tokens.yaml` (verify `deployment_block` with the
  archive `eth_getCode` no-code/code boundary and record it in a comment) and to the
  `daily_curated_balances` selector in `config/gnosis/jobs.yaml`; the assistant pins the
  image and runs a curated backfill from the token's `date_start`; then add it to
  `tokens_whitelist` and rebuild the affected months here. See `docs/workflows/add-token.md`.
- **Repair a month**: DROP the month partition on the three models
  (`dbt run-operation drop_partition`), append it once
  (`--vars '{start_month: M, end_month: M}'`, the append path), then
  `OPTIMIZE … PARTITION 'M' FINAL`. Never a bare `start_month` run over a populated month:
  the append path duplicates it, and `count() - uniqExact(grain)` per month is the check.
- **Price gap**: `scripts/maintenance/refill_after_price_gap.sh`; the priced model is a
  Phase 1 source (`refill_append`).
- **Verify against the chain, not the model**: `eth_call balanceOf(holder)` / `totalSupply()`
  at the day's `anchor_block` from `census_publications` reproduces the census exactly. The
  RPC is available from the indexer pods on GKE.
- **Tests**: `tests/rpc_state_indexer_token_balances_coverage_daily.sql` (curated token-days
  present, holder sum reconciles to `supply_total`, seven-day density per token), the
  schema tests on the three models, and `dq_daily_unpriced_tokens`.

## The transfer-derived chain

`int_execution_tokens_balances_native_daily`, `int_execution_tokens_balances_daily` and
`int_execution_tokens_supply_holders_daily` are frozen and `deprecated` after the 2026-09
cutover (WL-054): kept queryable, never rebuilt, never deleted. Their feeders
`int_execution_transfers_whitelisted_daily` and `int_execution_tokens_address_diffs_daily`
stay in production; the diffs table is a flow table the metrics dashboard reads directly.
