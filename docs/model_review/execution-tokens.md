# Model review: execution/tokens

> **Status (updated 2026-06-22):** triaged against prod `dbt` and acted on. Stale/low-value
> findings removed; implemented items recorded below; only genuinely open items remain.

---

## Scope

Gnosis Chain's authoritative ERC-20 analytics pipeline: raw transfers → per-address cumulative
balances → daily supply / holder counts → cohort distributions → sector attribution → UBO
unwinding. ~30 SQL models over 47 whitelisted tokens (STABLECOIN / OTHERS / RWA + synthetic xDAI),
history from 2020-07-01. Standard `Transfer` events from `execution.logs`; WxDAI decoded
separately (Deposit/Withdrawal as mint/burn); native xDAI via sentinel address.

Canonical definitions: **supply** = sum of *positive* balances excluding the zero address;
**holders** = distinct addresses with `balance > 0`; **volume** = abs transfer amounts × price;
**token_class** from `tokens_whitelist`; aTokens/spTokens whitelisted as OTHERS but excluded from
default runs via `symbol_exclude`.

---

## Resolution log

### Done (implemented this pass)

- **Semantic layer fixed.** `int_execution_tokens_balances_daily` semantic model corrected to the
  real columns (was referencing `from_value_binary`/`chain_id`/`net_delta_raw` etc. from the raw
  `execution.balance_diffs` source — would fail any MCP query); its 4 phantom metrics replaced
  with 2 real ones (`balance`, `balance_usd`); the two `fct_` duplicate semantic models for
  `supply_by_sector` / `supply_distribution` removed (kept the `api_` ones, per convention).
- **Negative-supply detector test added.** `dbt_utils.accepted_range(min_value: 0)` on `supply`
  in `int_execution_tokens_supply_holders_daily` and on `supply` + `supply_usd` in
  `fct_execution_tokens_metrics_daily`. `severity: warn` for now (wstETH is negative until its
  backfill); **flip to `error` once backfilled** so it can't regress. This is a detector, not a
  flooring guard — it surfaces under-ingested tokens rather than hiding them.
- **Phantom `AS` columns removed** from `int_execution_tokens_address_diffs_daily` and
  `int_execution_tokens_transfers_daily` in `intermediate/schema.yml` (described columns that
  don't exist in the SQL).

### Decided — no change

- **Per-wallet balance API access tier:** `api_execution_tokens_balances_daily` kept at `tier1`
  (Partner). It serves address-level balances (the `tier3`/Internal precedent set by the revenue
  per-user endpoints would also fit), but it is intentionally left Partner-accessible; `allow_unfiltered:false`
  + required `symbol`/`address` filter remain the access guardrails.

### Dropped (investigated, no change needed)

- ~~"supply computed without `balance > 0` guard" framed as a definition bug~~ — the negative
  supply is a **symptom of incomplete ingestion**, not a definitional one. Confirmed in prod:
  wstETH balances start 2026-05-01 despite a 2022-06-22 whitelist start; ~4 years of real
  transfers exist in `execution.logs` (23,916 in Jan 2024 alone) but were never backfilled. A
  `balance>0` guard would mask this. Fix is the backfill (open, below) + the detector test (done).
- ~~overview KPI INNER JOIN drops newly-debuted token classes~~ — only bites the week a brand-new
  token *class* appears (effectively never; classes are fixed). Left as-is.
- ~~double `symbol_filter` in transfers / address_diffs~~ — not a true redundancy: the `filters_sql`
  copy scopes the macro's internal per-symbol watermark subquery; the explicit copy scopes the main
  scan. Both do real work; working as intended.
- ~~`api:tokens_supply` / `api:holders_per_token` shared by two models~~ — the intended convention:
  `check_api_tags.py` requires no grain suffix in the `api:` id and exactly one `granularity:` tag,
  so daily vs snapshot variants share the id and are disambiguated by `granularity:`. Non-issue.
- ~~holders `balance_raw>0` vs `balance>0` inconsistency~~ — negligible (sub-dust only). Ignored.
- ~~`top_holders_latest` 7d double-count~~ — `top_holders_ranked` has 0 duplicate `(token,address)`
  rows; the asymmetry is conditional and only affects the `change_usd_7d` delta for dual-holding
  whales. Not changing.

---

## Open items (not owned here)

- **Backfill wstETH (and check the smaller-gap tokens).** wstETH must be rebuilt from its true
  2022-06-22 start through transfers → diffs → balances (the same full-backfill lesson as adding
  any token incrementally). Coverage audit found other, smaller gaps: GBPe (~374d), USDC.e (~75d),
  bIBTA/bIB01/bC3M (~42-50d) — worth backfilling too. **After the backfill, flip the
  negative-supply test severity from `warn` to `error`.** (Owned by the data team.)
- **Consider a coverage test** asserting each token's balances start ≈ its whitelist `date_start`,
  to catch under-ingestion before it manifests as negative supply.
- **Reporting caveats** (documentation, must travel with externally-shown figures): supply is
  sum-of-transfers, not on-chain `totalSupply()` — diverges for vault/rebasing/bridged tokens;
  and OTHERS-class supply excludes Aave/Spark wrapper balances by design (`symbol_exclude`).

---

## Pros (retained for reference)

- Sophisticated incremental architecture tuned around ClickHouse limits (partition cap, OOM bug
  341); each model documents its batch strategy.
- Clear canonical definitions and a documented four-tier price-source hierarchy (Chainlink → RWA/
  aToken wrappers → Dune historical → $1 peg).
- Healthy freshness/grain: max_date at yesterday, 0% NULL `balance_usd` over the last 7 days, clean
  `(date, token_address, address)` grain via ReplacingMergeTree + FINAL + uniqueness tests.
- Ambitious UBO unwinding (Aave V3, SparkLend, Balancer V2, Uniswap V3, Swapr V3, Curve, sDAI
  vault) resolving true end-holders; coverage model quantifies remaining container share.
- Per-wallet balance API blocks unbounded scans (`allow_unfiltered:false`, requires symbol/address).
