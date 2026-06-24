# Model review (revisit 2026-06-21): execution/yields

Re-verification of the `execution/yields` baseline (`docs/model_review/execution-yields.md`, dated `2026-06-11`) across 3 rounds: **17 cases re-verified** — `2 RESOLVED`, `2 CHANGED`, `13 STILL CONFIRMED`, `0 NEW`; the critical `least()` epoch-coercion bug (`C01`) persists and now hits `23,416/24,614` wallets, while the two stale-data defects (`C02` lending positions, `C16` SparkLend coverage) recovered after the incident reprocessing.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONYIELDS-C01 | P0-19 | unmatched LEFT JOIN -> non-null epoch `1970-01-01` in `first_yield_date` (non-nullable `min()` agg; coalesce is a no-op) | critical | CONFIRMED (fixed + verified in playground 2026-06-23, pending prod full-refresh) | critical | high | none | 3 |
| EXECUTIONYIELDS-C02 | P0-19 | `active_lending_positions = 0` for all wallets (stale table) | critical | RESOLVED | resolved | high | other (table refresh) | 3 |
| EXECUTIONYIELDS-C03 | P0-08 | grain omits `token_address`; RMT collapses multi-token LP legs | high | CONFIRMED (fixed + verified in playground 2026-06-23, pending prod full-refresh) | high | high | none | 3 |
| EXECUTIONYIELDS-C04 | — | overview snapshot forward-references CTE `lending_tvl_latest_date` | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONYIELDS-C05 | P0-07 | approved measures reference nonexistent `apy_7DMA`/`apy_30DMA` cols | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONYIELDS-C06 | — | `as_of_date` derived from Swapr Algebra events not source marts | medium | CONFIRMED | low | medium | none | 3 |
| EXECUTIONYIELDS-C07 | — | `daily_rate` schema desc (day-over-day) contradicts 7-day geom slope | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONYIELDS-C08 | — | same-day collect-minus-burn netting can zero legit fee claims | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONYIELDS-C09 | — | `lending_balances_daily` missing `as_of_date` vs peer views | medium | CHANGED | low | high | none | 3 |
| EXECUTIONYIELDS-C10 | — | `apply_monthly_incremental_filter` unguarded vs siblings | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONYIELDS-C11 | — | 7 overview cards share single `api:yields_overview` tag | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONYIELDS-C12 | — | Balancer V2 profit-as-fee proxy mislabels exit/IL PnL as fees | high | RESOLVED (real fees implemented + verified in playground 2026-06-23) | resolved | high | logs_ingestion_gap | 3 |
| EXECUTIONYIELDS-C13 | — | 7 user marts emit plaintext wallets, tier1, no privacy tag/MCP gate | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONYIELDS-C14 | — | TVL threshold mismatch: portfolio `>0.01` vs overview `>0` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONYIELDS-C15 | — | sDAI supply card keyed on `symbol='SDAI'` (USDS regime-flip risk) | medium | CONFIRMED | medium | medium | none | 3 |
| EXECUTIONYIELDS-C16 | — | SparkLend in activity feed but absent from positions/APY join | medium | RESOLVED | resolved | high | none | 3 |
| EXECUTIONYIELDS-C17 | — | opportunities silently excludes quiet pools (NULL `fee_apr_7d`) | low | CONFIRMED | low | high | none | 3 |

Roll-up: `confirmed=12`, `resolved=3`, `changed=1`, `new=0`, `unverifiable=0`, `unresolved=0`. (Update 2026-06-23: C12 moved CHANGED -> RESOLVED — real Balancer fees implemented; C01/C03/C05 fixes applied + verified in playground.)

## Delta vs baseline

### RESOLVED (2)

- **C02** — `active_lending_positions = 0` across all `6,055` wallets at baseline is fixed: now `19,709` of `24,614` wallets carry positions (`sum=20,306`, `max=9`), and that count reconciles `1:1` with distinct users having `balance_usd > 0.01` on the latest balances date. The defect was a stale `materialized='table'` build that ran before `int_execution_lending_aave_user_balances_daily` was populated. **Incident attribution corrected**: NOT `microbatch_insert_overwrite` (the model is `table`-materialized — no `REPLACE PARTITION`); it was an ordinary build-order/staleness issue resolved by a plain table refresh once the upstream was contiguous. Durability confirmed across the last 3 build dates (`19,709 / 19,703 / 19,025`).
- **C16** — SparkLend coverage asymmetry closed. SparkLend now appears in BOTH surfaces: `3,157` distinct wallets / `28,590` rows in the activity feed `int_execution_yields_user_activity` (unified via `UNION ALL` of `contracts_aaveV3_PoolInstance_events` + `contracts_spark_Pool_events`), AND `935` rows in `fct_execution_yields_user_lending_positions_latest` with `891/935` (`95.3%`) carrying non-NULL joined `supply_apy` (`max 23.41%`). The activity-feed count exceeding the open-positions count is expected (lifetime log vs current positions), not a re-opening.

### CHANGED (2)

- **C12** — Balancer V2 profit-as-fee proxy still fires (`int_execution_yields_user_lp_positions.sql` lines `~96-99`, `greatest(capital_out_usd - capital_in_usd, 0)` for non-V3 null-tick / no-active-token positions), but **magnitude collapsed from `$35.9M` / `1,931` positions to `$2.57M` / `313` positions** (still `~76%` of total LP fees `$3,383,380`). Code is byte-for-byte unchanged — the drop reflects **incident-B (`logs_ingestion_gap`) reprocessing of `int_execution_pools_dex_liquidity_events`** that re-priced `capital_in_usd`/`capital_out_usd`, so fewer positions satisfy `capital_out > capital_in`. Three sampled proxy positions confirm the formula still fires (`962585.30 - 641986.93 = 320598.37`, all `is_active=0`). Mislabel pattern persists at materially smaller amount → severity `high → medium`.
- **C09** — `api_execution_yields_user_lending_balances_daily` does lack `as_of_date`, but the baseline framing ("present in **all** other user-facing API views") is **inaccurate**: `3 of 7` user views lack `as_of_date` (`activity`, `lending_balances_daily`, `fee_collections_daily`), `4 of 7` have it (`lp_positions`, `lending_positions`, `kpis`, `top_wallets`). The three omitters are all daily/event-stream-grain views that carry a native date column — a granularity-pattern split, not a singleton omission → status `CONFIRMED → CHANGED`, severity `medium → low`.

### STILL CONFIRMED (13)

- **C01 (critical)** — `least(lp.first_lp_date, ll.first_lending_date)` at `fct_execution_yields_user_lifetime_metrics.sql` lines `55/57` still has **no coalesce guard**. `23,416` of `24,614` wallets (`95%`) carry `first_yield_date = 1970-01-01` on the deployed consumer view `api_execution_yields_user_kpis`. Round-3 refinement: epoch wallets carry an **inflated `~20,625`-day tenure** (`dateDiff('day', 1970-01-01, today())`), not a zeroed one (`tenure0=0`). Both-active wallets (`1,042`) all carry valid non-epoch dates (`nonepoch_both=1,042` exact), proving the epoch rows are precisely the single-NULL-arg (LP-only / lending-only) wallets. Worse than baseline (`6,055` → `23,416` epoch rows, wallet count grew after the lending recovery).
- **C03 (high)** — ORDER BY key at model line 6 still `(block_timestamp, source, transaction_hash, log_index)` — `token_address` omitted. **Realized collapse demonstrated**: tx `cadf0e76...` log_index `21` has `8` distinct tokens / `8` rows upstream in `int_execution_pools_dex_liquidity_events` but only `1` row / `1` token in `int_execution_yields_user_activity` (both FINAL and non-FINAL — permanent storage loss). A repo-wide FINAL scan found ZERO surviving `(source, tx, log_index)` groups with `>1` token. `115,301` colliding upstream groups exist in 2026.
- **C04 (high)** — Forward reference persists: `lending_tvl_latest_date` referenced at source lines `93/102` but defined at line `149` (compiled: ref `85/94`, def `141` — CH did not reorder). Isolated to this one model (grep found it nowhere else). All 7 metrics still materialize; CH resolves lazily; no `.sqlfluff`/forward-ref lint exists to catch it. Latent portability trap.
- **C05 (high)** — Approved-tier measures `yields_sdai_apy_7dma_value` (`expr: apy_7DMA`) and `yields_sdai_apy_30dma_value` (`expr: apy_30DMA`) at `semantic_models.yml` lines `1152-1157` reference columns absent from `fct_yields_sdai_apy_daily`, whose deployed columns are `(date, apy, label)` only (windows are long-format `label` rows). Metrics `yields_sdai_apy_7dma` (line `1352`) / `yields_sdai_apy_30dma` (line `1377`) are registered and `quality_tier: approved` (reachable). Runtime exercise blocked by `manifest_hash_mismatch`; column-absence is dispositive — any MCP query fails with unknown-column.
- **C06 (low, was medium)** — `as_of_date = (SELECT toDate(max(block_timestamp)) FROM contracts_Swapr_v3_AlgebraPool_events)` at `api_execution_yields_opportunities_latest.sql` line 8, not from source marts. Currently Swapr is the FRESHEST source (`2026-06-21`, `lag_days = -1` vs `fct_execution_pools_daily` `2026-06-20`), so not misleading today (baseline was `2026-06-08` and lagging). Latent/structural design risk → severity lowered `medium → low`.
- **C07 (low, was medium)** — `intermediate/schema.yml` line `143` still describes `daily_rate` as `(share_price_t / share_price_t_minus_1) - 1` while the model computes `pow(share_price / window_start_price, 1/7) - 1`. Isolated: canonical docs and the model-level description are correct, and the semantic `daily_rate_value` measure has NO description field, so the wrong text does not surface to MCP consumers → doc-hygiene-only, `medium → low`.
- **C08 (medium)** — `greatest(coalesce(sumIf(amount_usd,'collect'),0) - coalesce(sumIf(amount_usd,'burn'),0), 0)` at `fct_execution_yields_user_fee_collections_daily.sql` lines `21-25`, still undocumented. Quantified: `4,616` overlap groups net away `$1,569,308,135.76` of collect, `77` fully-zeroed groups erase `$2,891,122.74`. Gross `$1.57B` overstates the defect (V3 Collect-after-Burn legitimately claims principal moved to owed); the clean defect is the fully-zeroed groups. Holds at medium.
- **C10 (low)** — `apply_monthly_incremental_filter('block_timestamp','date','true')` at `int_yields_savings_xdai_rate_daily.sql` line `58` still lacks the `{% if not (start_month and end_month) %}` guard used by `insert_overwrite` siblings. Harmless today: model is `materialized='table'`, macro no-ops via `is_incremental()` (compiled SQL has 0 month filters). Convention-only.
- **C11 (low)** — All 7 overview marts share the single `api:yields_overview` tag; `api_execution_yields_opportunities_latest` also carries it (`8` nodes total — a cross-namespace mis-tag). `check_api_tags.py` `multi_api` rule (lines `69-70`) is per-node (`len(api) > 1`), so each single-tagged node passes CI. Baseline undercounted as 6 cards.
- **C13 (high)** — All 7 `api_execution_yields_user_*` marts emit plaintext `wallet_address`/`user_address`, tagged `tier1`, with NO `privacy:tier_*` tag and NO `expose_to_mcp` override. `scaffold_metrics.py _node_is_gated` (lines `132-140`) gates only on `expose_to_mcp == False` or `tags ∩ {internal_only, privacy:tier_internal}` — `tier1` is not a gating tag, so all are MCP-exposed by default. "Any caller can look up any wallet" confirmed at the config/gating layer (live discovery call not run; static proof dispositive).
- **C14 (medium)** — Portfolio uses `balance_usd > 0.01` (`fct_execution_yields_user_lifetime_metrics.sql` line `37`) while overview lenders use `balance > 0` (snapshot line `95`) and lending_tvl uses `balance_usd > 0` (lines `153/161`). Distinct-user gap `32,511` (overview native) vs `19,709` (portfolio usd) `= 12,802`, **entirely dust-band driven**: `native_no_usd = 0`, dust band (`0 < balance_usd <= 0.01`) `= 13,063` users. Not a native-vs-USD unit mismatch.
- **C15 (medium)** — `sdai_supply` at `fct_execution_yields_overview_snapshot.sql` line `137` still keys on `upper(symbol) = 'SDAI'`. Currently returns `$56,805,799.19` (nonzero, `max_date 2026-06-13`); the token has carried symbol `'sDAI'` continuously (`2023-09-22 → 2026-06-19`, `997` day-rows) and never appeared as USDS/sUSDS. Latent: a future relabel post the `2025-11-07` regime flip would silently zero the card. Address-keying would be robust.
- **C17 (low)** — Quiet-pool exclusion (LP inclusion requires non-null `fee_apr_7d`, line `151`) still present and undocumented; opportunities holds `11` LP / `12` lending rows (`max LP APR 77.40%`, `max lending APY 23.41%`). On the latest date `2` of `13` pools are dropped, totaling only `$41,081` TVL (largest `$22,372`) vs `$3,514,774` included (`1.2%`) — trivially small pools, no quiet-but-large pool hidden. By-design, low.

### NEW (0)

None.

### UNVERIFIABLE / UNRESOLVED (0)

None — all 17 cases reached a settled status with query-backed evidence over 3 rounds.

## Evidence appendix

**C01** — `SELECT count() AS n, countIf(toYear(first_yield_date)=1970) AS epoch_n, min(first_yield_date), max(first_yield_date), min(tenure_days), max(tenure_days), countIf(tenure_days=0) AS tenure0 FROM dbt.api_execution_yields_user_kpis` → `n=24,614; epoch_n=23,416; min(first_yield_date)=1970-01-01; max=2026-06-13; min(tenure_days)=8; max(tenure_days)=20,625; tenure0=0`. Reconciliation: both-active wallets `=1,042 = nonepoch_both=1,042` (exact). Code lines `55/57` `least(lp.first_lp_date, ll.first_lending_date)` unchanged.

**C02** — `SELECT sum(active_lending_positions), max(active_lending_positions), countIf(active_lending_positions>0), count() FROM dbt.fct_execution_yields_user_lifetime_metrics` → `sum=20,306; max=9; >0 count=19,709; total=24,614`. `SELECT date, countDistinct(user_address) FROM dbt.int_execution_lending_aave_user_balances_daily WHERE date>=today()-5 AND balance_usd>0.01 GROUP BY date ORDER BY date DESC` → `19,709 / 19,703 / 19,025` (latest 3 dates; latest matches `19,709` exactly).

**C03** — `SELECT transaction_hash, log_index, uniqExact(token_address), count() FROM dbt.int_execution_pools_dex_liquidity_events WHERE protocol='Balancer V2' AND provider!='' GROUP BY 1,2 HAVING uniqExact(token_address)>1` → tx `cadf0e76...` log_index `21`: `8` distinct tokens / `8` rows. `SELECT count(), uniqExact(token_address) FROM dbt.int_execution_yields_user_activity FINAL WHERE transaction_hash='cadf0e76...' AND log_index=21` → `1` row / `1` token (FINAL and non-FINAL). 2026 colliding groups `=115,301`.

**C04** — `code_only` + compiled grep: `lending_tvl_latest_date` referenced at source lines `93,102`, defined `149`; compiled `target/compiled/.../fct_execution_yields_overview_snapshot.sql` ref `85,94`, def `141`. grep across `models/`: name appears in exactly 1 file.

**C05** — `describe_table fct_yields_sdai_apy_daily` → columns `= (date, apy Nullable(Float64), label)`. `semantic_models.yml` lines `1152-1157`: `yields_sdai_apy_7dma_value (expr: apy_7DMA)`, `yields_sdai_apy_30dma_value (expr: apy_30DMA)`; metrics at lines `1352/1377`, `quality_tier: approved`. `reload_semantic_registry` → `execution_available=false, stale_reason=manifest_hash_mismatch`.

**C06** — `toDate(max(block_timestamp))` from `contracts_Swapr_v3_AlgebraPool_events` `=2026-06-21`; `max(date)` `fct_execution_pools_daily` `=2026-06-20`; `lag_days=-1` (Swapr fresher). Line 8 unchanged.

**C07** — `code_only`: `intermediate/schema.yml` line `143` `(share_price_t / share_price_t_minus_1) - 1`; model lines `29-31/110` `pow(share_price/window_start_price, 1/7)-1`; semantic `daily_rate_value` measure (lines `1263-1265`) has no description field. grep of `daily_rate` across `docs/`/`semantic/`: canonical docs correct.

**C08** — Per `(date,provider,pool_address)` over `int_execution_pools_dex_liquidity_events`: `4,616` groups with both positive collect AND burn; `sumIf(least(burn,collect)) = $1,569,308,135.76` netted; `77` fully-zeroed groups erasing `$2,891,122.74`; burn>collect groups have collect `=$0` (no real loss). Code lines `21-25` unchanged, no comment.

**C09** — `as_of_date` occurrence count across 7 user marts: `activity=0, lp_positions=1, lending_positions=1, kpis=1, top_wallets=1, lending_balances_daily=0, fee_collections_daily=0` → `4` have it, `3` lack it. DESCRIBE confirms `api_execution_yields_user_lending_balances_daily` deployed columns `=(date, user_address, reserve_address, symbol, balance, balance_usd)` (no `as_of_date`).

**C10** — `code_only`: line `58` bare macro call, `materialized='table'` (line 3); `get_incremental_filter.sql` line `24` gates on `is_incremental()`; compiled artifact has `0` `toStartOfMonth`/`incremental_end_date`/WHERE-month filters. Sibling guards: `int_execution_yields_user_activity.sql` lines `39-41/95-97`, `fct_execution_yields_user_fee_collections_daily.sql` lines `29-31` (both `insert_overwrite` incremental).

**C11** — `grep -rln 'api:yields_overview' models/` → `8` nodes: 7 overview cards (`lending_best_apy, lending_lenders, lending_tvl, lp_best_apr, lp_tvl, sdai_apy, sdai_supply`) + `api_execution_yields_opportunities_latest`. `check_api_tags.py` lines `69-70`: `if len(api) > 1: fail('multi_api')` — per-node.

**C12** — `SELECT countIf(protocol='Balancer V2' AND tick_lower IS NULL AND fees_collected_usd>0), round(sumIf(fees_collected_usd, protocol='Balancer V2' AND tick_lower IS NULL AND fees_collected_usd>0),0), round(sum(fees_collected_usd),0) FROM dbt.int_execution_yields_user_lp_positions` → `313` positions, `$2,568,272` proxy fees, total `$3,383,380`. 3 sampled positions: `962585.30-641986.93=320598.37`, `335731.77-16864.00=318867.77`, `1126293.95-900442.32=225851.63`, all `is_active=0`. Upstream Balancer V2 mint/burn events `=1,592,574`, `max_ts 2026-06-20`.

**C13** — grep of config blocks: all 7 `api_execution_yields_user_*` marts carry `tier1` + `api:*`, NONE carry `privacy:tier_*` or `expose_to_mcp`. `scaffold_metrics.py _node_is_gated` lines `132-140`: gates only on `expose_to_mcp==False` OR `tags ∩ INTERNAL_TAGS={internal_only, privacy:tier_internal}`.

**C14** — `WITH ld AS (SELECT max(date) m FROM dbt.int_execution_lending_aave_user_balances_daily WHERE date<today()) SELECT countDistinctIf(user_address,balance>0), countDistinctIf(user_address,balance_usd>0.01), countDistinctIf(user_address,balance_usd>0 AND balance_usd<=0.01), countDistinctIf(user_address,balance>0 AND (balance_usd IS NULL OR balance_usd<=0)) FROM ... WHERE b.date=ld.m` → `native(>0)=32,511; usd(>0.01)=19,709; dust(0..0.01)=13,063; native_no_usd=0`. Gap `=12,802`.

**C15** — `SELECT upper(symbol), argMax(supply,date), max(date) FROM dbt.fct_execution_tokens_metrics_daily WHERE upper(symbol)='SDAI' AND date<today() GROUP BY 1` → `latest_supply=56,805,799; max_date=2026-06-13`. Symbol-history scan: only `'sDAI'` (and `WxDAI`) present, `2023-09-22 → 2026-06-19`, never USDS/sUSDS. Code line `137` unchanged.

**C16** — `SELECT countDistinct(wallet_address), count() FROM dbt.int_execution_yields_user_activity WHERE protocol='SparkLend' AND source='lending'` → `3,157` distinct / `28,590` rows. `SELECT protocol, count(), countIf(supply_apy IS NOT NULL) FROM fct_execution_yields_user_lending_positions_latest GROUP BY protocol` → SparkLend `935` rows, `891` non-NULL apy (`95.3%`, `max 23.41%`).

**C17** — `WITH md AS (SELECT max(date) m FROM dbt.fct_execution_pools_daily WHERE date<today()) SELECT countDistinct(pool_address), countDistinctIf(pool_address,fee_apr_7d IS NULL), round(sumIf(tvl_usd,fee_apr_7d IS NULL),0), round(maxIf(tvl_usd,fee_apr_7d IS NULL),0), round(sumIf(tvl_usd,fee_apr_7d IS NOT NULL),0) FROM ... WHERE p.date=md.m` → `13` distinct pools, `2` NULL `fee_apr_7d` excluded, excluded TVL `$41,081` (max `$22,372`), included TVL `$3,514,774`. Opportunities: `11` LP / `12` lending rows.

## Review log (>=3 rounds per case)

- **C01** — R1 CONFIRMED critical (`23,416/24,614` epoch, code unfixed) → challenge: prove epoch rows are exactly single-NULL-arg wallets + propagation to KPI view → R2 reconciled (`both=nonepoch_both=1,042` exact; SELECT * propagation) → R3 challenge: query deployed `api_execution_yields_user_kpis` directly + tenure → R3 CONFIRMED critical, refined: epoch wallets carry inflated `~20,625`-day tenure, not zeroed.
- **C02** — R1 RESOLVED (`19,709` wallets, attribution `microbatch_insert_overwrite`) → challenge: reconcile per-wallet + correct attribution → R2 RESOLVED, attribution corrected to plain table-refresh (`19,709=19,709` exact) → R3 challenge: durability across 3 dates + plausible max → R3 RESOLVED (`19,709/19,703/19,025` stable, `max=9`).
- **C03** — R1 CHANGED medium (`dup_excess=0`, NULL-symbol dropped to `0.08%`) → challenge: prove latent risk real → R2 CONFIRMED high (`115,301` colliding upstream groups in 2026) → R3 challenge: demonstrate realized downstream collapse → R3 CONFIRMED high (8 upstream legs → 1 FINAL row for tx `cadf0e76`).
- **C04** — R1 CONFIRMED high → challenge: portability/compile + lint → R2 CONFIRMED high (compiled preserves forward ref, no lint) → R3 challenge: blast radius → R3 CONFIRMED high (isolated to 1 file).
- **C05** — R1 CONFIRMED high → challenge: exercise runtime failure → R2 CONFIRMED high (metrics layer blocked by `manifest_hash_mismatch`, column-absence proof) → R3 challenge: confirm metrics registered/reachable → R3 CONFIRMED high (registered + approved at lines `1352/1377`).
- **C06** — R1 CONFIRMED medium → challenge: quantify current lag → R2 CONFIRMED, lowered to low (Swapr `lag 0/-1`) → R3 challenge: historical-lag teeth → R3 CONFIRMED low (current `lag=-1`, latent design risk).
- **C07** — R1 CONFIRMED medium → challenge: confirm not propagated to consumer docs → R2 CONFIRMED medium (isolated to schema.yml line `143`) → R3 challenge: semantic-consumer surface → R3 CONFIRMED, lowered to low (semantic measure has no description field).
- **C08** — R1 CONFIRMED medium → challenge: quantify understatement → R2 CONFIRMED medium (`4,616` groups, `$1.57B` netted, `77` zeroed / `$2.89M`) → R3 challenge: isolate V3 fully-zeroed → R3 CONFIRMED medium (netting unchanged, fully-zeroed framing accepted).
- **C09** — R1 CONFIRMED medium → challenge: confirm observable on live API → R2 CONFIRMED medium (DESCRIBE confirms absent) → R3 challenge: completeness across all 7 views → R3 CHANGED, lowered to low (`3 of 7` lack it, granularity-pattern split).
- **C10** — R1 CONFIRMED low → challenge: verify macro no-ops → R2 CONFIRMED low (macro gates on `is_incremental()`, 0 compiled filters) → R3 challenge: sibling-parity → R3 CONFIRMED low (siblings are incremental; table-mat omission cosmetic).
- **C11** — R1 CONFIRMED low (corrected 6→7 cards) → challenge: re-list all nodes, classify opportunities_latest → R2 CONFIRMED low (`8` nodes, cross-namespace mis-tag) → R3 challenge: CI behavior → R3 CONFIRMED low (`multi_api` is per-node, passes).
- **C12** — R1 CHANGED high (`$35.9M`→`$2.57M`) → challenge: explain mechanism/audit by pool → R2 CHANGED, lowered to medium (incident-B reprocessing, code unchanged) → R3 challenge: prove inputs changed not position set → R3 CHANGED medium (3 sampled positions, upstream `1,592,574` events).
- **C13** — R1 CONFIRMED high → challenge: confirm MCP-reachable by default → R2 CONFIRMED high (`tier1` not a gating tag) → R3 challenge: end-to-end discovery exposure → R3 CONFIRMED high (static gating proof dispositive; discovery call not run).
- **C14** — R1 CONFIRMED medium → challenge: quantify dust band → R2 CONFIRMED medium (`13,063` dust users) → R3 challenge: native-vs-USD split → R3 CONFIRMED medium (gap `12,802` all dust-driven, `native_no_usd=0`).
- **C15** — R1 CONFIRMED medium → challenge: check historical symbol relabel → R2 CONFIRMED medium (never flipped, `997` day-rows of `sDAI`) → R3 challenge: address-vs-symbol stability → R3 CONFIRMED medium (symbol resolves, address cross-check beyond budget; latent holds).
- **C16** — R1 RESOLVED → challenge: prove JOIN yields non-NULL APY for SparkLend → R2 RESOLVED (`891/935` non-NULL apy) → R3 challenge: activity-feed parity → R3 RESOLVED (`3,157` activity users + `935` positions, both populated).
- **C17** — R1 CONFIRMED low → challenge: prove quiet pools dropped → R2 CONFIRMED low (`4 of 26` NULL `fee_apr_7d`) → R3 challenge: materiality of dropped pools → R3 CONFIRMED low (`2 of 13`, `$41k` excluded vs `$3.5M`).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (KEEP/ESCALATE) | **CORRECTED 2026-06-23 — a plain `coalesce` here is a NO-OP (see re-verification addendum).** The unmatched arg is epoch `1970-01-01` (non-null, `isNull()=0`), not NULL, so `coalesce`-wrapped args never fire. Setting-independent fix: convert the sentinel first — `least(nullIf(lp.first_lp_date, toDateTime64('1970-01-01 00:00:00',0)), nullIf(ll.first_lending_date, toDate('1970-01-01')))` then `coalesce`, and cast to a common type (`first_lp_date` is `DateTime64(0)`, `first_lending_date` is `Date`). `first_yield_date`/`tenure_days` wrong for `23,864/25,083` (95.1%) wallets; propagates to the deployed KPI view. Full-refresh after fix. | `models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql`, `models/execution/yields/marts/api_execution_yields_user_kpis.sql` |
| P0 (KEEP/ESCALATE) | Add `privacy:tier_*` tag and/or `expose_to_mcp: false` to the 7 plaintext-wallet user marts; they are MCP-exposed by default — any caller can look up any wallet's full history. | `api_execution_yields_user_{activity,lp_positions,lending_positions,kpis,top_wallets,lending_balances_daily,fee_collections_daily}.sql` |
| P1 (KEEP) | Add `token_address` to the ReplacingMergeTree ORDER BY key; multi-token Balancer V2 legs are being permanently collapsed (`8`→`1` realized; `115,301` colliding groups in 2026). Backfill after fix. | `models/execution/yields/intermediate/int_execution_yields_user_activity.sql`, `intermediate/schema.yml` |
| P1 (KEEP) | Fix the broken approved-tier semantic measures: either add `apy_7DMA`/`apy_30DMA` columns to the model or rewrite the measures to filter `label='7DMA'/'30DMA'` over the long-format `apy`. Any MCP query of these metrics fails at runtime. | `semantic/authoring/execution/yields/semantic_models.yml`, `models/execution/yields/marts/fct_yields_sdai_apy_daily.sql` |
| P1 (KEEP) | Reorder CTE `lending_tvl_latest_date` before its first reference; the forward reference is a non-portable maintenance trap (lazy-resolved only by CH today). | `models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` |
| DONE 2026-06-23 | ~~Restrict/relabel the Balancer V2 profit-as-fee proxy~~ — RESOLVED: proxy removed, replaced with real event-derived swap fees attributed by contribution share (see "C12 — replaced with real Balancer fees"). | `int_execution_yields_user_lp_positions.sql`, `int_execution_yields_balancer_lp_fees.sql`, `int_execution_pools_fees_daily.sql`, `contracts/BalancerV2/*` |
| P2 (KEEP) | Document or refine the same-day collect-minus-burn netting; `77` groups fully zeroed erasing `$2.89M` of fee claims. | `models/execution/yields/marts/fct_execution_yields_user_fee_collections_daily.sql` |
| P2 (KEEP) | Align TVL/lender thresholds between portfolio (`>0.01`) and overview (`>0`); `~12,802` dust-band lenders fail to reconcile across surfaces. | `marts/fct_execution_yields_user_lifetime_metrics.sql`, `marts/fct_execution_yields_overview_snapshot.sql` |
| P2 (KEEP) | Key the sDAI supply card on the vault/token address, not `symbol='SDAI'`, to survive a USDS/sUSDS relabel (latent since `2025-11-07`). | `models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` |
| P3 (KEEP) | Derive `as_of_date` from the actual source marts (`fct_execution_pools_daily`/`int_execution_lending_aave_daily`), not Swapr Algebra events. | `models/execution/yields/marts/api_execution_yields_opportunities_latest.sql` |
| P3 (KEEP) | Fix the `daily_rate` column description (day-over-day → 7-day geometric slope). | `models/execution/yields/intermediate/schema.yml` |
| P3 (KEEP) | Give distinct `api:` tags per overview card and remove `api:yields_overview` from `opportunities_latest` (cross-namespace mis-tag). | the 7 `api_execution_yields_overview_*` marts + `api_execution_yields_opportunities_latest.sql` |
| P3 (KEEP) | Add the `{% if not (start_month and end_month) %}` guard for sibling parity (latent footgun only). | `models/execution/yields/intermediate/int_yields_savings_xdai_rate_daily.sql` |
| P3 (KEEP) | Document the quiet-pool exclusion (LP requires non-null `fee_apr_7d`); low user impact today but undocumented. | `models/execution/yields/marts/fct_execution_yields_opportunities_latest.sql` |
| P3 (KEEP) | Add `as_of_date` to the 3 omitting daily/event-stream user views for consistency (downgraded — granularity-pattern split, not a singleton). | `api_execution_yields_user_{activity,lending_balances_daily,fee_collections_daily}.sql` |
| — (DROP) | ~~Refresh stale `active_lending_positions=0`~~ — RESOLVED: `19,709` wallets now carry positions, reconciling `1:1` with upstream. | `fct_execution_yields_user_lifetime_metrics.sql` |
| — (DROP) | ~~Unify SparkLend across activity and positions/APY~~ — RESOLVED: SparkLend present in both (`3,157` activity users, `935` positions, `95.3%` non-NULL apy). | `int_execution_yields_user_activity.sql`, `fct_execution_yields_user_lending_positions_latest.sql` |

---

## Re-verification 2026-06-23 (independent, vs prod `dbt`)

Independent re-run of the four load-bearing must-fix checks against prod `dbt`. All four confirmed; figures refreshed (the warehouse moved again since the 2026-06-21 pass); one **fix correction** that applies to both this file and the baseline.

### Refreshed figures

| case | 2026-06-21 | 2026-06-23 (verified) |
|---|---|---|
| C01 epoch wallets | 23,416 / 24,614 (95%) | **23,864 / 25,083 (95.14%)**, max tenure 20,627d |
| C01 reconciliation | both-active 1,042 | in_both **1,219**; lp_only **4,888** + lending_only **18,976** = 23,864 epoch (exact) |
| C03 leg collapse | 8→1 on tx `cadf0e76`; 115,301 colliding groups (Balancer V2, 2026) | repo-wide LP (all protocols): **2,516,209 source legs → 2,024,540 kept = 491,669 (~19.5%) dropped**; every `(tx,log_index)` collapsed to exactly 1 token |
| C05 broken measures | `apy_7DMA`/`apy_30DMA` absent | confirmed — table cols are `(date, apy, label)` only; measures at `semantic_models.yml` 1152-1157 feed **approved** metrics 1352/1377 |
| C12 Balancer proxy | $2.57M / 313 (~76%) | **$2,745,733 / 321 / 81.1%** of $3,383,787 total LP fees |

### Fix correction — C01 (critical)

The fix recommended in this file (and the baseline) — wrapping `least()` args in `coalesce(...)` — is a **no-op**. Verified directly: on an unmatched LEFT JOIN row the `min(date)`/`min(entry_date)` aggregate resolves to epoch `1970-01-01` with `isNull() = 0` (non-null), because the aggregate column is non-nullable. `coalesce` only catches NULL, so `coalesce(epoch, fallback) = epoch` and `least(valid, epoch) = epoch` — the bug survives a verbatim apply.

- The "(no coalesce)" / "`least(date, NULL)`" framing is the wrong mechanism: the arg is **not** NULL.
- Whether a NULL ever appears at build time depends on the build's `join_use_nulls` setting, which **could not be tested here** (the MCP query guard blocks the `SETTINGS` keyword). The materialized output column is non-nullable `DateTime`, so the stored value alone cannot distinguish "NULL coerced to epoch on store" from "epoch produced directly."
- **Setting-independent fix:** convert the sentinel explicitly with `nullIf(date, <epoch>)` before `coalesce`/`least`, using type-correct sentinels (`first_lp_date` `DateTime64(0)`, `first_lending_date` `Date`). Works regardless of `join_use_nulls`. (The "SET `join_use_nulls=1`" alternative only works if that setting reliably nulls the non-nullable aggregate — unverified here, so prefer `nullIf`.)

### Verification limits

- `join_use_nulls=1` behavior and any `SETTINGS`-based fix could not be exercised (MCP guard blocks `SETTINGS` / `SYSTEM`). The `nullIf` fix sidesteps this.
- C04, C06–C11, C13, C15, C17 were not independently re-run on 2026-06-23 — they were code/config-static or low-severity and unchanged from the 2026-06-21 pass.

### Code fixes applied + verified in playground (2026-06-23)

Two fully-verified, decision-free correctness fixes applied to the model source, then rebuilt and verified in `playground_max` (NOT yet deployed to prod — both require a `--full-refresh` of already-materialized tables, a prod `dbt` operation):

- **C01** — `fct_execution_yields_user_lifetime_metrics.sql`: introduced a `joined` CTE that converts the epoch sentinel to NULL per column (`nullIf(first_lp_date, toDateTime64('1970-01-01 00:00:00',0))`, `toDateTime64(nullIf(first_lending_date, toDate('1970-01-01')),0)` to reconcile the `DateTime64(0)` vs `Date` mismatch), then `least(coalesce(a,b), coalesce(b,a))`. Output type becomes `Nullable(DateTime64(0))` (semantically correct; 0 nulls in practice). **Verified in playground after rebuild**: `epoch = 0` (was 23,864), `null = 0`, max tenure `1,289` days (was 20,627), dates `2022-12-12 -> 2026-05-10`; fix propagates clean to `api_execution_yields_user_kpis`.
- **C03** — `int_execution_yields_user_activity.sql`: added `token_address` to the ReplacingMergeTree `order_by`; `intermediate/schema.yml`: added `token_address` to the `unique_combination_of_columns` grain test. **Verified in playground after full-refresh**: activity LP rows `= 2,882,679 =` source legs (every token leg retained; ~644,830 previously-dropped legs recovered, ~22%); full-grain uniqueness `3,501,586 = 3,501,586`, 0 dupes -> grain test passes with `--vars '{test_full_refresh: true}'`.
- **C03 prerequisite (latent bug fixed in the same model)** — `int_execution_yields_user_activity` plumbed `start_month`/`end_month` vars but never applied them to any WHERE clause (the `{% if not (start_month and end_month) %}` guard only *disabled* the incremental macro), so a batched full-refresh scanned all history and OOM'd at the 10.8 GiB ceiling. Added real whole-month window pruning (`toStartOfMonth(block_timestamp)` bounds, matching the partition key) on both the LP and lending branches, plus a `meta.full_refresh` block (`start_date 2022-12-01`, `batch_months 3`) so `scripts/full_refresh/refresh.py` walks it in slices. Continuous (non-windowed) runs are unchanged. NOTE: full-refreshes of this model must now go through `refresh.py`, not a plain `dbt run --full-refresh` (which passes no vars and re-OOMs).

Not applied (need a decision or further verification, deferred to the next pass): C05 (redefine vs drop the broken measures), C12 (rename proxy to `estimated_pnl_usd` + flag), C13 (privacy tagging policy), C14 (canonical active-lender threshold — align with the lending section), C04/C06/C07/C11/C15/C17 doc/structure items.

### C12 — replaced with real Balancer fees (2026-06-23)

The baseline recommendation (rename the `capital_out - capital_in` proxy to `estimated_pnl_usd` + flag) was superseded by a full fix: Balancer V2/V3 LP fees are now computed from real on-chain swap fees, entirely in dbt. The proxy is gone; `fees_collected_usd` for Balancer positions is genuine attributed swap fees.

Key realization that unblocked this: the V2 Vault `Swap` event carries volume only (no fee), but each pool emits `SwapFeePercentageChanged(uint256)` (inherited from BasePool) — and decoding is dbt-side off `execution.logs` via the `event_signatures` seed. So no rpc-caller / re-indexing was needed.

New infrastructure (all dbt):
- `seeds/event_signatures.csv` — 2 rows for the reference pool `0xdd4393...91ef7`: `SwapFeePercentageChanged(uint256)` (topic0 `a9ba3ffe...322dafc`) and `ProtocolFeePercentageCacheUpdated(uint256,uint256)` (topic0 `6bfb6895...11959a`). topic0s verified by reproducing the V3 seed entry via keccak.
- `models/contracts/BalancerV2/contracts_BalancerV2_pool_registry.sql` — all V2 pools -> `abi_source_address` reference (shared-ABI pattern, so 2 signatures cover every pool type).
- `models/contracts/BalancerV2/contracts_BalancerV2_Pool_events.sql` — `decode_logs` over the registry, `event_name_filter` on the two events (microbatch + `meta.full_refresh`).
- `int_execution_pools_fees_daily` — Balancer V2 branch: ASOF-join decoded fee history to V2 `Swap` events (`fee_ppm = swapFeePercentage / 1e12`), mirroring the Swapr V3 dynamic-fee CTE.
- `int_execution_yields_balancer_lp_fees.sql` — monthly contribution-based, value-weighted attribution of pool fees to LPs, NET of the Balancer protocol-fee cut (feeType 0: 0% before ~2023-03, 50% after, from `ProtocolFeePercentageCacheUpdated`, applied ASOF). Attributes to the Join/Exit `provider`.
- `int_execution_yields_user_lp_positions` — Balancer branch of `fees_collected_usd` now reads the attributed fees (proxy removed).

Verification (playground, all history):
- Decoder: 1,131 `SwapFeePercentageChanged` across 1,106 pools; fee band min 0.0001% / max 10% (exact Balancer floor/cap); 1,106/1,114 pools covered (8 fall back to 0).
- Pool fees: blended V2 0.098% / V3 0.069% on $2.72B / $175M volume — sane swap-fee economics.
- Attribution: gross conserves to pool fees (97.66%; 100% per-pool); after the protocol-fee cut, net-to-LP total is $1.40M (50.3% of $2.79M gross). Reference pool `0xdd4393...` (fully in the 50% era): net LP fees $0.248M vs gross $0.50M vs old PnL proxy $2.26M.
- Propagation: `fct_execution_yields_user_lifetime_metrics` rebuilt — 0 epoch (C01 intact), total LP fees $2.74M ($1.40M Balancer net + $1.34M V3).

Open tails (non-blocking): 879 pools (~2.3% of fees) have fees but no tracked `provider` (gauge custody / LPs outside our liquidity events) so their fees aren't credited to a wallet; 8 pools lack a decoded fee event (default 0).

Prod deployment runbook (team lead; prod `dbt`, in dependency order):
1. `dbt seed --select event_signatures`
2. `dbt run --select contracts_BalancerV2_pool_registry`
3. `python scripts/full_refresh/refresh.py --select contracts_BalancerV2_Pool_events --inprocess`
4. `python scripts/full_refresh/refresh.py --select int_execution_pools_fees_daily --inprocess`
5. `dbt run --select int_execution_yields_balancer_lp_fees int_execution_yields_user_lp_positions`
6. `dbt run --select fct_execution_yields_user_lifetime_metrics api_execution_yields_user_kpis api_execution_yields_user_top_wallets`
7. (optional) rebuild `models/execution/pools/marts` so `fct_execution_pools_daily` reflects V2 fees.

### Baseline file

`docs/model_review/execution-yields.md` (2026-06-11) still carries the superseded figures ($35.9M, 6,055 rows, "all wallets", C02/C16 open) and the same flawed coalesce fix. It is a dated snapshot superseded by this revisit; left unedited — flag if you want it annotated too.
