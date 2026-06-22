# Model review (revisit 2026-06-21): execution/gpay

Re-verified all `26` cases from the baseline (`docs/model_review/execution-gpay.md`, dated 2026-06-11) over 3 rounds against current code and warehouse: `1` resolved, `4` changed, `21` still confirmed (incl. both critical defects — the direction-collapse spine bug now quantified at `$291,742.20` net silently-dropped USD, and cashback GNO published as USD — both unfixed).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONGPAY-C01 | P0-14 | RMT order_by + unique test omit `direction`; in/out rows collapse, one direction's amount silently dropped | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONGPAY-C02 | | LEFT JOIN to prices lacks `join_use_nulls`; `coalesce(p.price,0)` zeros amount_usd for unpriced tokens | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONGPAY-C03 | | `api_execution_gpay_user_top_wallets` tier1 but zero documented columns; `fct_execution_gpay_users_distinct` absent from schema.yml | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONGPAY-C04 | | roles/delay/spender decode models use `append` + `unique_key` (metadata-only; not enforced) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C05 | | monthly marts stale + activity spine lag; no freshness tests | medium | CHANGED | low | high | none | 3 |
| EXECUTIONGPAY-C06 | | ASOF JOIN right CTE (`cashback_cumulative`) has no explicit ORDER BY; non-deterministic (latent) | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONGPAY-C07 | | GP-to-GP transfer emits both Crypto Withdrawal + Crypto Deposit; double-counts ecosystem volume | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C08 | | `int_execution_gpay_wallets` table-materialized; manual `--var start_month` run leaves incomplete wallet universe, no guard | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C09 | | cumulative window lacks explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGPAY-C10 | | redundant final/subquery ORDER BY (RMT insert_overwrite + flows snapshot view) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGPAY-C11 | | token-whitelist + `action != 'Other'` filters commented out; any whitelisted token flows in | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGPAY-C12 | | `first_inflow` CTE full-scans all 4.4M+ activity rows every run, no incremental filter | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGPAY-C13 | P0-13 | cashback endpoints publish native GNO as 'USD' (~100-300x consumer-facing misstatement) | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONGPAY-C14 | | `api_execution_gpay_total_funded` reads snapshots `PaymentUsers`, surfaces as funded — funnel-stage conflation | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONGPAY-C15 | | churn_rate (current-month base) vs retention_rate (prior-month base) — non-complementary | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONGPAY-C16 | | `balance_usd / total_usd` division unguarded (no greatest/nullIf) | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONGPAY-C17 | | `api_execution_gpay_user_activity` exposes per-wallet tx history at tier0 (should be tier1) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C18 | | retention `initial_users` via `max(users) OVER` not month-0 value; unguarded division (latent) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C19 | | snapshots `change_pct` returns -100% when prev=0 (live for FiatOfframp/Reversal labels) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C20 | | 7D window uses strict `>` on `curr_start = max_date - 7`; earliest day never counted | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C21 | | hardcoded spender `0x4822...172ee` absent from all seed registries; cashback addr hardcoded | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGPAY-C22 | | semantic layer dual-registers `api_` + `fct_` for same metrics; MCP planner can bypass api filtering | low | CHANGED | low | high | none | 3 |
| EXECUTIONGPAY-C23 | | attribution endpoints tag `granularity:rolling_180d` (non-standard, documents fact window not grain) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGPAY-C24 | | data: PaymentUsers -62.3% / CryptoWithdrawal -89.9% 7D drops, not root-caused | low | RESOLVED | resolved | high | none | 3 |
| EXECUTIONGPAY-C25 | | data: ~1,770 funded Safes (~5.4%) have only 2 conversion rows (no delegate identity) | low | CHANGED | low | medium | none | 3 |
| EXECUTIONGPAY-C26 | | data baseline: 4.48M rows / 34,771 wallets / $2.63M balance / GNO-only cashback / 0 RMT dups | low | CHANGED | low | high | none | 3 |

Roll-up: CONFIRMED `21`, RESOLVED `1`, CHANGED `4`, NEW `0`, UNVERIFIABLE/UNRESOLVED `0`.

## Delta vs baseline

### RESOLVED (1)
- **EXECUTIONGPAY-C24** — the baseline `PaymentUsers` 7D drop of `-62.3%` reverted to `+4.6%` (value `4,752`); `CryptoWithdrawal -89.9%` moderated to `-52.8%` (Users) / `-73.0%` (Volume). The underlying `int_execution_gpay_activity_daily` daily series is fully contiguous across `2026-05-25..2026-06-21` (27 consecutive days, no zero/missing day), proving the baseline figure was a rolling-7D window-boundary comparison artifact, not lost data. Incident attribution: **none** (verifier initially tagged `microbatch_insert_overwrite`, self-corrected to none — a wipe causes drops, not the recovery observed).

### CHANGED (4)
- **EXECUTIONGPAY-C05** — staleness fully resolved (spine fresh to today, contiguous; monthly marts are table-materialized last-complete-month by design via `toStartOfMonth(date) < toStartOfMonth(today())`). The residual finding narrowed: `int_execution_gpay_activity_daily` now HAS `elementary.freshness_anomalies` (`intermediate/schema.yml` L190), but the two monthly marts (`fct_execution_gpay_cashback_impact_monthly`, `fct_execution_gpay_churn_monthly`) still have only `elementary.schema_changes` and NO freshness/recency test. Severity dropped medium -> low. Incident: none (forward recovery 3d -> 1d lag, not a wipe).
- **EXECUTIONGPAY-C22** — dual `api_`/`fct_` registration of the same metrics persists (e.g. `execution_gpay_cashback_impact_monthly` refs the api view at `semantic_models.yml` L965, `fct_execution_gpay_cashback_impact_monthly` refs the fct table at L1017, both exposing 10 identical parallel measures L978-1005 vs L1030-1057). The baseline's "MTA attribution / conversions / coverage have no semantic entries" no longer holds: attribution models AND `execution_gpay_conversions` (L2796) + `execution_gpay_coverage_daily` (L2833) are now registered. Surviving issue re-scoped to the dual-registration planner-routing risk. Severity stays low.
- **EXECUTIONGPAY-C25** — pattern persists with materially different magnitude: `1,843` funded Safes have exactly 2 conversion rows (baseline `1,770`), and `18,699` (`37.69%`) of `49,610` funded Safes have no `delegate` identity row. The conversions registry was rebuilt since baseline (now 3 `conversion_kind`s + identity-role fan-out for the June migration), so the baseline `~5.4%` denominator no longer maps cleanly. Severity stays low; confidence medium.
- **EXECUTIONGPAY-C26** — all baseline figures grew with data/refresh: `int_execution_gpay_activity` `4,480,505 -> 4,593,695` rows; distinct wallets `34,771 -> 50,478`; `PaymentUsers` All `34,771 -> 38,572`; `TotalBalance` `~$2.63M -> $3,933,264.43`. Invariants hold: cashback still GNO-only (`['GNO']`), RMT duplicate counts still `0` on both `snapshots` and `actions_by_token_daily`. The baseline coincidence (distinct wallets == PaymentUsers) broke because the universe now has `11,906` non-payment wallets; `38,572` is exactly the payment subset. Incident: none (organic growth + June migration, not a wipe).

### STILL CONFIRMED (21)
- **EXECUTIONGPAY-C01** (critical) — `int_execution_gpay_activity_daily.sql` RMT `order_by=(date,wallet_address,action,symbol)` (L6) and `schema.yml` `unique_combination_of_columns` (L203-207) both omit `direction`, while SELECT+GROUP BY include it. Net silently-dropped USD = **`$291,742.20`** across `372` two-direction keys (both-directions total `$624,282.71`, surviving direction `~$332,540.51`). Baseline had `234` collapse-prone groups; now `372`. Daily spine carries `0` two-direction groups.
- **EXECUTIONGPAY-C13** (critical) — `api_execution_gpay_user_total_cashback.sql` L30 returns `round(toFloat64(sum(amount)),6)` (native GNO) and `api_execution_gpay_user_cashback_daily.sql` L51 returns `round(toFloat64(amount),6)` (native GNO), while `schema.yml` L3809 documents the value as 'The total cashback amount in USD.' Cashback confirmed GNO-only (`['GNO']`, `21,201.31` native). At GNO ~$100-300 this is a ~100-300x consumer-facing misstatement (top wallet `88.09` GNO surfaced as `88.09` "USD" vs true ~$8.8k-$26.4k).
- **EXECUTIONGPAY-C02** (high) — `int_execution_gpay_activity.sql` L160 `coalesce(p.price,0)` with no `join_use_nulls` hook; `47` rows have `amount>0 AND amount_usd=0`, all `GBPe`, `216.67` native GBPe, all genuinely price-NULL. Zeroing propagates to served mart `fct_execution_gpay_actions_by_token_daily` (10 GBPe rows, same `216.67` GBPe at `$0`). Of `4,593,695` total, `22,555` carry `amount_usd=0` (`0.49%`).
- **EXECUTIONGPAY-C03** (high) — `api_execution_gpay_user_top_wallets` (tier1, `api:gpay_user_top_wallets`) has zero documented columns in `marts/schema.yml`; the CI guard passes only because `check_api_tags.allow` L112-113 explicitly allowlist `::columns_missing` + `::no_as_of_date` (suppression, not documentation). `fct_execution_gpay_users_distinct` is not api:-tagged (tags production/execution/gpay/mixpanel) and also absent from schema.yml.
- **EXECUTIONGPAY-C04** (medium) — `int_execution_gpay_roles_events.sql` (L5 append, L8 composite unique_key), `_delay_events.sql` (L4/L8), `_spender_events.sql` (L4/L8) declare `unique_key` on `incremental_strategy='append'` (metadata-only in dbt-clickhouse). Runtime dup excess `0` on all three (RMT already merged) — dedup relies on async merge only.
- **EXECUTIONGPAY-C06** (low) — `fct_execution_gpay_cashback_impact_monthly.sql` `cashback_cumulative` CTE (L37-43) has no explicit final ORDER BY; the ASOF LEFT JOIN (L56-58) requires the right side sorted on `month`. CH does not guarantee CTE row order. Latent (CH ASOF sorts internally today).
- **EXECUTIONGPAY-C07** (medium, upgraded from low) — `int_execution_gpay_activity.sql` (L120-128) emits Crypto Withdrawal for GP sender + Crypto Deposit for GP receiver; **`10,569`** tx_hashes produce BOTH rows (GP wallet each leg). The Round-1/2 "0 / latent" framing was overturned in Round 3 — the cross-wallet double-count is ACTIVE today, not latent. Severity upgraded low -> medium.
- **EXECUTIONGPAY-C08** (medium) — `int_execution_gpay_wallets.sql` `materialized='table'` (L4); `start_month`/`end_month` vars (L13-14) gate `apply_monthly_incremental_filter` (L33). Full table rebuild with no partial-run guard; a manual `--var start_month/end_month` run overwrites the table with a window-bounded wallet universe.
- **EXECUTIONGPAY-C09** (low) — `fct_execution_gpay_actions_by_token_daily.sql` L18-20 and `_weekly.sql` L18-20 use `SUM(...) OVER (PARTITION BY action, token ORDER BY date/week)` with no `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. Correct by CH default today; implicit reliance fragile.
- **EXECUTIONGPAY-C10** (low) — `int_execution_gpay_activity.sql` L166 final `ORDER BY c.wallet_address, c.block_timestamp` on RMT insert_overwrite (pure sort cost); `api_execution_gpay_flows_snapshot.sql` L40 subquery `ORDER BY days ASC` inside `FROM(...) AS sub` not guaranteed to propagate.
- **EXECUTIONGPAY-C11** (low) — `int_execution_gpay_activity.sql` L46 (`--WHERE symbol IN (...)`) and L165 (`--WHERE c.action != 'Other'`) both commented out. Leak is ACTIVE: `23` live distinct symbols (`19` beyond the historical EURe/GBPe/USDC.e/GNO 4; whitelist seed has `44`). `action='Other'` rows still `0`.
- **EXECUTIONGPAY-C12** (low) — `int_execution_gpay_conversions.sql` `first_inflow` CTE (L100-113) full-scans `int_execution_gpay_activity` (`4,593,695` rows) every run; the `{{ this }}` window gate is only on the outer `funded` SELECT (L133-143), not the CTE.
- **EXECUTIONGPAY-C14** (high) — `api_execution_gpay_total_funded.sql` L9-13 reads `fct_execution_gpay_snapshots WHERE label='PaymentUsers' AND window='All'` (`38,572`) and serves it as funded, while canonical `gpay_funded` (`int_execution_gpay_conversions`, first inflow per Safe) = `49,610`. Conflation delta `11,038`. Output column is now `value` (not `funded_addresses`).
- **EXECUTIONGPAY-C15** (high) — `fct_execution_gpay_churn_monthly.sql` `churn_rate` (L76, L149) uses current-month `total_active`; `retention_rate` (L77-79, L150-152) uses `lagInFrame(total_active,1)` prior-month base. Non-complementary: sums range `76.3%` to `102.7%` across recent months (largest divergence 2026-01 at `102.7%`).
- **EXECUTIONGPAY-C16** (low, downgraded from medium) — `api_execution_gpay_wallet_balance_composition.sql` L24 `balance_usd / t.total_usd >= 0.01` has no greatest/nullIf guard, but the `latest` CTE (L10-15) prefilters `balance_usd>0`. On `2026-06-21`: `15` positive rows, `0` negatives, `total_usd=$6,853,711.35` -> division unreachable, not merely improbable.
- **EXECUTIONGPAY-C17** (medium) — `api_execution_gpay_user_activity.sql` L4 tagged `tier0`; exposes `transaction_hash, wallet_address, block_timestamp, date, action, symbol, direction, amount, amount_usd, counterparty` (L62-72). Gated by `allow_unfiltered:false` + `require_any_of:[wallet_address]` but per-user financial history at tier0 remains a policy concern.
- **EXECUTIONGPAY-C18** (medium) — three retention models compute `initial_users` via `max(users) OVER (PARTITION BY cohort_month)` (e.g. `fct_execution_gpay_retention_monthly.sql` L45) with no nullIf/greatest guard (L56). Latent: across `29` Payment cohorts, `max(users)==month-0` in 100% (0 mismatches), 0 missing month-0.
- **EXECUTIONGPAY-C19** (medium) — `fct_execution_gpay_snapshots.sql` `change_pct = (coalesce(curr/nullIf(prev,0),0)-1)*100` (L120 etc) returns `-100%` when prev=0. Live 7D: `FiatOfframpVolume`/`Count`/`Users` all `-100` (nonzero current values `1524.87`/`2`/`2`, prev=0); Reversal labels `-31.5`/`-42.4`/`-56.4` are real (prev>0).
- **EXECUTIONGPAY-C20** (medium) — `fct_execution_gpay_snapshots.sql` bounds CTE (L15-23) `curr_start = subtractDays(max_date,7)`; filter (L34) `d.date > b.curr_start AND d.date <= b.curr_end`. Strict `>` excludes `max_date-7` -> 6-full-day + partial window. `prev_7d` (L47) has the same off-by-one (internally consistent).
- **EXECUTIONGPAY-C21** (medium) — spender `0x4822521e6135cd2599199c83ea35179229a172ee` hardcoded in `int_execution_gpay_activity.sql` L29, `int_execution_gpay_wallets.sql` L15, `fct_execution_gpay_payments_hourly.sql` L57; absent from all 3 seed registries (0 grep hits). `rpc_get_code` confirmed it is a live deployed contract (171 bytes, has_code=1, is_eip1167=0). Cashback `0xcdf50be9061086e2ecfe6e4a1bf9164d43568eec` hardcoded at L30 (registered only in dao_treasury_wallets.csv).
- **EXECUTIONGPAY-C23** (low) — `api_execution_gpay_attribution_30d/60d/7d.sql` L4 all tag `granularity:rolling_180d` — a non-standard value documenting the fact window not the output grain. Project-wide grep: `daily(151)`, `monthly(47)`, `weekly(43)`, `all_time(22)` dominate; `rolling_180d(12)` is the outlier.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None.

## Evidence appendix

**C01** (direction collapse, net dropped USD):
```sql
WITH src AS (SELECT toDate(block_timestamp) date,wallet_address,action,symbol,direction,sum(amount_usd) usd
            FROM dbt.int_execution_gpay_activity GROUP BY 1,2,3,4,5),
     keys AS (SELECT date,wallet_address,action,symbol FROM src GROUP BY 1,2,3,4 HAVING countDistinct(direction)>1),
     src2 AS (SELECT s.* FROM src s JOIN keys k USING(date,wallet_address,action,symbol)),
     daily AS (SELECT date,wallet_address,action,symbol,direction surviving_dir
               FROM dbt.int_execution_gpay_activity_daily d JOIN keys k USING(date,wallet_address,action,symbol))
SELECT count(),sum(s2.usd) FROM daily dl JOIN src2 s2
  ON dl.date=s2.date AND dl.wallet_address=s2.wallet_address AND dl.action=s2.action
     AND dl.symbol=s2.symbol AND dl.surviving_dir!=s2.direction;
```
Returned: `372` two-direction keys; both-directions USD `$624,282.71` (744 src dir-rows); `372` surviving daily rows; net dropped-direction USD = `$291,742.20`. `order_by`/`unique_combination_of_columns` omit `direction`.

**C02** (coalesce price-zero):
```sql
SELECT countIf(amount_usd=0), countIf(amount_usd=0 AND amount>0), count() FROM dbt.int_execution_gpay_activity;
-- served mart
SELECT countIf(token='GBPe' AND volume>0 AND (volume_usd=0 OR volume_usd IS NULL)) FROM dbt.fct_execution_gpay_actions_by_token_daily;
```
Returned: `22,555` zero-usd of `4,593,695` (`0.49%`); `47` with amount>0, all `GBPe` (price NULL), `216.67` native; mart shows `10` GBPe rows, `216.67` GBPe at `$0`. Code L160 `coalesce(p.price,0)`, no `join_use_nulls`.

**C03** (allowlist suppression): `check_api_tags.allow` L112-113 = `api_execution_gpay_user_top_wallets::columns_missing` + `::no_as_of_date`. Model `api_execution_gpay_user_top_wallets.sql` L1-6 tags `['production','execution','gpay','api:gpay_user_top_wallets','granularity:snapshot','tier1']`. grep `marts/schema.yml` for `user_top_wallets` / `users_distinct` = `0` matches. `fct_execution_gpay_users_distinct.sql` tags include `mixpanel`, no `api:` tag.

**C04** (append + unique_key): code-only — `roles_events` L5 append / L8 composite unique_key; `delay_events`/`spender_events` L4 append / L8 unique_key. Runtime: `GROUP BY <key> HAVING count()>1` excess = `0` / `0` / `0`.

**C05** (freshness):
```sql
SELECT max(date),min(date),count() FROM dbt.int_execution_gpay_activity_daily;
```
Spine max `2026-06-21`, contiguous `2026-05-25..2026-06-21`. `int_execution_gpay_activity_daily` HAS `elementary.freshness_anomalies` (intermediate/schema.yml L190); `fct_execution_gpay_cashback_impact_monthly` and `fct_execution_gpay_churn_monthly` model-level tests = only `elementary.schema_changes` (no freshness).

**C06/C09/C10** (code-only): C06 `cashback_cumulative` CTE L37-43 no final ORDER BY, ASOF JOIN L56-58. C09 `SUM(...) OVER (... ORDER BY date/week)` no ROWS frame (daily/weekly L18-20). C10 activity L166 final ORDER BY, flows_snapshot L40 subquery ORDER BY.

**C07** (active double-count):
```sql
SELECT count() FROM (SELECT transaction_hash FROM dbt.int_execution_gpay_activity
  WHERE action IN ('Crypto Withdrawal','Crypto Deposit') GROUP BY transaction_hash
  HAVING countDistinct(action)=2
     AND countDistinctIf(wallet_address,action='Crypto Withdrawal')>0
     AND countDistinctIf(wallet_address,action='Crypto Deposit')>0);
```
Returned: `10,569` tx_hashes producing both legs.

**C08** (code-only): `int_execution_gpay_wallets.sql` L4 `materialized='table'`; L29-34 start/end_month branch; L33 `apply_monthly_incremental_filter`; no partial-run guard.

**C11** (whitelist leak):
```sql
SELECT groupUniqArray(symbol), uniqExact(symbol) FROM dbt.int_execution_gpay_activity;
```
Returned: `23` distinct symbols (WBTC, SAFE, COW, WETH, sDAI, USDT, wstETH, ZCHF, BRLA, aGno*/sp* etc. beyond the historical 4); seed `tokens_whitelist.csv` has `44`. Code L46/L165 filters commented; `action='Other'` = `0`.

**C12** (code-only): `int_execution_gpay_conversions.sql` `first_inflow` CTE L100-113 reads `int_execution_gpay_activity WHERE action IN ('Fiat Top Up','Crypto Deposit')` with no incremental filter; `{{ this }}` gate at L133-143 is on the outer SELECT.

**C13** (cashback GNO-as-USD):
```sql
SELECT groupUniqArray(symbol), sum(amount), count() FROM dbt.int_execution_gpay_activity_daily WHERE action='Cashback';
```
Returned: `['GNO']` only, `21,201.31` GNO native. Endpoints L30 `sum(amount)` / L51 `round(toFloat64(amount),6)` (native); `schema.yml` L3809 'in USD'.

**C14** (funnel conflation):
```sql
SELECT value FROM dbt.fct_execution_gpay_snapshots WHERE label='PaymentUsers' AND window='All';
SELECT uniqExact(gp_safe) FROM dbt.int_execution_gpay_conversions WHERE conversion_kind='gpay_funded';
```
Returned: `38,572` (PaymentUsers) vs `49,610` (canonical funded); delta `11,038`.

**C15** (non-complementary rates): code L76/L149 (current-month base) vs L77-79/L150-152 (lagInFrame prior-month). Per-month sums: 2026-01 `102.7`, 2026-02 `96.7`, 2026-03 `99.9`, 2026-04 `99.9`, 2026-05 `76.3`.

**C16** (unguarded division):
```sql
SELECT countIf(balance_usd>0),countIf(balance_usd<0),sumIf(balance_usd,balance_usd>0)
FROM dbt.fct_execution_gpay_balances_by_token_daily WHERE date=(SELECT max(date) FROM dbt.fct_execution_gpay_balances_by_token_daily);
```
Returned (2026-06-21): `15` positive, `0` negative, `total_usd=$6,853,711.35`. Code L24 unguarded; `latest` CTE L10-15 prefilters `balance_usd>0` -> unreachable.

**C17** (code-only): `api_execution_gpay_user_activity.sql` L4 `tier0`; L62-72 exposes tx_hash/amounts/counterparty; gated `allow_unfiltered:false` + `require_any_of:[wallet_address]`.

**C18** (latent retention base):
```sql
-- per Payment cohort: maxIf(users,months_since=0) vs max(users)
SELECT count(),countIf(mx!=month0),countIf(month0=0) FROM (
  SELECT cohort_month, maxIf(users,ms=0) month0, max(users) mx FROM coh GROUP BY cohort_month) agg;
```
Returned: `29` cohorts, `0` with `max!=month-0`, `0` missing month-0. Code L45 `max(users) OVER`, L56 division no guard.

**C19** (-100% at prev=0):
```sql
SELECT label,window,value,change_pct FROM dbt.fct_execution_gpay_snapshots WHERE window='7D' AND (label LIKE 'FiatOfframp%' OR label LIKE 'Reversal%');
```
Returned: FiatOfframp Count/Users/Volume all `-100`; Reversal Volume `-56.4`, Count `-31.5`, Users `-42.4`. Formula L120 `(coalesce(curr/nullIf(prev,0),0)-1)*100`.

**C20** (code-only): bounds CTE L15-23 `curr_start = subtractDays(max_date,7)`; curr_7d L34 `d.date > b.curr_start AND d.date <= b.curr_end`; prev_7d L47 same.

**C21** (hardcoded address): grep spender `0x4822...172ee` in `int_execution_gpay_activity.sql` L29, `int_execution_gpay_wallets.sql` L15, `fct_execution_gpay_payments_hourly.sql` L57; `0` matches in contracts_whitelist.csv / dao_treasury_wallets.csv / contracts_factory_registry.csv. `rpc_get_code` -> has_code=1, code_size=171, is_eip1167=0.

**C22** (dual registration): `semantic_models.yml` `execution_gpay_cashback_impact_monthly` L965 `model: ref('api_execution_gpay_cashback_impact_monthly')` AND `fct_execution_gpay_cashback_impact_monthly` L1017 `model: ref('fct_execution_gpay_cashback_impact_monthly')`; identical measure sets L978-1005 vs L1030-1057. `execution_gpay_conversions` L2796, `execution_gpay_coverage_daily` L2833 now registered.

**C23** (granularity tag): `api_execution_gpay_attribution_30d/60d/7d.sql` L4 `granularity:rolling_180d`. Project grep: daily 151, latest 103, monthly 47, weekly 43, all_time 22, snapshot 20, rolling_180d 12.

**C24** (resolved artifact):
```sql
SELECT label,window,value,change_pct FROM dbt.fct_execution_gpay_snapshots WHERE window='7D' AND label='PaymentUsers';
SELECT date,uniqExact(wallet_address) FROM dbt.int_execution_gpay_activity_daily WHERE date>=toDate('2026-05-25') GROUP BY date ORDER BY date;
```
Returned: PaymentUsers 7D `+4.6` (value `4,752`); daily series contiguous `2026-05-25..2026-06-21`, no zero/missing day.

**C25** (no-delegate Safes):
```sql
WITH per_safe AS (SELECT gp_safe,count() n_rows,
  countDistinctIf(identity_role,'delegate'=identity_role) has_delegate
  FROM dbt.int_execution_gpay_conversions WHERE conversion_kind='gpay_funded' GROUP BY gp_safe)
SELECT count(),countIf(has_delegate=0),countIf(n_rows=2) FROM per_safe;
```
Returned: `49,610` funded Safes; `18,699` (`37.69%`) no-delegate; `1,843` exactly-2-row.

**C26** (data baseline):
```sql
SELECT count(),uniqExact(wallet_address) FROM dbt.int_execution_gpay_activity_daily;
SELECT count() FROM dbt.int_execution_gpay_activity;
SELECT label,window,value FROM dbt.fct_execution_gpay_snapshots WHERE (label='PaymentUsers' OR label='TotalBalance') AND window='All';
```
Returned: tx-grain `4,593,695` (was `4,480,505`); distinct wallets `50,478` (was `34,771`); PaymentUsers `38,572` (was `34,771`); TotalBalance `$3,933,264.43` (was `~$2.63M`); cashback `['GNO']`; RMT dup `0` on snapshots + actions_by_token_daily.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED (372 src groups vs 0 daily) -> challenge: quantify net dropped USD -> R2 answered ($624,282.71 both-directions across 1,174 rows) -> challenge: single auditable net figure -> R3 net dropped USD = `$291,742.20`. Final CONFIRMED/critical.
- **C02**: R1 CONFIRMED (22,555 zero-usd, 47 positive) -> challenge: isolate true price-miss vs real zeros -> R2 47 all GBPe price-NULL -> challenge: show served-mart blast -> R3 propagates to 10 mart rows (216.67 GBPe @ $0). Final CONFIRMED/high.
- **C03**: R1 CHANGED (guard now passes) -> challenge: WHY does it pass (allowlist?) -> R2 CONFIRMED (allowlist L112-113, users_distinct not api-tagged) -> challenge: confirm consumer-reachable tier1 -> R3 tier1 + no schema.yml columns. Final CONFIRMED/high.
- **C04**: R1 CONFIRMED (code) -> challenge: runtime dup evidence -> R2 dup excess 0 all three -> R3 re-confirmed code-only. Final CONFIRMED/medium.
- **C05**: R1 CHANGED (spine recovered; attributed microbatch) -> challenge: recovery != wipe, justify or relabel -> R2 relabeled to none -> challenge: confirm no recency test anywhere -> R3 narrowed to two monthly marts (spine HAS freshness test). Final CHANGED/low.
- **C06**: R1 CONFIRMED (no ORDER BY) -> R2 reasoned latent (ASOF sorts internally) -> R3 re-confirmed code-only. Final CONFIRMED/low.
- **C07**: R1 CHANGED (0 GP-to-GP, latent) -> challenge: verify format parity of the 0 -> R2 parity verified, still 0 -> challenge: check tx_hashes with both legs historically -> R3 found `10,569` active dual-leg tx (overturns latent). Final CONFIRMED/medium (upgraded).
- **C08**: R1 CONFIRMED (code) -> challenge: confirm prod path is no-op -> R2 confirmed (table -> is_incremental False; risk only manual --var) -> R3 re-confirmed. Final CONFIRMED/medium.
- **C09**: R1 CONFIRMED (no ROWS frame) -> challenge: show implicit frame correct today -> R2 reasoned (one row/partition -> RANGE==ROWS) -> R3 re-confirmed. Final CONFIRMED/low.
- **C10**: R1 CONFIRMED -> challenge: check flows ORDER BY propagation -> R2 confirmed unspecified output ordering -> R3 re-confirmed. Final CONFIRMED/low.
- **C11**: R1 CONFIRMED (filters commented, Other=0) -> challenge: enumerate live symbols vs whitelist -> R2 code-level confirmed -> challenge: data comparison -> R3 `23` live symbols (19 beyond historical 4). Final CONFIRMED/low.
- **C12**: R1 CONFIRMED (full scan) -> challenge: query-profile evidence -> R2 code-definitive (no predicate) -> R3 re-confirmed. Final CONFIRMED/low.
- **C13**: R1 CONFIRMED (native GNO, GNO-only) -> challenge: quantify consumer dollar figures -> R2 top wallet 88.09 GNO ~$8.8k-$26.4k -> R3 re-confirmed (21,201.31 GNO native). Final CONFIRMED/critical.
- **C14**: R1 CONFIRMED (PaymentUsers as funded) -> challenge: numeric conflation delta -> R2 38,572 vs 49,610 (delta 11,038) -> R3 re-confirmed. Final CONFIRMED/high.
- **C15**: R1 CONFIRMED (denominators) -> challenge: show non-complementary sums -> R2 sums 76.3-102.7% -> R3 re-confirmed both scopes. Final CONFIRMED/high.
- **C16**: R1 CONFIRMED (unguarded) -> challenge: prove unreachable vs improbable -> R2 reasoned (>0 prefilter) -> challenge: column semantics -> R3 data confirms 15 pos / 0 neg / $6.85M -> unreachable. Final CONFIRMED/low.
- **C17**: R1 CONFIRMED (tier0) -> challenge: access-policy reality -> R2 require_any_of reduces bulk risk but still tier1-shaped -> R3 re-confirmed. Final CONFIRMED/medium.
- **C18**: R1 CONFIRMED (max not month-0; 0 violating) -> challenge: check all 3 models + missing-m0 mode -> R2 0/0 across all three -> R3 re-confirmed (29 cohorts, 0 mismatch). Final CONFIRMED/medium.
- **C19**: R1 CONFIRMED (-100% FiatOfframp) -> challenge: classify fresh vs went-to-zero -> R2 nonzero current + prev=0 (fresh-uptick) -> R3 re-confirmed live. Final CONFIRMED/medium.
- **C20**: R1 CONFIRMED (strict >) -> challenge: quantify dropped day + prev consistency -> R2 confirmed internally consistent off-by-one -> R3 re-confirmed. Final CONFIRMED/medium.
- **C21**: R1 CONFIRMED (hardcoded, absent from seeds) -> challenge: rpc verify live contract -> R2 rpc_get_code 171-byte live contract -> R3 re-confirmed (now in 3 SQL files). Final CONFIRMED/medium.
- **C22**: R1 CHANGED (attribution gained entries; conversions/coverage absent) -> challenge: prove planner routing concretely -> R2 dual cashback_impact instance (but wrongly said conversions/coverage absent) -> challenge: correct the delta (both now registered) -> R3 corrected; dual-registration risk proven (L965 vs L1017). Final CHANGED/low.
- **C23**: R1 CONFIRMED (rolling_180d) -> challenge: is it the lone outlier -> R2 grep distribution (rolling_180d 12, outlier) -> R3 re-confirmed. Final CONFIRMED/low.
- **C24**: R1 CHANGED (reverted +4.6%; attributed microbatch) -> challenge: drop attribution, show daily contiguity -> R2 RESOLVED, attribution none, spine contiguous -> challenge: show baseline-window daily present -> R3 contiguous 27 days, artifact confirmed. Final RESOLVED.
- **C25**: R1 CONFIRMED (1,843 two-row) -> challenge: verify the 2 rows lack delegate row -> R2 1,746 two-row all no-delegate -> challenge/rebuild context -> R3 CHANGED (registry rebuilt: 1,843 two-row, 18,699/37.69% no-delegate). Final CHANGED/low.
- **C26**: R1 CHANGED (numbers grew; attributed microbatch) -> challenge: relabel attribution + reconcile wallets!=PaymentUsers -> R2 attribution none, 38,572 is payment subset of 50,478 -> R3 re-confirmed (invariants GNO-only + 0 dups hold). Final CHANGED/low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE) | Add `direction` to the RMT `order_by` AND the `unique_combination_of_columns` test; or aggregate both directions before write. This is a six-figure (`$291,742.20`) silent USD loss on the central spine inherited by 20+ marts. | `models/execution/gpay/intermediate/int_execution_gpay_activity_daily.sql` + `marts/schema.yml` |
| P0 (ESCALATE) | Either convert cashback to USD (`amount_usd`) at the endpoint, or rename the column/description to GNO and stop labeling it USD. ~100-300x consumer-facing misstatement. | `marts/api_execution_gpay_user_total_cashback.sql`, `marts/api_execution_gpay_user_cashback_daily.sql`, `marts/schema.yml` (L3809) |
| P1 (KEEP) | Replace `coalesce(p.price,0)` with a `join_use_nulls` pre/post hook so unpriced tokens are NULL not $0 (currently 47 GBPe rows / `216.67` GBPe zeroed into a served mart). | `intermediate/int_execution_gpay_activity.sql` |
| P1 (KEEP) | Document the tier1 endpoint columns in schema.yml and REMOVE the `check_api_tags.allow` suppression (L112-113); document `fct_execution_gpay_users_distinct`. | `marts/api_execution_gpay_user_top_wallets.sql`, `scripts/checks/check_api_tags.allow`, `marts/schema.yml` |
| P1 (KEEP) | Re-source `api_execution_gpay_total_funded` from canonical `gpay_funded` (`int_execution_gpay_conversions`), not snapshots `PaymentUsers` — currently understates funded by `11,038` Safes. | `marts/api_execution_gpay_total_funded.sql` |
| P1 (KEEP) | Make `churn_rate` and `retention_rate` use a consistent (prior-month) denominator so they are complementary (sums vary `76.3-102.7%`). | `marts/fct_execution_gpay_churn_monthly.sql` |
| P2 (KEEP) | Decide and document the GP-to-GP transfer convention; exclude one leg from ecosystem-volume aggregates (`10,569` tx actively double-counted). | `intermediate/int_execution_gpay_activity.sql` |
| P2 (KEEP) | Fix the 7D window start bound (`>` -> `>=` on `curr_start = max_date-7`) for a true rolling 7-calendar-day window. | `marts/fct_execution_gpay_snapshots.sql` |
| P2 (KEEP) | Return NULL (not `-100%`) when `prev=0` in `change_pct` (live -100 on all FiatOfframp 7D labels). | `marts/fct_execution_gpay_snapshots.sql` |
| P2 (KEEP) | Move the hardcoded spender `0x4822...172ee` into a seed registry + dbt var (currently in 3 SQL files, absent from all seeds; a migration silently stops Payment classification). | `intermediate/int_execution_gpay_activity.sql`, `intermediate/int_execution_gpay_wallets.sql`, `marts/fct_execution_gpay_payments_hourly.sql` |
| P2 (KEEP) | Promote `api_execution_gpay_user_activity` to tier1 (per-user financial tx history). | `marts/api_execution_gpay_user_activity.sql` |
| P3 (KEEP) | Compute retention `initial_users` from `minIf(users, months_since=0)` (not `max OVER`) and add a division guard (latent: 0/29 cohorts violate today). | `marts/fct_execution_gpay_retention_monthly.sql`, `_retention_by_action_monthly.sql`, `_cashback_cohort_retention_monthly.sql` |
| P3 (KEEP) | Add freshness/recency tests to the two monthly marts (spine already covered). | `marts/fct_execution_gpay_churn_monthly.sql`, `marts/fct_execution_gpay_cashback_impact_monthly.sql` |
| P3 (KEEP) | Add explicit ORDER BY to the `cashback_cumulative` CTE; add explicit `ROWS BETWEEN` window frame; drop redundant ORDER BYs; re-enable token-whitelist / `action != 'Other'` filters; convert `append`->`delete+insert` or document the RMT-only dedup; add a partial-run guard or default-window to `int_execution_gpay_wallets`. | C06, C09, C10, C11, C04, C08 models |
| P3 (KEEP) | Disambiguate the dual `api_`/`fct_` semantic registration so the MCP planner routes to the api view. | `semantic/authoring/execution/gpay/semantic_models.yml` |
| P4 (KEEP) | Normalize `granularity:rolling_180d` to the output grain convention. | `marts/api_execution_gpay_attribution_30d/60d/7d.sql` |
| - (DROP) | Drop the C24 anomaly investigation — PaymentUsers reverted to `+4.6%`; the baseline `-62.3%` was a rolling-window boundary artifact on a contiguous daily series. | `marts/fct_execution_gpay_snapshots.sql` |
