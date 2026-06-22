# Model review (revisit 2026-06-21): execution/gnosis_app

Baseline `docs/model_review/execution-gnosis_app.md` (dated `2026-06-11`); `28` baseline cases + `1` new case re-verified over `3` rounds. Headline: the two critical/high blockers from the batch-vars truncation are **resolved** (onboard anchor restored to `24,020` rows over 8 months; retention/churn marts now measure the full base), but swap-fee revenue is **still `$0` across all `44,904` filled trades (critical)**, and a new partition-staleness issue (`346` GNO claims mis-priced at `$0`) surfaced during re-verification — net: `5` resolved, `4` changed, `17` still confirmed, `1` new.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | conf | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONGNOSISAPP-C01 | P0-12 | onboard anchor truncated to 2 months (2,477 rows) | critical | RESOLVED | resolved | high | batch-vars trunc | 3 |
| EXECUTIONGNOSISAPP-C02 | P0-12 | swap fee revenue `$0` on all filled trades | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONGNOSISAPP-C03 |  | identity bridge drops ~828 users | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONGNOSISAPP-C04 |  | retention_pct_latest returns 0.0 not NULL | high | CHANGED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C05 |  | delete+insert + any(block_number) + stale freshness | high | CHANGED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C06 |  | 248 token_offer_claims priced `$0` (registry gap) | high | CHANGED | high | high | other | 3 |
| EXECUTIONGNOSISAPP-C07 | P0-12 | cohort/retention/churn marts measure wrong population | high | RESOLVED | resolved | high | batch-vars trunc | 3 |
| EXECUTIONGNOSISAPP-C08 |  | lagInFrame no explicit ROWS frame | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C09 |  | coverage mixes countDistinct + count() estimators | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C10 |  | toFloat64(NULL)->0.0 price collapse | medium | CHANGED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C11 |  | gpay_volume double-casts via toString | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C12 |  | any(offer_price_in_crc) non-deterministic | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C13 |  | attribution as_of_date proxied from gpay_topups | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C14 |  | foundation tables lack uniqueness/not_null tests | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C15 |  | ~60+ api models allowlisted from columns/grain checks | medium | CHANGED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C16 |  | api views expose partial today() rows | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONGNOSISAPP-C17 |  | swaps fill-rate has no anomaly test | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C18 |  | most 7d KPIs missing window: tag | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C19 |  | churn_retention_complementary test warns on bootstrap | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C20 |  | user_activity_daily table-mat + inert unique_key | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C21 |  | retention denominator = peak-activity not cohort size | high | RESOLVED | resolved | high | none | 3 |
| EXECUTIONGNOSISAPP-C22 |  | high-confidence threshold drifts >=2 vs >=3 | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONGNOSISAPP-C23 |  | MTA event-kind seed mismatch | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C24 |  | marketplace_buy carries NULL USD pipeline-wide | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C25 |  | topup volume implausibly low (11 rows / 5 users) | medium | RESOLVED | resolved | high | none | 3 |
| EXECUTIONGNOSISAPP-C26 |  | daily vs weekly returning-user definitions inconsistent | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONGNOSISAPP-C27 |  | funnel_daily emits level=0 rows | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONGNOSISAPP-C28 |  | semantic layer covers only 2 of ~12 surfaces | low | RESOLVED | resolved | medium | none | 3 |
| EXECUTIONGNOSISAPP-N01 |  | 2026-05 token_offer_claims partition not reprocessed | — | NEW | medium | high | other | 1 |

## Delta vs baseline

### RESOLVED (5)
- **C01** — onboard anchor restored: `int_execution_gnosis_app_user_activity_daily` `activity_kind='onboard'` is now `24,020` rows over `8` contiguous months (`2025-11-12`..`2026-06-19`), equal to `users_current` (`24,020`), vs baseline `2,477` rows over a 2-month window (~11%). Root cause re-attributed to the **persisted `start_month`/`end_month` table-mat batch-vars footgun** (compiled artifact's `toStartOfMonth >= '2026-05-01'` filter is gone; vars NULL), **not** the June `insert_overwrite`/REPLACE-PARTITION wipe.
- **C07** — downstream of C01: `fct_execution_gnosis_app_retention_monthly` now has `7` distinct cohorts (was `1`) and `fct_execution_gnosis_app_churn_monthly` spans `7` months (was 1, all-zero bootstrap). Reconciled end-to-end against the foundation in two places: earliest cohort `2025-11` size `4,320` = onboard count; `2026-05` churn `Any` total_active `6,677` = non-onboard distinct actives. Same batch-vars attribution.
- **C21** — retention denominator fixed: `fct_execution_gnosis_app_retention_monthly.sql` `cohort_size` CTE (L49-59) now uses `count(DISTINCT address)` of onboard rows; the old `max(users) OVER (PARTITION BY cohort_month)` is gone. Every M0 = `100.0`, oldest cohort decays monotonically (`2025-11`: M0=`100.0` -> M1=`76.3`); `0` rows >100%, `0` upper-triangle rows.
- **C25** — topup volume now plausible: `int_execution_gnosis_app_gpay_topups` is `49,451` rows / `1,081` distinct users (`2025-11-13`..`2026-06-20`), vs baseline `11` rows / `5` users. Redefined off the `gpay_wallets` bridge; `1,081` topup users <= `1,173` GA-owned pay_wallets <= `1,240` bridge wallets <= `24,020` GA users, so it is provably bounded, not over-counted.
- **C28** — semantic coverage expanded: `semantic/authoring/execution/gnosis_app/semantic_models.yml` now has `125` semantic_model blocks (was `2` models / `5` metrics), with swaps/topups/retention/churn/attribution each authored with real `model:` refs. Marked resolved at medium confidence only because the runtime registry was non-executable this round (`manifest_hash_mismatch`), so live queryability could not be exercised — a deploy-state issue, not a coverage gap.

### CHANGED (4)
- **C04** — bootstrap `0.0` symptom gone: `api_execution_gnosis_app_kpi_retention_pct_latest` now returns `23.6` (the `2026-04` cohort's M1). The unguarded `anyIf` remains in code, but `retention_pct` is `Nullable(Float64)`, so an unmatched filter returns NULL — the baseline's claimed `0.0` failure mode does **not** reproduce. Downgraded high -> low.
- **C05** — freshness sub-claim resolved (swaps max `block_timestamp` = `2026-06-20`, ~1 day behind, was `2026-06-01`/10 days). Two code sub-claims persist: `incremental_strategy='delete+insert'` (L7) and `any(block_number)` for `first_fill_block` (L70). Determinism risk now **proven reachable** — `any(block_number)` differs from `argMin(block_number, block_timestamp)` on multi-fill orders (verifier `37` of `1,502`; orchestrator re-run `9` of `1,502` — count unstable across runs, but non-zero either way). Stays medium.
- **C06** — half-fixed: the offer `0x48313df0...` is now present and priced (GNO, cycle `0x68e2c29...`) in `int_execution_gnosis_app_token_offers`, so the upstream registry gap is closed. But `346/346` claims for that offer in `int_execution_gnosis_app_token_offer_claims` still carry NULL cycle / empty symbol / `$0` USD (now `2026-05-14`..`05-21`) because the incremental partition was not reprocessed. Stays high; attribution `other`. (Drives new case N01.)
- **C15** — contract debt materially reduced but still substantive: `scripts/checks/check_api_tags.allow` exempts `27` distinct `api_execution_gnosis_app_*` models from `columns_missing`/`no_grain_col` (`54` lines), down from baseline ~`60+`. Spot-checked 3 — `users_daily`, `swaps_daily`, `retention_monthly` all carry explicit `api:` tags (served endpoints), so residual debt is consumer-facing. Stays medium.

### STILL CONFIRMED (17)
- **C02** (critical) — swap fee revenue still `$0`: all `44,904` filled trades in `int_execution_gnosis_app_swaps` have `fee_amount=0` (0 positive, 0 null). Root cause: the GA swaps model sources `int_execution_cow_trades` (on-chain Trade-event fee, `0` in the surplus-fee era), not `fct_execution_cow_trades.fee_usd`, which holds `239,223` rows with `fee_usd>0` summing to `$34,665.20` (2026 YTD, `fee_source='api'`). Recoverable revenue quantified; fix = switch source.
- **C03** (high) — identity bridge gap `781` (was `828`): `users_current` `24,020` (0 null addr) vs `user_identities` `23,239`. Downstream `INNER JOIN bridge` confirmed in `conversions.sql`/`events_chain_unified.sql`. Open: net-drop vs pseudonym-collapse split is **unverifiable** — the bridge is privacy-tier blocked and the conversions blast-radius probe is non-decisive (only `2,512` distinct pseudonyms ever convert).
- **C08** (medium) — `lagInFrame(total_active, 1) OVER (ORDER BY month)` still lacks an explicit ROWS frame (L78/81, L153/155); cross-scope leak ruled out (Any/Swap in separate CTEs). Empirically benign today (default == explicit-ROWS frame across all 7 months); latent correctness risk.
- **C09** (low) — coverage_daily still mixes `countDistinct` (L49) numerator with `count()` (L64) denominator; bias proven `0.000` across all `723` (date,kind) groups. Cosmetic.
- **C11** (low) — `gpay_volume_daily` still double-casts `sumIf(toFloat64OrNull(toString(amount_usd)), ...)` (L39-40); value-preserving on current data (sum identical across `1,875,402` rows, 0 nulled; `amount_usd` is `Nullable(Float64)`, and even `inf`/`nan` survive the roundtrip). Cosmetic.
- **C12** (low) — `any(offer_price_in_crc)` (L21) still non-deterministic in principle, but `0` of `41` offers carry >1 distinct `token_price_in_crc` — prices immutable per offer, so effectively always-safe. Latent.
- **C13** (medium) — all three `api_execution_gnosis_app_attribution_{7d,30d,60d}.sql` still derive `as_of_date` from `max(block_timestamp) FROM int_execution_gnosis_app_gpay_topups` (L8) and expose it via `SELECT sub.*`. Proxy leads marketplace_buy data by `3` days (`2026-06-20` vs `2026-06-17`); consumer-visible inaccurate freshness stamp.
- **C14** (medium) — foundation tables (`users_current`, `gpay_wallets`, `marketplace_offers`) carry only `elementary.schema_changes`; `swap_fees_weekly`/`monthly` have no tests at all. `0` current dupes on all 4 grains — preventive, not active.
- **C16** (low) — `api_execution_gnosis_app_users_daily` (`SELECT *`) and `api_execution_gnosis_app_gpay_wallets_daily` still lack `WHERE date < today()`; guarded sibling `api_execution_gnosis_app_token_offer_claim_funnel_daily` (L17) proves the inconsistency.
- **C17** (low) — swaps fill rate `73.1%` (`44,904`/`61,411`, `16,507` unfilled); only `elementary.volume_anomalies` and `freshness_anomalies` on the swaps model, both keyed on `date` (row-count/recency), neither covers `was_filled`/fill-rate.
- **C18** (low) — only `2` of `12` `*_7d` endpoints carry `window:7d` (`attribution_7d`, `kpi_swap_fees_7d`). `check_api_tags.py` passes (exit 0) because the `7d` suffix lives in the model name not an `api:` id, so CI does not flag it. Silent convention gap.
- **C19** (low) — `churn_retention_complementary` test (`churn_rate + retention_rate BETWEEN 80 AND 120`, `severity: warn`, L793/795) fires on `6` of `14` month-scopes; the two extremes (`105000`, `15600`) are each scope's first month (`rn=1`) `greatest(lagInFrame,1)=1` bootstrap artifacts (same mechanic as C08). The `where` clause excludes `2025-11`, dropping the worst row from CI. Warn-only.
- **C20** (low) — `int_execution_gnosis_app_user_activity_daily` still `materialized='table'` + inert `unique_key` (L3-6); `0` duplicate tuples (RMT `order_by` dedups). Cosmetic config-confusion shared by `27` repo-wide models.
- **C22** (high) — threshold drift persists: intermediate `schema.yml` L137 says `>=3` (`8,363` distinct addresses) but `fct_execution_gnosis_app_users_distinct.sql` L65, marts `schema.yml` L2471/2519, and the approved semantic metric `gnosis_app_high_confidence_users` all use `>=2` (`22,157`). `2.65x` / `13,794`-user divergence in a headline reach number; the served metric contradicts the docs.
- **C23** (medium) — `events_chain_unified` emits `chain.circles_fee` (`178,623`) and `chain.circles_personal_mint` (`197,968`) = `376,591` touchpoint rows absent from `seeds/mta_event_kinds.csv` (which lists legacy `chain.circles_metri_fee`, `0` emitted). Relationship test exists at `severity: warn` (intermediate `schema.yml` L921-925) — warns, does not break CI.
- **C24** (medium) — `marketplace_buy` still `428/428` NULL `conversion_amount_usd` (hard-coded `CAST(NULL)` at `conversions.sql` L96) while the other 3 kinds are 100% priced. Documented in 3 places; attribution credits are count-weighted (no USD column), so this is a documented count-only kind.
- **C26** (medium) — daily `returning` (prior-7-day, `users_daily.sql` L71-74) vs weekly `returning` (adjacent prior week, `users_weekly.sql` L43-51) use different windows/populations; the daily `8-30`-day dead-zone is non-empty (`141` of `1,424` active on the latest complete day). Non-reconcilable across grains.
- **C27** (low) — `funnel_daily` still emits `151,710` `level=0` rows (vs `233,160` level>=1) with no `level>=1` guard, but no `api_` view selects from `fct_execution_gnosis_app_funnel_daily` — over-count risk is internal-only, not consumer-reachable.

### NEW (1)
- **N01** (medium) — the incremental `int_execution_gnosis_app_token_offer_claims` model (`insert_overwrite`, monthly partition) was not reprocessed for the `2026-05` partition after offer `0x48313df0...` became resolvable in `int_execution_gnosis_app_token_offers` (now priced GNO). Result: `346` real GNO claims permanently mis-priced at `$0` (NULL cycle, empty symbol) downstream. A targeted refresh of the `2026-05` `token_offer_claims` partition would heal them. Attribution `other` (stale-partition). This is the precise residual mechanism behind C06's "half-fixed" state.

### UNVERIFIABLE / UNRESOLVED (0 fully, 1 partial)
- **C03** carries an open sub-question: the `781`-gap's net-drop-vs-collapse split is unverifiable because `int_execution_gnosis_app_user_identity_bridge` is privacy-tier blocked. The gap itself and the INNER-JOIN drop mechanic are confirmed; severity held at high.

## Evidence appendix

**C01** — `SELECT count() onboard, min/max date, countDistinct(toStartOfMonth(date)) FROM int_execution_gnosis_app_user_activity_daily WHERE activity_kind='onboard'` (+ per-month groupBy): `onboard=24,020` == `users_current=24,020` (0 null addr); `8` contiguous months (`2025-11`: 4320, `2025-12`: 1778, `2026-01`: 7260, `2026-02`: 3554, `2026-03`: 2012, `2026-04`: 1243, `2026-05`: 2063, `2026-06`: 1790). Compiled artifact onboard CTE = only `WHERE first_seen_at IS NOT NULL` (no `toStartOfMonth>=2026-05-01` filter; vars NULL).

**C02** — `SELECT countIf(fee_amount=0), countIf(fee_amount>0), countIf(fee_amount IS NULL), count() FROM int_execution_gnosis_app_swaps WHERE was_filled=1`: `44,904` zero, `0` positive, `0` null. `SELECT countIf(fee_usd>0), count(), max(fee_usd), sum(fee_usd), countIf(fee_source='api') FROM fct_execution_cow_trades WHERE block_timestamp>='2026-01-01'`: `239,223` fee_usd>0, sum=`$34,665.20`, max=`$372.32`, `239,268` api-sourced. Upstream `int_execution_cow_trades` 2026: `331,624` trades 100% `fee_amount=0` (chain-wide, by surplus-fee design).

**C03** — `SELECT (SELECT count() FROM int_execution_gnosis_app_users_current) uc, (countIf address IS NULL), (SELECT count() FROM int_execution_gnosis_app_user_identities) ui`: `uc=24,020` (0 null), `ui=23,239`, gap `781`. `conversions` has only `2,512` distinct `user_pseudonym`. Bridge query rejected (internal-only). INNER-JOIN quoted: `INNER JOIN bridge b ON b.address = lower(s.taker)`.

**C04** — `SELECT value, toTypeName(value) FROM api_execution_gnosis_app_kpi_retention_pct_latest`: `23.6`, `Nullable(Float64)`. `anyIf(retention_pct, months_since=1 AND cohort_month=toDate('2026-05-01'))` (cohort has M0 but no M1 row) returns `NULL`, not `0.0`.

**C05** — code: `gpay_wallets.sql` L7 `incremental_strategy='delete+insert'`; `swaps.sql` L70 `any(block_number) AS first_fill_block`. `SELECT max(block_timestamp) FROM int_execution_gnosis_app_swaps`: `2026-06-20 23:58`. Multi-fill divergence query over `int_execution_cow_trades` since `2025-11-12`: `1,502` multi-fill `(taker, order_uid)` orders, `any()` differs from `argMin(block_number, block_timestamp)` on `37` (verifier) / `9` (orchestrator re-run).

**C06 / N01** — `SELECT countIf(cycle_address IS NULL), count(), min/max date, groupUniqArray(offer_address) FROM int_execution_gnosis_app_token_offer_claims`: `346/8,822` NULL cycle, `offer_token_symbol=''`, `amount_received_usd=0`, dates `2026-05-14`..`05-21`, all offer `0x48313df0...`. `SELECT count(), any(cycle_address), any(offer_token_symbol) FROM int_execution_gnosis_app_token_offers WHERE offer_address='0x48313df0...'`: now resolves (cycle `0x68e2c29feed2a4d0f22cc6d271e2b25124d99892`, symbol `GNO`, token `0x9c58bacc...`). Parent cycle has `CycleConfiguration(1)` + `NextOfferCreated(29)` decoded.

**C07** — `SELECT uniqExact(cohort_month) FROM fct_execution_gnosis_app_retention_monthly` = `7`; `uniqExact(month) FROM fct_execution_gnosis_app_churn_monthly` = `7`. Reconciliations: `2025-11` cohort_size `4,320` = onboard distinct addr for `2025-11`; churn `Any` `2026-05` total_active `6,677` = non-onboard distinct actives `2026-05`.

**C08** — code L78/81: `lagInFrame(s.total_active, 1) OVER (ORDER BY s.month)`, no ROWS frame. Empirical: `lag_default == lag_explicit` for all 7 months (`0, 4552, 5260, 11058, 7915, 6668, 5605`).

**C09** — code L49 `countDistinct(user_pseudonym, conversion_ts)`, L50 `uniqExact`, L64 `count()`. `max abs(cd_ratio - ue_ratio)` = `0.000` across `723` (date,kind) groups.

**C10** — code L66/68 `toFloat64(o.token_price_in_crc_raw)/1e18`. `SELECT toFloat64(toUInt256OrNull('not_a_number'))` = `NULL` (type `Nullable(Float64)`), **not** `0.0`; guarded form also NULL. `41` rows, `0` with raw NULL. Baseline premise (NULL->0.0) does not hold.

**C11** — code L39-40 `sumIf(toFloat64OrNull(toString(amount_usd)), ...)`. `sum_direct = sum_roundtrip = 157,514,080.9431` over `1,875,402` rows, `0` nulled. `toFloat64OrNull(toString(inf))`='inf', `toString(nan)`='nan' both roundtrip (not nulled).

**C12** — code L21 `any(c.offer_price_in_crc)`. `SELECT count() offers, countIf(n_prices>1) FROM (SELECT offer_address, uniqExact(token_price_in_crc) n_prices ... GROUP BY offer_address)`: `41` offers, `0` multi-price.

**C13** — all three siblings L8: `(SELECT toDate(max(block_timestamp)) FROM int_execution_gnosis_app_gpay_topups) AS as_of_date`. gpay_topups max `2026-06-20`; `marketplace_buy` max `conversion_date` `2026-06-17`. Served via `SELECT sub.*`.

**C14** — `intermediate/schema.yml`: `users_current` (L144-148), `gpay_wallets` (L472-475), `marketplace_offers` (L510-513) carry only `elementary.schema_changes`; `swap_fees_weekly` (L1269-1281) and `swap_fees_monthly` (L1283-1294) have no tests. `0` dupes on all 4 grains (`address`, `pay_wallet`, `week`, `month`).

**C15** — `scripts/checks/check_api_tags.allow`: `54` GA `columns_missing`/`no_grain_col` lines = `27` distinct models. Spot-check tags: `users_daily`=`api:gnosis_app_users`, `swaps_daily`=`api:gnosis_app_swaps` (not allowlisted), `retention_monthly`=`api:gnosis_app_retention`.

**C16** — `api_execution_gnosis_app_users_daily` L21 `SELECT * ... ORDER BY date` (no guard); `api_execution_gnosis_app_gpay_wallets_daily` L23-29 (no guard); guarded sibling `api_execution_gnosis_app_token_offer_claim_funnel_daily` L17 `WHERE date < today()`.

**C17** — `SELECT countIf(was_filled=1), count() FROM int_execution_gnosis_app_swaps`: `44,904`/`61,411` = `73.1%`. swaps `schema.yml`: `elementary.volume_anomalies` (L374, `timestamp_column: date`), `freshness_anomalies` (L386, `timestamp_column: date`) — neither targets `was_filled`.

**C18** — `12` `api_*_7d` endpoints; `2` carry `window:7d`. `scripts/checks/check_api_tags.py` exits `0`.

**C19** — `marts/schema.yml` L793 `churn_rate + retention_rate BETWEEN 80 AND 120`, L795 `severity: warn`, `where` `month>2025-12-01 AND month<toStartOfMonth(today())`. Out-of-band: Any `2025-11` (105000, rn=1), `2026-03` (202.9), `2026-04` (70.1), `2026-06` (75.9); Swap `2025-11` (15600, rn=1), `2026-06` (79.8).

**C20** — code L3-6: `materialized='table'`, `engine='ReplacingMergeTree()'`, `order_by='(date,address,activity_kind)'`, `unique_key='(date,address,activity_kind)'`. `0` duplicate tuples; `27` table-mat models repo-wide co-occur with `unique_key`.

**C21** — `SELECT cohort_month, anyIf(retention_pct, months_since=0) m0, anyIf(retention_pct, months_since=1) m1, anyIf(initial_users, months_since=0) FROM fct_execution_gnosis_app_retention_monthly GROUP BY cohort_month`: `2025-11`: m0=`100.0`, m1=`76.3`, cohort_size=`4,320`; all 7 cohorts m0=`100.0`. `0` rows >100%, `0` upper-triangle. Code L49-59 `cohort_size = count(DISTINCT address)`, L74 `retention=users/initial_users`.

**C22** — `SELECT countIf(n_distinct_heuristics>=3), countIf(n_distinct_heuristics>=2) FROM int_execution_gnosis_app_users_current`: `8,363` vs `22,157` (gap `13,794`, `2.65x`). `get_metric_details('gnosis_app_high_confidence_users')`: description `... n_distinct_heuristics >= 2`, root_model `fct_execution_gnosis_app_users_distinct`, status `approved`. Intermediate `schema.yml` L137 says `>= 3`.

**C23** — `SELECT event_kind, count() FROM int_execution_gnosis_app_events_chain_unified WHERE event_kind IN (...) GROUP BY event_kind`: `chain.circles_fee`=`178,623`, `chain.circles_personal_mint`=`197,968` (total `376,591`). Seed has `chain.circles_metri_fee` (`0` emitted), lacks both. Relationship test at `intermediate/schema.yml` L921-925, `severity: warn`.

**C24** — `SELECT conversion_kind, count(), countIf(conversion_amount_usd IS NULL), countIf(>0) FROM int_execution_gnosis_app_conversions GROUP BY conversion_kind`: `marketplace_buy` `428`/`428` NULL/`0` positive; swap_filled `44,842`/`44,842`, topup `49,425`/`49,451`, token_offer_claim `8,475`/`8,821` priced. `describe_table fct_execution_gnosis_app_attribution_30d`: no USD-weighted column (credits in conversion-equivalent units). Hard-coded `CAST(NULL)` at `conversions.sql` L96.

**C25** — `SELECT count(), uniqExact(ga_user), max(block_timestamp) FROM int_execution_gnosis_app_gpay_topups`: `49,451` / `1,081` / `2026-06-20`. `gpay_wallets`: `1,173` currently-GA-owned, `1,240` total. Bound: `1,081 <= 1,173 <= 1,240 <= 24,020`.

**C26** — `users_daily.sql` L71-74 returning = active prior 7d, L88-95 reactivated = inactive prior 30d. `users_weekly.sql` L43-51 returning = `prev.week = curr.week - INTERVAL 7 DAY`, L55-71 reactivated = NOT active prior 4 weeks. Dead-zone: `141` of `1,424` active on latest complete day had prior activity 8-30 days ago, none in prior 7d.

**C27** — `SELECT level, count() FROM fct_execution_gnosis_app_funnel_daily GROUP BY level`: level0=`151,710`, level1=`209,389`, level2=`17,449`, level3=`6,322`. Grep: no `api_` view selects from the fct (only `schema.yml`).

**C28** — `semantic/authoring/execution/gnosis_app/semantic_models.yml`: `125` semantic_model blocks; explicit entries for `execution_gnosis_app_attribution_30d/60d/7d` (`model: ref('api_execution_gnosis_app_attribution_*')`), swaps/topups/retention/churn. `reload_semantic_registry` reported `execution_available:false`, `stale_reason: manifest_hash_mismatch`.

## Review log (>=3 rounds per case)

- **C01**: R1 RESOLVED (onboard `24,020`=users_current, 8 months) -> orchestrator challenged incident_attribution (batch-vars not insert_overwrite) -> R2 RESOLVED, re-tagged via compiled-artifact (filter gone, vars NULL) -> R3 RESOLVED, durability confirmed (earliest month `2025-11` still full `4,320`).
- **C02**: R1 CONFIRMED critical (`44,904` filled all `fee_amount=0`) -> challenge: trace one level deeper -> R2 CONFIRMED (`int_execution_cow_trades` 2026 `331,624` 100% zero, surplus-fee root cause) -> R3 CONFIRMED, proved off-chain fee exists (`fct_execution_cow_trades` `239,223` rows, `$34,665.20`).
- **C03**: R1 CONFIRMED gap `781` -> challenge: prove the drop -> R2 CHANGED (bridge privacy-blocked, INNER-JOIN confirmed in code) -> R3 CONFIRMED; orchestrator left open: collapse-vs-drop unverifiable, conversions probe non-decisive (`2,512` pseudonyms).
- **C04**: R1 CHANGED low (KPI=`23.6`, no guard) -> challenge: prove silent-zero reachable -> R2 CHANGED (selects intended cohort) -> R3 CHANGED, `anyIf` over Nullable returns NULL not `0.0`; failure mode benign.
- **C05**: R1 CHANGED (delete+insert/any persist, freshness resolved) -> challenge: prove determinism reachable -> R2 CHANGED (`1,502` multi-block orders) -> R3 CHANGED, divergence demonstrated (`37`/`9` of `1,502`); count unstable but non-zero.
- **C06**: R1 CONFIRMED high (`346` null-cycle) -> challenge: pin to registry gap -> R2 CONFIRMED (all map to `0x48313df0...`, absent from seeds) -> R3 CHANGED, offer now resolved in token_offers (GNO) but `346` claims stale -> spawned N01.
- **C07**: R1 RESOLVED (7 cohorts/months) -> challenge (attribution + reconcile latest month) -> R2 RESOLVED (`2026-05` `6,677` reconciles) -> R3 CONFIRMED-resolved, second mart reconciled (`2025-11` `4,320`).
- **C08**: R1 CONFIRMED medium (no ROWS frame) -> challenge: cross-scope leak? -> R2 CONFIRMED (separate CTEs, no leak) -> R3 CONFIRMED, default==explicit empirically; latent.
- **C09**: R1 CONFIRMED medium -> challenge: quantify bias -> R2 CONFIRMED low (max group 506 << HLL threshold) -> R3 CONFIRMED low, bias `0.000` across `723` groups.
- **C10**: R1 CONFIRMED medium -> challenge: prove reachability -> R2 CONFIRMED low (0 NULL rows) -> R3 CHANGED low, mechanic check shows `toFloat64(NULL)`=NULL not `0.0`.
- **C11**: R1 CONFIRMED medium -> challenge: quantify impact -> R2 CONFIRMED low (value-preserving) -> R3 CONFIRMED low, inf/nan survive roundtrip.
- **C12**: R1 CONFIRMED medium -> challenge: reachability -> R2 CONFIRMED low (0 multi-price groups) -> R3 CONFIRMED low, prices immutable per offer (0/41).
- **C13**: R1 CONFIRMED medium (insufficient — siblings assumed) -> challenge: read all 3 + show lead/lag -> R2 CONFIRMED (all 3 identical, leads marketplace by 4d) -> R3 CONFIRMED, consumer-visible via `SELECT sub.*`, 3d lead.
- **C14**: R1 CONFIRMED medium -> challenge: test for dupes -> R2 CONFIRMED (0 dupes, preventive) -> R3 CONFIRMED, swap_fees grains also 0 dupes.
- **C15**: R1 CHANGED medium (~60+ -> 29) -> challenge: which are served -> R2 CHANGED (all api-prefixed) -> R3 CHANGED, spot-checked 3 carry api: tags; `27` distinct.
- **C16**: R1 CONFIRMED low (1 today() row) -> challenge: show guarded sibling -> R2 CONFIRMED (funnel view guards) -> R3 CONFIRMED (medium conf on live partial-row; code defect high conf).
- **C17**: R1 CONFIRMED low (`73.1%`, no test) -> challenge: sector-wide grep -> R2 CONFIRMED -> R3 CONFIRMED, only volume/freshness anomalies on `date`.
- **C18**: R1 CONFIRMED low (2/12) -> challenge: cite convention -> R2 CONFIRMED (POINT_GRANS requires it) -> R3 CONFIRMED, `check_api_tags.py` passes (silent gap).
- **C19**: R1 CONFIRMED low (sums to 105000) -> challenge: confirm warn severity -> R2 CONFIRMED (warn, 6/7 outside) -> R3 CONFIRMED, violations concentrated in rn=1 bootstrap rows.
- **C20**: R1 CONFIRMED low (inert unique_key) -> challenge: confirm RMT dedups -> R2 CONFIRMED (0 dupes) -> R3 CONFIRMED, `27` models repo-wide co-occur, harmless.
- **C21**: R1 RESOLVED (cohort_size CTE, max OVER gone) -> challenge: no >100% / no upper-triangle -> R2 RESOLVED (0/0, max=100.0) -> R3 CONFIRMED-resolved, monotonic decay (`2025-11` M0=100->M1=76.3).
- **C22**: R1 CONFIRMED high (>=3 docs vs >=2 SQL) -> challenge: quantify reach -> R2 CONFIRMED (`8,363` vs `22,157`) -> R3 CONFIRMED, metric metadata serves >=2.
- **C23**: R1 CONFIRMED medium -> challenge: quantify unmatched -> R2 CONFIRMED (`376,591` rows) -> R3 CONFIRMED, relationship test exists at warn severity.
- **C24**: R1 CONFIRMED medium (`428`/`428` NULL) -> challenge: trace to attribution -> R2 CONFIRMED (documented 3 places) -> R3 CONFIRMED, attribution is count-weighted (no USD column).
- **C25**: R1 RESOLVED (`49,451`/`1,081`) -> challenge: not over-counted -> R2 RESOLVED (definition correct) -> R3 RESOLVED, bounded `1,081 <= 1,173 <= 1,240`.
- **C26**: R1 CONFIRMED medium (insufficient — code only) -> challenge: quantify dead-zone -> R2 CONFIRMED (`141`/`1,424`) -> R3 CONFIRMED, weekly def differs (adjacent-week).
- **C27**: R1 CONFIRMED low -> challenge: quantify -> R2 CONFIRMED (`151,710` level=0) -> R3 CONFIRMED, no api view exposes it (internal-only).
- **C28**: R1 RESOLVED (semantic_models.yml expanded) -> challenge: enumerate names -> R2 RESOLVED (`120` models) -> R3 RESOLVED (`125` blocks; live registry non-executable, medium conf).
- **N01**: discovered R3 -> NEW medium, offer resolved in token_offers but `346` claims stale in unreprocessed `2026-05` partition.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE) | Switch GA swap-fee source from on-chain `int_execution_cow_trades` (zero in surplus-fee era) to `fct_execution_cow_trades.fee_usd` (off-chain api fees, `$34,665` 2026 YTD) so `fee_usd_total` / `kpi_swap_fees_7d` stop reporting `$0` | `int_execution_gnosis_app_swaps.sql`, `int_execution_gnosis_app_swap_fees_daily.sql` |
| P1 (KEEP) | Unify the high-confidence threshold to a single value (docs `>=3` vs served metric `>=2`, a `13,794`-user / `2.65x` reach divergence) across docs, SQL, and the semantic metric | `int_execution_gnosis_app_users_current.sql` (schema docs), `fct_execution_gnosis_app_users_distinct.sql`, `semantic/authoring/execution/gnosis_app/semantic_models.yml` |
| P1 (NEW) | Refresh the `2026-05` `int_execution_gnosis_app_token_offer_claims` partition (`insert_overwrite`) now that offer `0x48313df0...` resolves to GNO — heals `346` claims stuck at `$0` / NULL cycle | `int_execution_gnosis_app_token_offer_claims.sql` |
| P1 (KEEP) | Add an anti-divergence test on the identity bridge gap (`781` rows; `users_current` `24,020` vs `user_identities` `23,239`); bridge is privacy-blocked so the net-drop split is unverifiable without access | `int_execution_gnosis_app_user_identity_bridge.sql`, `int_execution_gnosis_app_user_identities.sql` |
| P2 (KEEP) | Replace `any(block_number)` with `argMin(block_number, block_timestamp)` for `first_fill_block` (diverges on multi-fill orders today) and migrate `gpay_wallets` off banned `delete+insert` | `int_execution_gnosis_app_swaps.sql`, `int_execution_gnosis_app_gpay_wallets.sql` |
| P2 (KEEP) | Add both emitted event kinds (`chain.circles_fee`, `chain.circles_personal_mint`) and drop/rename legacy `chain.circles_metri_fee` in the seed; `376,591` touchpoint rows currently warn on the relationship test | `seeds/mta_event_kinds.csv` |
| P2 (KEEP) | Derive attribution `as_of_date` from the attribution fct's own max conversion_date instead of the `gpay_topups` proxy (leads marketplace_buy by 3 days) | `api_execution_gnosis_app_attribution_{7d,30d,60d}.sql` |
| P2 (KEEP) | Add uniqueness/not_null tests to foundation tables (`address`, `pay_wallet`, `gateway_address`) and grain tests to `swap_fees_weekly`/`monthly` (currently `0` tests; `0` dupes — preventive) | `int_execution_gnosis_app_users_current.sql`, `int_execution_gnosis_app_gpay_wallets.sql`, `int_execution_gnosis_app_marketplace_offers.sql`, `int_execution_gnosis_app_swap_fees_weekly.sql`, `int_execution_gnosis_app_swap_fees_monthly.sql` |
| P3 (KEEP) | Pay down api-tag/column-schema debt for the remaining `27` allowlisted served endpoints; add `window:7d` to the `10` of `12` 7d KPIs missing it | `models/execution/gnosis_app/marts/schema.yml`, `scripts/checks/check_api_tags.allow` |
| P3 (KEEP) | Add an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame to `lagInFrame` (benign today, latent risk); align daily vs weekly `returning`/`reactivated` definitions and document the `8-30`-day dead-zone | `fct_execution_gnosis_app_churn_monthly.sql`, `fct_execution_gnosis_app_users_daily.sql`, `fct_execution_gnosis_app_users_weekly.sql` |
| P3 (KEEP) | Flag `marketplace_buy` as count-only until the Circles/CRC price feed lands (`428`/`428` NULL USD); add a fill-rate anomaly test on swaps (`73.1%`, uncovered) | `int_execution_gnosis_app_conversions.sql`, `int_execution_gnosis_app_swaps.sql` |
| P4 (KEEP) | Cosmetic cleanups: `level>=1` guard/test on `funnel_daily`; remove inert `unique_key` from table-mat `user_activity_daily`; switch coverage/gpay_volume estimators (bias `0.000`, value-preserving today) | `fct_execution_gnosis_app_funnel_daily.sql`, `int_execution_gnosis_app_user_activity_daily.sql`, `int_execution_gnosis_app_coverage_daily.sql`, `fct_execution_gnosis_app_gpay_volume_daily.sql` |
| — (DROP) | Resolved — no action: onboard truncation (C01), wrong-population marts (C07), retention denominator (C21), topup volume (C25), semantic coverage (C28). Also drop the `retention_pct_latest`-returns-0.0 worry (C04): column is Nullable, returns NULL not `0.0`; the `toFloat64(NULL)->0.0` price collapse (C10): mechanic returns NULL | — |
