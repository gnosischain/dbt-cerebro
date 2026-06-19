# Model review: revenue

**Convergence:** Converged in 1 round — all three inspectors (intermediate shard, marts-1, marts-2) and the context agent produced mutually consistent findings, all warehouse-validated with no material contradictions.

---

## Scope and inventory

| Layer | Count | Notes |
|---|---|---|
| `models/revenue/intermediate/` | 7 SQL files | 4 daily per-stream, 1 unified daily view, 1 weekly per-user, 1 monthly per-user |
| `models/revenue/marts/` | ~42 SQL files | 21 api_ thin wrappers, 14 fct_ cohort/totals/per-user models |
| `semantic/authoring/revenue/` | 12 semantic models, 23 metrics | Missing gnosis_app stream entirely |
| Macros consumed | 4 | `apply_monthly_incremental_filter`, `cohort_buckets`, `refill_safe_hooks`, `pseudonymize_address` |
| CI guards | 2 | `check_api_tags.py`, `no_delete_insert.py` (both partially bypassed for this module) |

---

## Business context

The revenue unit models **potential** economic value that could accrue to the Gnosis DAO from four on-chain fee streams: (1) imputed interest on stablecoin holdings (EURe, USDC.e, BRLA, ZCHF at hardcoded APY rates); (2) imputed sDAI yield (live sDAI/sUSDS rate times a hardcoded 10% DAO share); (3) Gnosis Pay per-payment fees (actual ERC20 transfer events to settlement address `0x4822521e6135cd2599199c83ea35179229a172ee`); and (4) Gnosis App (Metri CRC) fees (actual CRC ERC-1155 transfers to fee receiver `0x97fd8f7829a019946329f6d2e763a72741047518`, scoped to 2025-11-12 onward). This is explicitly **not** a treasury/realised-revenue report — that lives in a separate module.

**Primary headline output:** a cross-stream deduplicated count of "economically active" users — weekly threshold $6/year (trailing 52-week rolling fees), monthly threshold $0.50/month. The unit is a dbt port of the Dune `gnosis_rev_*` query family, extended from EURe-only holdings to four stablecoins. The lending stream present in the Dune spec is deliberately omitted.

**Canonical definitions (abbreviated):**
- Active weekly user: `annual_rolling_fees >= 6.0` (sum across all streams, trailing 52 calendar weeks, per `fct_revenue_active_users_totals_weekly`).
- Active monthly user: `month_fees >= 0.50` (sum across all streams in the calendar month, per `fct_revenue_per_user_monthly`).
- `user_pseudonym`: `sipHash64(lowercased_address, pii_salt)` — canonical cross-sector join key compatible with GPay, Gnosis App, and Mixpanel hashes.
- Holdings fee: `balance_usd * daily_rate` where rates are compile-time Jinja constants.
- GPay fee: `amount_native * fee_bps / 10000 * price_usd` (EURe/GBPe 20 bps, USDC.e 100 bps).
- Gnosis App fee: `fee_native_CRC * crc_price_usd` with daily-median fallback when per-avatar price is absent.

---

## Implementation assessment

### High

**refill_append models missing refill_safe pre/post hooks**
`models/revenue/intermediate/int_revenue_holdings_fees_daily.sql`, `int_revenue_gpay_fees_daily.sql`, `int_revenue_sdai_fees_daily.sql` all carry the `refill_append` tag but contain no `pre_hook`/`post_hook` calls. The `refill_safe_hooks` macro docstring explicitly states any model tagged `refill_append` whose source aggregation could span a whole month must use these hooks to cap memory at 8 GiB and spill GROUP BY/sort to disk. Omitting them exposes all three models to Code 241 OOM during any price-gap recovery refill run.

**`int_revenue_fees_weekly_per_user` uses delete+insert (project-banned) in normal incremental mode**
Line 41: `incremental_strategy=('append' if start_month else 'delete+insert')`. Normal runs (no `start_month` var) always take the delete+insert path, which `no_delete_insert.py` CI guard explicitly bans on ClickHouse Cloud. The model is on `no_delete_insert.allow` as acknowledged migration debt, but is live in production. On the ReplacingMergeTree engine, delete+insert can leave duplicate rows visible between OPTIMIZE cycles.

**`int_revenue_sdai_fees_daily` INNER JOIN on rates silently drops all user-days when rate source has a gap**
`int_revenue_sdai_fees_daily` uses `INNER JOIN rates r USING (date)`. Any date absent from `int_yields_sdai_rate_daily` (freshness lag, first-run empty state) silently drops every sDAI user-balance row for that day with no error or NULL sentinel. GPay uses LEFT JOIN (producing NULL fees but preserving the row); sDAI is the only stream with a complete-row-drop failure mode.

**NULL USD fees propagate silently from gnosis_app and gpay into all downstream aggregates**
`int_revenue_gnosis_app_fees_daily`: 728 rows with `fees = NULL` (June 2026 partial month) where both per-token CRC price and daily-median fallback are absent. `int_revenue_gpay_fees_daily`: 8 rows with `fees = NULL` for GBPe at token launch (2024-01-08 to 2024-01-15). Both flow through `int_revenue_fees_unified_daily` (no `COALESCE` guard) into the weekly and monthly per-user intermediates. The mart `countIf(annual_rolling_fees > 0)` silently excludes NULL users rather than flagging them, understating active user counts.

### Medium

**Weekly fct_ views read ReplacingMergeTree without FINAL — latent duplicate risk**
`fct_revenue_per_user_weekly` and `fct_revenue_active_users_cohorts_weekly` are plain views over `int_revenue_fees_weekly_per_user` (ReplacingMergeTree) with no `SELECT ... FINAL` or `SETTINGS deduplicate=1`. During the merge window after a delete+insert run, unmerged duplicates inflate user counts and fee totals. No duplicates found in spot checks, but risk is latent on heavy insert days.

**`int_revenue_fees_weekly_per_user` has no uniqueness test**
`tests: []` in `models/revenue/intermediate/schema.yml`. This is the most complex model in the unit (densification + window function + multi-stream UNION ALL + dual incremental strategies). All four daily intermediates and the monthly model have `dbt_utils.unique_combination_of_columns` tests; the weekly model has none.

**`int_revenue_fees_unified_daily` and the weekly model carry no model-level tests**
`int_revenue_fees_unified_daily` is the fan-in view consumed by all cross-stream active-user marts and has zero tests. A silent schema change in any upstream daily model fails only at mart query time.

**All 28 api_ revenue views globally allowlisted in `check_api_tags.allow`**
Every `api_revenue_*` model has both `columns_missing` and `no_grain_col` entries in `scripts/checks/check_api_tags.allow` (56 total entries). No `api_` view carries a typed columns block in `schema.yml`. The entire revenue module is invisible to the API convention guard.

**Hardcoded APY rates and sDAI DAO share constant require SQL edits and full-history rebuilds**
Holdings APY rates (EURe/USDC.e 0.351%, BRLA 2.07%, ZCHF 0.5%) and the sDAI DAO share (10%) are Jinja compile-time constants with no `dbt var` override, effective-date record, or audit trail. The `schema.yml` comment "edit to retune" is the only change-management documentation. The 10% DAO share has not been verified against post-November 2025 sUSDS regime parameters following the sDAI vault backing-asset switch.

**~31% of BRLA user-days have `balance_usd_total = 0` after rounding**
917k of 2.99M BRLA rows have `balance_usd_total = 0` and `fees = 0` because `round(sum(balance_usd), 6)` truncates dust balances to zero. The source filter `WHERE balance_usd > 0` does not exclude them; the rounding in the final SELECT destroys precision. These zero-economic-signal rows count as distinct user-days and may inflate BRLA user-period counts in downstream cohort tables.

### Low

**`countIf` predicate in monthly cohort models is dead code**
`fct_revenue_active_users_cohorts_monthly` (and all per-stream monthly variants) use `countIf(month_fees > 0) AS users_cnt`, but the outer `WHERE month_fees >= 0.01` makes `> 0` always true for any row that reaches the SELECT. No wrong numbers result, but the `countIf` intent is misleading.

**`schema.yml` description for `fct_revenue_per_user_weekly` omits gnosis_app stream**
The description reads "Sums annual_rolling_fees across all streams (holdings + sDAI + gpay)" — gnosis_app is absent despite the SQL correctly including it and exposing `has_gnosis_app`. The same omission appears in `fct_revenue_active_users_totals_weekly`. Documentation drift misleads schema consumers.

**Weekly revenue API views expose `annual_rolling_fees` with no `window:rolling_52w` tag**
The weekly cohort and totals api_ views carry `granularity:weekly` but no `window:rolling_52w` tag per the project API tag convention (`project_api_tag_convention.md`).

---

## Business-logic assessment

### Critical

**Monthly pipeline has systematic coverage gap — ~75% of history missing**
Warehouse queries confirmed `fct_revenue_active_users_totals_monthly` contains only 11 rows: Oct/Nov/Dec for each of 2023, 2024, 2025, then Apr and May 2026. Jan–Sep 2023/2024/2025 and Jan–Mar 2026 are entirely absent. The same pattern appears in `fct_revenue_per_user_monthly` (452k rows across 11 months only) and `fct_revenue_sdai_cohorts_monthly` (24 rows/year = 8 cohorts × 3 months). Root cause is an incomplete backfill of `int_revenue_fees_monthly_per_user`, which uses `insert_overwrite` with `partition_by='toStartOfYear(month)'` — a failed or partial year backfill drops the entire year's data. All monthly revenue trend analysis, year-over-year comparisons, and Q1–Q3 period reporting are silently empty. Q1 2026 is entirely unreportable.

### High

**GPay settlement address may not match the current Spender router post-April 2025 architecture change**
`int_revenue_gpay_fees_daily` hardcodes `settlement_address = 0x4822521e6135cd2599199c83ea35179229a172ee`. Context analysis found that `cerebro-docs/protocols/gnosis-pay/index.md` documents a global Spender router at `0xcff260bfbc199dc82717494299b1acade25f549b` — a different address — that superseded the per-Safe Spender proxy assumption. If the settlement/fee-receiver address also changed with the April 2025 Spender architecture update, the GPay revenue model would be missing all card-spend fees since that date. No inspector ran a volume-continuity query across April 2025; this is the single highest-stakes open question in the unit.

**Cohort table and totals table use different fee floor thresholds — undocumented semantic split**
`fct_revenue_active_users_cohorts_monthly` uses `WHERE month_fees >= 0.01` (includes 0.01–0.1 and 0.1–0.5 monthly cohort buckets). `fct_revenue_active_users_totals_monthly` uses `countIf(month_fees >= 0.50)`. Warehouse confirmed ~89k user-months in sub-$0.50 buckets (60,731 in 0.01–0.1; 28,385 in 0.1–0.5) appear in cohorts but are excluded from totals. For 2026-05: 16,936 sub-threshold users in cohorts vs 10,775 in totals (~1.6x discrepancy). `schema.yml` references the $6/$0.50 threshold but does not document that cohorts intentionally extend below it. Naive cohort summation cannot reproduce the headline totals count.

**Gnosis App stream absent from semantic layer — `has_gnosis_app` dimension unqueryable via MCP**
`fct_revenue_gnosis_app_cohorts_weekly` and `fct_revenue_gnosis_app_cohorts_monthly` have marts and API endpoints but no semantic model in `semantic/authoring/revenue/semantic_models.yml`. Additionally, the `has_gnosis_app` dimension present in `fct_revenue_per_user_weekly/monthly` is absent from both `revenue_per_user_weekly` and `revenue_per_user_monthly` semantic model dimension lists (which cover only `has_holdings`, `has_sdai`, `has_gpay`, `is_revenue_active`). MCP `query_metrics` cannot filter or group by `has_gnosis_app`, cannot surface gnosis_app stream cohorts, and cannot include gnosis_app in cross-sector overlap analysis — the primary stated use case of the per-user marts.

### Medium

**Cross-stream weekly cohort includes sub-$1 users; per-stream cohorts exclude them — undocumented divergence**
`fct_revenue_active_users_cohorts_weekly` uses `cohort_bucket_yearly` with `include_below_one=true`. All per-stream cohort models (sDAI, GPay, gnosis_app, holdings) use `include_below_one=false` with `WHERE annual_rolling_fees >= 1`. User counts in the cross-stream cohort table cannot be summed against per-stream cohort totals for the same week. This is likely intentional but is undocumented in `schema.yml`.

**Hardcoded rates require SQL edits and full rebuilds on any policy change** (see Implementation section above for detail).

### Low

**Weekly revenue API endpoints expose `annual_rolling_fees` with no `window:rolling_52w` tag** (see Implementation section above).

---

## Data findings

Eight warehouse queries were executed per inspector shard (24 total across the review).

| Finding | Value |
|---|---|
| `int_revenue_gpay_fees_daily` NULL fees | 8 rows (GBPe, 2024-01-08 to 2024-01-15) |
| `int_revenue_gnosis_app_fees_daily` NULL fees | 728 rows (all in June 2026 partial month) |
| `int_revenue_gnosis_app_fees_daily` total rows | 146,995 |
| `int_revenue_holdings_fees_daily` total rows | 30.6M |
| BRLA zero-balance rows after rounding | 917k of 2.99M (31%) |
| `int_revenue_sdai_fees_daily` zero-fee (dust) rows | 18.6M of 23.5M (79%) |
| `fct_revenue_active_users_totals_monthly` row count | 11 rows — Oct/Nov/Dec for 2023/2024/2025 + Apr/May 2026 |
| `fct_revenue_per_user_monthly` row count / months | 452k rows across 11 months |
| `int_revenue_fees_weekly_per_user` rows / max_week | 9.47M rows; max week 2026-06-02 (10 days behind today) |
| `fct_revenue_active_users_totals_monthly` 2026-05 users_cnt | 10,775 |
| Sub-threshold users in monthly cohorts (2026-05) | 16,936 (cohorts) vs 10,775 (totals) |
| Per-user monthly duplicate check | 0 duplicates (month, user_pseudonym unique) |
| Weekly intermediate partition count | 33 monthly partitions (toStartOfMonth, 2023-10 through 2026-06) |

---

## Pros / Cons

**Pros**
- Cross-stream deduplication is architecturally correct — fees are summed per user before thresholding, so double-counting is impossible in headline active-user metrics.
- Privacy boundary is well-enforced at the mart layer: `pseudonymize_address` macro applied consistently, raw addresses dropped, `tier3 + allow_unfiltered:false` on per-user endpoints.
- Weekly densification design (`arrayJoin` calendar + `ROWS BETWEEN 51 PRECEDING`) correctly anchors the 52-week rolling window regardless of user activity gaps.
- "Potential not realised" framing is explicitly documented in `schema.yml` header and propagated through semantic model descriptions, protecting consumers from misreading the unit as treasury accounting.
- Four-stream UNION ALL architecture is extensible — new streams (lending, when built) can be added to two intermediates without touching any downstream mart or semantic model.
- Grain uniqueness tests exist on all four daily intermediates and the monthly aggregate; config includes `lookback_days` guards to keep CI fast.
- Cohort bucket definitions are explicitly linked to the Dune `gnosis_rev_*` spec with intentional extensions documented, providing an auditable methodology trail.
- Pseudonym space is documented as compatible with GPay, Gnosis App, and Mixpanel hash spaces, enabling verified cross-sector joins via the semantic layer.

**Cons**
- Monthly pipeline has a critical coverage gap covering ~75% of history (Jan–Sep missing for 2023/2024/2025, Jan–Mar 2026 absent), making quarterly trend analysis and year-over-year comparisons entirely unreportable.
- `int_revenue_fees_weekly_per_user` uses a project-banned delete+insert strategy in normal incremental mode — live migration debt.
- Gnosis App stream has marts and API endpoints but no semantic model; `has_gnosis_app` dimension is absent from both approved-tier per-user semantic models.
- NULL USD fees from gnosis_app (728 rows, current month) and GPay GBPe (8 historical rows) propagate silently through the unified daily into weekly/monthly intermediates, understating active user counts.
- Cohort table includes sub-$0.50 users while totals table uses $0.50 floor — undocumented ~89k user-month discrepancy breaks naive cohort summation.
- All three `refill_append`-tagged daily models are missing the `refill_safe` pre/post hooks mandated by their own tag convention — OOM risk on any price-gap recovery refill.
- All 28 api_ views are globally allowlisted in `check_api_tags.allow` — entire revenue module is invisible to the CI convention guard.
- Hardcoded APY rates and 10% sDAI DAO share require SQL edits and full-history rebuilds for any adjustment, with no audit trail or effective-date mechanism.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| IMMEDIATE | Investigate GPay settlement address continuity: run a fee-volume query by month across April 2025 against `int_revenue_gpay_fees_daily` and confirm whether `0x4822521e6135cd2599199c83ea35179229a172ee` is still active or whether the Spender router change at `0xcff260bfbc199dc82717494299b1acade25f549b` also changed the fee-collection target. | `models/revenue/intermediate/int_revenue_gpay_fees_daily.sql` |
| IMMEDIATE | Run targeted backfill of `int_revenue_fees_monthly_per_user` for missing Jan–Sep periods (2023, 2024, 2025) and Jan–Mar 2026 using the existing `start_month`/`end_month` var mechanism. Verify `fct_revenue_active_users_totals_monthly` reaches 33+ rows before re-enabling monthly dashboard panels. | `models/revenue/intermediate/int_revenue_fees_monthly_per_user.sql`, all `fct_revenue_*_monthly` models |
| HIGH | Add `COALESCE(fees, 0)` guards in `int_revenue_gnosis_app_fees_daily` GROUP BY sum and in `int_revenue_fees_unified_daily`. Add a `not_null` test on `fees` in `int_revenue_fees_unified_daily` with a recent-window config to catch future price-gap regressions. | `models/revenue/intermediate/int_revenue_gnosis_app_fees_daily.sql`, `int_revenue_fees_unified_daily.sql` |
| HIGH | Add `refill_safe_pre_hook()` and `refill_safe_post_hook()` to the config blocks of `int_revenue_holdings_fees_daily`, `int_revenue_gpay_fees_daily`, and `int_revenue_sdai_fees_daily`. These models are already tagged `refill_append`; the hooks are the required complement per the macro contract. | `models/revenue/intermediate/int_revenue_holdings_fees_daily.sql`, `int_revenue_gpay_fees_daily.sql`, `int_revenue_sdai_fees_daily.sql` |
| HIGH | Add `has_gnosis_app` as a categorical dimension to both `revenue_per_user_weekly` and `revenue_per_user_monthly` semantic models. Add `revenue_gnosis_app_cohorts_weekly` and `revenue_gnosis_app_cohorts_monthly` semantic models to `semantic/authoring/revenue/semantic_models.yml`. | `semantic/authoring/revenue/semantic_models.yml` |
| MEDIUM | Document the cohort-vs-totals threshold split in `schema.yml`: note that cohorts floor at $0.01 while totals floor at $0.50, that sub-$0.50 buckets are for distribution visibility only, and that cohort `users_cnt` cannot be summed to reproduce headline totals. Add the same caveat to the affected semantic model descriptions. | `models/revenue/marts/schema.yml` |
| MEDIUM | Migrate `int_revenue_fees_weekly_per_user` from delete+insert to `insert_overwrite` to remove the project-ban violation. The existing `start_month`/batch slice mechanics are compatible; the only change is the config line and removal from `no_delete_insert.allow`. | `models/revenue/intermediate/int_revenue_fees_weekly_per_user.sql` |
| MEDIUM | Change sDAI `INNER JOIN` on rates to `LEFT JOIN` with a `COALESCE(fees_raw, 0)` guard and add a dbt source freshness assertion on `int_yields_sdai_rate_daily` so a rate-source gap surfaces as a CI warning rather than a silent row drop. | `models/revenue/intermediate/int_revenue_sdai_fees_daily.sql` |
| MEDIUM | Move holdings APY rates and sDAI DAO share to a dbt var or config seed with effective-date columns so rate changes are auditable and do not require full-history rebuilds without a record. | `models/revenue/intermediate/int_revenue_holdings_fees_daily.sql`, `int_revenue_sdai_fees_daily.sql` |
| LOW | Add `dbt_utils.unique_combination_of_columns` test on `int_revenue_fees_weekly_per_user` (week, stream_type, symbol, user) with the same recent-window config as the other daily intermediates. | `models/revenue/intermediate/int_revenue_fees_weekly_per_user.sql`, `intermediate/schema.yml` |
| LOW | Clear the `check_api_tags.allow` backlog for the revenue module by adding typed columns blocks to `marts/schema.yml`, prioritising the per-user endpoints (`api_revenue_per_user_weekly`, `api_revenue_per_user_monthly`) which carry `tier3` user-keyed data. | `models/revenue/marts/schema.yml`, `scripts/checks/check_api_tags.allow` |

---

## Open disagreements

None. All three inspectors converged with mutually consistent findings. The single genuinely open question — whether the GPay settlement address is still live post-April 2025 — requires a domain answer and a targeted warehouse query; it is not a disagreement between reviewers.

---

## Review log

| Round | Shard | Challenges issued | Outcome |
|---|---|---|---|
| 1 | intermediate | None issued to inspectors; context agent provided clarification on Dune spec alignment and GPay address | Resolved in the same round — context agent confirmed "potential not realised" framing and raised the settlement-address discrepancy as an open question |
| 1 | marts-1 | None | Converged; all findings consistent with intermediate shard |
| 1 | marts-2 | None | Converged; monthly coverage gap confirmed independently via warehouse queries |
