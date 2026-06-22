# Model review (revisit 2026-06-21): execution/accounts

Baseline `docs/model_review/execution-accounts.md` (2026-06-11); 21 cases re-verified over 3 rounds. Headline: the critical empty-fct outage and its 2 downstream propagation findings are RESOLVED via the June insert_overwrite incident recovery; 3 CHANGED (staleness symptoms cleared / risk mitigated at the view layer); 15 STILL CONFIRMED — including 3 unguarded high-severity correctness/freshness risks (no-FINAL RMT reads, silent-empty balance rebuild, non-reproducible old-cohort retention) and a total semantic-coverage gap for the whole unit.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | conf | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONACCOUNTS-C01 | P0-09 | `fct_execution_account_token_movements_daily` empty; 100% null propagation | critical | RESOLVED | resolved | high | microbatch_insert_overwrite | 3 |
| EXECUTIONACCOUNTS-C02 | | two `api_` views read RMT without FINAL; uniqueness test 7d-windowed | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONACCOUNTS-C03 | | `fct_execution_account_token_balances_latest` `today()-14` window rebuilds empty on >14d gap | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONACCOUNTS-C04 | | 90d/180d retention non-reproducible for old cohorts (181-day DAA filter) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONACCOUNTS-C05 | | dead `incr_end` / in_daily vs out_daily microbatch sibling inconsistency | high | CHANGED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C06 | | `fct_execution_address_resolver` AggregatingMergeTree over plain ints = last-write-wins | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONACCOUNTS-C07 | | retention self-ref cold-start NULL-collapse; latest cohort 10d stale | medium | RESOLVED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C08 | | `fct_execution_gnosis_app_user_profile_latest` gpay LEFT JOIN no `join_use_nulls` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONACCOUNTS-C09 | | counterparty daily `max(date)=date` no-op (harmless/misleading) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C10 | | `fct_execution_network_retention_monthly` missing `granularity:` tag | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C11 | | zero semantic-layer coverage for the whole accounts unit | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONACCOUNTS-C12 | | network retention fact stranded — no `api_` view, no metric registration | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONACCOUNTS-C13 | | `address_type` binary (safe vs eoa_or_contract) — no EOA/contract split | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONACCOUNTS-C14 | | native xDAI absent from balances/movements — `total_balance_usd` understated | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONACCOUNTS-C15 | | counterparty `_latest` graph has no recency window / top-N | low | CHANGED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C16 | | Gnosis App heuristic floored at `2025-11-12` relayer since_date | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C17 | | data finding: balance history 296.6M rows, 0 dups last 7d | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C18 | | data finding: in/out movement legs fresh & populated | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C19 | | data finding: token_balances_latest 384K rows, 0 zero-USD | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONACCOUNTS-C20 | | data finding: counterparty edges — token_transfer 0 (empty fct propagation) | medium | RESOLVED | resolved | high | microbatch_insert_overwrite | 3 |
| EXECUTIONACCOUNTS-C21 | | data finding: retention fact 93 cohorts, latest 10d stale | low | CHANGED | low | high | microbatch_insert_overwrite | 3 |

Rollup: 3 RESOLVED, 3 CHANGED, 15 STILL CONFIRMED, 0 NEW, 0 UNVERIFIABLE/UNRESOLVED.

## Delta vs baseline

### RESOLVED (3)
- **C01** — `models/execution/accounts/marts/fct_execution_account_token_movements_daily.sql`: empty fct (`0` rows at baseline) repopulated to `40,116,990` rows, `max_date 2026-06-20`. Verified not single-month-shaped — every month Jan–Jun 2026 carries full contiguous day-coverage (`31/28/31/30/31/20` days) with `dup_keys=0`. Profile propagation now real: `fct_execution_account_profile_latest` token_transfer_count positive on `670,442` of `1,632,466` rows (41%) vs baseline `0/1,318,764` (100% null). Incident: microbatch_insert_overwrite (June 2026 wipe recovery).
- **C20** — `models/execution/accounts/marts/fct_execution_account_counterparty_edges_latest.sql`: token_transfer edges `0 -> 534,184`, downstream of the C01 refill. Well-formed: `0` zero/negative weights, all `534,184` distinct `(source,target)`, `277,827` with June activity, `max(last_seen_date)=2026-06-20` (current, not a stale-only backfill). Incident: microbatch_insert_overwrite.
- **C07** — `models/execution/accounts/marts/fct_execution_network_retention_monthly.sql`: staleness symptom cleared — latest cohort `2026-05-01 (10d stale) -> 2026-06-01 (current)`. The cold-start self-ref NULL-collapse remains a latent code property (an empty `{{this}}` makes `max()` NULL → `date >= NULL` matches zero rows → all retention zero) but cannot trigger while the table is warm. Resolved with an open question on the cold-rebuild risk.

### CHANGED (3)
- **C05** — `int_execution_account_token_movements_in_daily.sql` / `..._out_daily.sql`: baseline "dead `incr_end` variable" premise is now stale — `incr_end` is captured (L3) and gates the strategy at L16 (`'append' if (start_month or incr_end) else 'insert_overwrite'`). Residual is sibling inconsistency: in_daily is microbatch-wired (tag `microbatch` L37) while out_daily is pure insert_overwrite (L7, no `microbatch` tag L28), so the two legs can diverge in coverage on a microbatch slice. Data-equal today (`20,645,855` rows each, both fresh to `2026-06-20`), so severity high → **low**.
- **C15** — `fct_execution_account_counterparty_edges_latest.sql` still has no recency window / top-N (top real address `~31,078` edges). But the consumer endpoint `api_execution_account_counterparty_graph.sql` (L8-16) sets `allow_unfiltered=false`, `require_any_of=['source']`, sorts `weight DESC`, and caps `max_limit=250`/page — so the unbounded all-history graph cannot reach the Relationships tab. Render-blowup mitigated at the view layer; severity returns to **low** (R2 had nudged it to medium).
- **C21** — `fct_execution_network_retention_monthly.sql`: latest cohort advanced `2026-05-01 -> 2026-06-01` (`93` cohorts, max rate `0.353`, none `>1.0`). `9` recent cohorts diverge across 30/90/180d while `83` old cohorts collapse to equal/zero rates — the C04 rolling-window artifact, cleanly separated from window-immaturity of the newest cohort. Incident: microbatch_insert_overwrite (rebuild advanced the cohort).

### STILL CONFIRMED (15)
- **C02** (high) — `api_execution_account_balance_history_daily.sql` (L22-30) and `api_execution_account_token_movements_daily.sql` (L25-36) both plain `SELECT` over RMT, no FINAL. The only uniqueness test sits on the fct (`schema.yml` L125-134), is windowed to `today()-7`, and `dbt_utils.unique_combination_of_columns` emits a plain `SELECT` — same pre-merge blind spot. Current observable dup exposure nil (`0` dups over 90d) only because background merges are complete.
- **C03** (high) — `fct_execution_account_token_balances_latest.sql` (L33-34, L48-49) `today()-14` window unchanged. Source fresh (`max_date=2026-06-20`, recent-14 window `5,401,511` rows, prior window `5,950,324`), so failure latent. `schema.yml` L96 model-level `tests:[]` plus only column `not_null` (vacuous on 0 rows) = no guard against a silent-empty rebuild.
- **C04** (high) — `int_execution_transactions_daily_active_addresses.sql` L15 still `WHERE d.date > subtractDays(today(),181)`. Blast radius sized: `83` of `86` cohorts older than 7 months carry `retention_rate_90d=0 OR retention_rate_180d=0` — ~89% of cohort history uncomputable/frozen on rebuild.
- **C06** (medium) — `fct_execution_address_resolver.sql` engine `AggregatingMergeTree()` (L4) over plain `UInt64/Int8` SELECT (L95-134) = last-write-wins. `1,129,933` rows vs `1,101,974` distinct addresses (`27,959` unmerged). `api_execution_address_resolver` GROUP BY address + max() collapses exactly: `view_rows=view_distinct=1,101,974` — the sole correctness guarantee.
- **C08** (medium) — `fct_execution_gnosis_app_user_profile_latest.sql` pre_hook (L8-16) has no `SET join_use_nulls=1`; gpay LEFT JOIN (L42-43). Proven in data: `n_ga_owners_current=0` for `22,849` rows, `IS NULL` for `0` of `24,020` — not-a-GPay-wallet vs zero-owner ambiguity is live. (Sibling `fct_execution_account_profile_latest` got the fix; this named model did not — corrects the verifier's R1 over-claim.)
- **C09** (low) — `fct_execution_account_counterparty_edges_daily.sql` `max(date) AS last_seen_date` over GROUP BY key `date` (L24/L33) = no-op. Empirically `0` mismatches over `606,960` June rows. Cosmetic.
- **C10** (low) — `fct_execution_network_retention_monthly.sql` L11 tags `['production','execution','accounts','monthly']` — no `granularity:` tag, no `api:` tag. `granularity:` is read only in `scripts/checks/` (which enforces only on api-tagged models), so it slips CI cleanly — invisibility to granularity routing, not breakage.
- **C11** (high) — `semantic/authoring/execution/accounts/` does not exist; grep of `semantic/` for accounts marts is empty; `preflight_analytics_request` returns `covered_topics=[]` (route `semantic_unavailable`, fallback `manifest_hash_mismatch`). Total governed-coverage gap for the unit.
- **C12** (medium) — no `api_execution_network_retention_monthly` view and no semantic registration; `discover_metrics('network retention monthly cohort')` returns only gpay/gnosis_app metrics — the fct does not surface. Reachable only by direct SQL.
- **C13** (medium) — `fct_execution_account_profile_latest.sql` L199 `multiIf(r.is_safe,'safe','eoa_or_contract')` — binary. `eoa_or_contract=1,003,432` (61.5%) vs `safe=629,034`. Grep for `is_contract/has_code/code_size/EOA` across the resolver and all `models/execution/` returns zero hits — the EOA/contract split is unavailable, not merely unused.
- **C14** (medium) — `int_execution_transfers_whitelisted_daily.sql` unions only ERC-20 Transfer logs + WxDAI events, no `native_transfers` union. `seeds/tokens_whitelist.csv` lists native xDAI `0xeeee...` but it can never match a Transfer log. `native_or_wrapped_xdai_balance` is populated (`60,590,076` rows >0) from WxDAI ONLY — `total_balance_usd` understates native-xDAI-primary holders; column name misleading.
- **C16** (low) — `seeds/gnosis_app_relayers.csv`: all three bundlers `since_date=2025-11-12`. Hard cliff confirmed: `min(first_seen_at)=2025-11-12 00:03:50`, `0` of `24,020` GA users pre-floor. Undocumented downward bias for pre-Nov-2025 cohorts.
- **C17** (low) — `int_execution_account_balance_history_daily`: `300,508,170` rows (up from `296,577,514`), `max_date=2026-06-20`, `426,269` distinct addresses, `0` dup `(address,date)` in last 7d (and over 90d). Healthy/fresh.
- **C18** (low) — in_daily `20,645,855` rows / out_daily `20,645,855` rows, both `max 2026-06-20`; distinct addresses differ (`660,557` vs `507,488`), `0` dup keys. Identical totals are a structural identity (one inbound + one outbound aggregate per transfer tuple), not a bug.
- **C19** (low) — `fct_execution_account_token_balances_latest`: `386,581` rows, `302,788` distinct addresses, `0` zero-USD (and `0` NULL, `0` negative), `max_date=2026-06-20`. Unpriced tokens correctly filtered.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None — all 21 cases reached agreed status at 3 rounds.

## Evidence appendix

**C01** — `SELECT toStartOfMonth(date) mon, count() rows, uniqExact(date) days, count()-uniqExact((address,date,counterparty,token_address,direction)) dup_keys FROM fct_execution_account_token_movements_daily WHERE date>='2026-01-01' GROUP BY mon`: Jan `1,171,039r/31d`; Feb `1,000,305r/28d`; Mar `1,014,044r/31d`; Apr `1,082,049r/30d`; May `981,673r/31d`; Jun `748,171r/20d`; `dup_keys=0` every month. Full-history `40,116,990` rows, `max_date 2026-06-20`. Profile: `670,442/1,632,466` token_transfer_count>0, `962,024` NULL, `0` exactly-zero.

**C02** — code: `api_execution_account_balance_history_daily.sql` L22-30 plain SELECT over int RMT (no FINAL); `api_execution_account_token_movements_daily.sql` L25-36 plain SELECT over fct RMT (no FINAL). `schema.yml`: api views carry only `not_null` (L79-86, L136-141); uniqueness test on the fct L125-134 windowed to `today()-7`, no FINAL. Data: balance_history dup over 90d = `0`; fct movements dup over June = `0`.

**C03** — `SELECT max(date) max_date, countIf(date>=today()-14 AND date<today()) recent14, countIf(date>=today()-30 AND date<today()-14) prior_window FROM int_execution_tokens_balances_daily WHERE date>=today()-30`: `max_date=2026-06-20`; `recent14=5,401,511`; `prior_window=5,950,324`. Code L33-34 & L48-49 bound scan to `date>=today()-14`. `schema.yml` L96 `tests:[]`.

**C04** — `SELECT countIf(cohort_month<subtractMonths(today(),7) AND (retention_rate_90d=0 OR retention_rate_180d=0)) old_frozen, countIf(cohort_month<subtractMonths(today(),7)) old_total FROM fct_execution_network_retention_monthly FINAL`: `old_frozen=83`; `old_total=86` (of 93 total). For cohort `2022-01`: `int_execution_transactions_daily_active_addresses` returns `0` rows in the 90d/180d window; fct stores `retention_rate_90d=retention_rate_180d=0`.

**C05** — code: in_daily L3 `mb_var` captures `incr_end`; L16 `incremental_strategy=('append' if (start_month or incr_end) else 'insert_overwrite')`; L61 `apply_monthly_incremental_filter`; L37 tags include `microbatch`. out_daily L7 pure insert_overwrite; no `incr_end`; L28 tags have NO `microbatch`. Data C18: both legs `20,645,855` rows, fresh to `2026-06-20`.

**C06** — `SELECT count(), uniqExact(address) FROM fct_execution_address_resolver`: `1,129,933` rows / `1,101,974` distinct. `SELECT count() view_rows, uniqExact(address) view_distinct FROM api_execution_address_resolver`: `1,101,974` / `1,101,974`. Engine `AggregatingMergeTree()` L4 over plain ints L95-134.

**C07** — `SELECT max(cohort_month) FROM fct_execution_network_retention_monthly FINAL` = `2026-06-01` (was `2026-05-01`). Code L60-65 (is_incremental branch): `date >= (SELECT toStartOfMonth(addDays(max(toDate(cohort_month)),-1)) FROM {{this}})`; empty `{{this}}` → `max()` NULL → predicate matches zero rows (latent cold-start collapse).

**C08** — `SELECT countIf(n_ga_owners_current=0) zero_owners, countIf(n_ga_owners_current IS NULL) null_owners, countIf(controlled_gpay_wallet IS NULL) null_wallet, count() total FROM fct_execution_gnosis_app_user_profile_latest`: `zero_owners=22,849`; `null_owners=0`; `null_wallet=22,784`; `total=24,020`. Pre_hook L8-16 has no `SET join_use_nulls=1`; gpay LEFT JOIN L42-43.

**C09** — `SELECT countIf(last_seen_date != date) mismatches, count() total FROM fct_execution_account_counterparty_edges_daily WHERE date>='2026-06-01'`: `mismatches=0`; `total=606,960`. token_edges CTE L24 `max(date) AS last_seen_date`, `date` in GROUP BY L33.

**C10** — code: `fct_execution_network_retention_monthly.sql` L11 tags `['production','execution','accounts','monthly']` (no `granularity:`, no `api:`). Grep: `granularity:` read only in `scripts/checks/migrate_api_tags.py` (L55/L89) and `check_api_tags.py` (enforces only on api-tagged models).

**C11** — `semantic/authoring/execution/accounts/` does not exist (only Circles, gnosis_app, gpay, lending, pools, prices, tokens, transactions, transfers, safe, state, etc.). Grep of `semantic/` for `fct_execution_account_*` / `balance_history` / `token_movements` / `network_retention` / `portfolio` → no matches. `preflight_analytics_request('account portfolio token balance history for an address')` → route `semantic_unavailable`, `covered_topics=[]`, `recommended_metrics=[]`, `fallback_reason=manifest_hash_mismatch`.

**C12** — glob `api_execution_network_retention*` → only the fct `.sql`. `marts/schema.yml` has no api entry. `discover_metrics('network retention monthly cohort')` → only gpay/gnosis_app metrics (`api_execution_gpay_cashback_cohort_retention_monthly`, `fct_execution_gnosis_app_gpay_topups_cohort_monthly`, etc.) — fct does not surface.

**C13** — `SELECT address_type, count() FROM fct_execution_account_profile_latest GROUP BY address_type`: `eoa_or_contract=1,003,432` (61.5%), `safe=629,034`. Code L199 `multiIf(r.is_safe,'safe','eoa_or_contract')`. Grep `is_contract/has_code/code_size/eoa/bytecode` across resolver + profile + `models/execution/` → zero hits.

**C14** — `SELECT sumIf(1, native_or_wrapped_xdai_balance>0) FROM int_execution_account_balance_history_daily`: `60,590,076`. Code: `int_execution_transfers_whitelisted_daily.sql` unions `raw_whitelisted_transfers` (execution.logs ERC-20 Transfer topic0, INNER JOIN `tokens_whitelist` excluding WxDAI, L16-74) + WxDAI events (`contracts_wxdai_events`, L91-167); NO native_transfers union. `seeds/tokens_whitelist.csv` lists `0xeeee...` (xDAI) but it can never match a Transfer log.

**C15** — code: `fct_execution_account_counterparty_edges_latest.sql` no date/recency filter, no top-N (aggregates all history). `api_execution_account_counterparty_graph.sql` L8-16: `allow_unfiltered=false`, `require_any_of=['source']`, sort `weight DESC`, `default_limit=60`/`max_limit=250`, plus `as_of_date`. Underlying token_transfer edges `534,184`; top real address `~31,078`.

**C16** — `SELECT min(first_seen_at), countIf(first_seen_at<'2025-11-12') pre_floor, countIf(first_seen_at>='2025-11-12') post_floor, count() FROM fct_execution_gnosis_app_user_profile_latest WHERE first_seen_at IS NOT NULL`: `min=2025-11-12 00:03:50`; `pre_floor=0`; `post_floor=24,020`; `total=24,020`. `seeds/gnosis_app_relayers.csv` all 3 bundlers `since_date=2025-11-12`.

**C17** — `SELECT count(), max(date), uniqExact(address), (... dup_7d) FROM int_execution_account_balance_history_daily`: `300,508,170` rows; `max_date=2026-06-20`; `426,269` distinct; `dup_7d=0` (and `0` over 90d / `26,030,123` rows).

**C18** — `SELECT 'in', count(), max(date), uniqExact(address) ... UNION ALL 'out' ...`: in `20,645,855` / `2026-06-20` / `660,557`; out `20,645,855` / `2026-06-20` / `507,488`. Both `0` dup keys; range `2020-07-01..2026-06-20`.

**C19** — `SELECT count(), uniqExact(address), countIf(balance_usd=0), max(date) FROM fct_execution_account_token_balances_latest`: `386,581` rows; `302,788` distinct; `0` zero-USD (`0` NULL, `0` negative); `max_date=2026-06-20`.

**C20** — `SELECT edge_type, count(), max(last_seen_date), countIf(last_seen_date>='2026-06-01') FROM fct_execution_account_counterparty_edges_latest GROUP BY edge_type`: token_transfer `534,184` (was `0`), `max_seen=2026-06-20`, June `277,827`, `0` zero-weight, `11,302` `raw_volume=0` (legit zero-value transfers), `0` dup `(source,target)`. Others: safe_relation `1,545,365`; circles_trust `699,330`; gpay_activity `200,608`; validator_relation `2,628`.

**C21** — `SELECT count(), min(cohort_month), max(cohort_month), max(retention_rate_180d), countIf(retention_rate_180d>1.0), countIf(... diverging) FROM fct_execution_network_retention_monthly FINAL`: cohorts `93`; earliest `2018-10-01`; latest `2026-06-01`; max_rate `0.3532`; over_one `0`; diverging `9` (vs `83` old cohorts collapsed).

## Review log (>=3 rounds per case)

- **C01**: R1 RESOLVED (fct populated, June 748,171r/20d, profile 41% positive) → challenge: verify grain integrity + propagation not a join artifact → R2 answered (40,116,990r, 0 dup keys, 962,024 NULLs are no-history addresses) → challenge: confirm recovery not June-shaped → R3 answered (Jan–Jun 31/28/31/30/31/20 days, 0 dup_keys every month). Final RESOLVED.
- **C02**: R1 CONFIRMED (code-only, both views no FINAL) → challenge: quantify real dup exposure beyond 7d → R2 (0 dups over 90d; risk transient) → challenge: does the uniqueness test guard it → R3 (test on fct only, 7d-windowed, plain SELECT = same blind spot; api views only not_null). Final CONFIRMED/high.
- **C03**: R1 CONFIRMED (today()-14 window present) → challenge: demonstrate silent-empty + no guard → R2 (tests:[], not_null vacuous on 0 rows) → challenge: show numerically + no source-freshness check → R3 (recent14=5,401,511, prior=5,950,324, latent). Final CONFIRMED/high.
- **C04**: R1 CONFIRMED (medium conf, code-only) → challenge (insufficient): prove empirically with stored rate vs 0 source rows → R2 (cohort 2022-01: 0 source rows, fct rate 0) → challenge: bound blast radius → R3 (83/86 old cohorts frozen, 93 total). Final CONFIRMED/high.
- **C05**: R1 CHANGED (incr_end now gates strategy; sibling inconsistency persists) → challenge: quote exact meta/tags on both siblings → R2 CONFIRMED (in_daily microbatch meta, out_daily none) → challenge: confirm runner drives only in_daily; re-frame severity → R3 CHANGED (legs can diverge on a slice; data-equal today → low). Final CHANGED/low.
- **C06**: R1 CONFIRMED (1,129,933 vs 1,101,974; comment now acknowledges max semantics) → challenge: prove api view collapses to 1 row/address → R2 (api GROUP BY address) → challenge: actually run the view → R3 (view_rows=view_distinct=1,101,974). Final CONFIRMED/medium.
- **C07**: R1 CHANGED (cohort advanced to 2026-06-01; cold-start latent) → challenge: quantify the 1-day-lookback drop → R2 CHANGED (no zero-collapse on closed cohorts; drop sub-month/small) → challenge: demonstrate cold NULL-collapse via compiled SQL → R3 RESOLVED (latent-only, table warm; open question on cold rebuild). Final RESOLVED/low.
- **C08**: R1 RESOLVED (verifier over-claim — read sibling model) → orchestrator correction to CHANGED: named model lacks pre-hook → R2 CONFIRMED (pre_hook L8-16 no join_use_nulls; columns g.n_ga_owners_current etc.) → challenge: prove ambiguity in data → R3 (zero_owners=22,849, null_owners=0). Final CONFIRMED/medium.
- **C09**: R1 CONFIRMED (code no-op) → challenge: close empirically → R2 (analytic, deferred query) → challenge: spend one cheap query → R3 (0 mismatches over 606,960 rows). Final CONFIRMED/low.
- **C10**: R1 CONFIRMED (no granularity: tag) → challenge: confirm CI guard only checks api-tagged → R2 (check_api_tags.py skips non-api) → challenge: confirm query-time routing skips, not errors → R3 (granularity: read only in scripts/checks/; invisibility not breakage). Final CONFIRMED/low.
- **C11**: R1 CONFIRMED (dir absent, preflight semantic_unavailable) → challenge: rule out manifest_hash_mismatch masking; grep independently → R2 (filesystem+grep clean) → challenge: confirm gap is total across other semantic dirs → R3 (no accounts mart referenced anywhere; covered_topics=[]). Final CONFIRMED/high.
- **C12**: R1 CONFIRMED (no api_ view, no registration) → challenge: confirm reachability from consumer side → R2 (find/schema/grep) → challenge: use discover_metrics → R3 (discover_metrics does not surface the fct). Final CONFIRMED/medium.
- **C13**: R1 CONFIRMED (L199 binary) → challenge: quantify the conflated bucket → R2 (eoa_or_contract=1,003,432, 61.5%) → challenge: confirm no upstream is_contract signal → R3 (grep zero hits → not delivered). Final CONFIRMED/medium.
- **C14**: R1 CONFIRMED (native xDAI absent, only WxDAI) → challenge: trace to root → R2 (int_execution_transfers_whitelisted_daily has no native_transfers union) → challenge: reconcile native_or_wrapped_xdai_balance column → R3 (populated from WxDAI ONLY; native omitted, name misleading). Final CONFIRMED/medium.
- **C15**: R1 CONFIRMED (no recency/top-N on _latest) → challenge: size render risk → R2 (top real addr 31,078 edges; nudged to medium) → challenge: check api view caps → R3 CHANGED (api_execution_account_counterparty_graph caps to source-filtered max 250/page → mitigated, back to low). Final CHANGED/low.
- **C16**: R1 CONFIRMED (seed floor 2025-11-12) → challenge: quantify monthly discontinuity → R2 (seed floor unambiguous, query deferred) → challenge: run the discontinuity query → R3 (min first_seen_at=2025-11-12 00:03:50, 0 pre-floor of 24,020). Final CONFIRMED/low.
- **C17**: R1 CONFIRMED (300.5M rows, 0 dups 7d) → challenge: extend dup check to 90d → R2 (0 dups over 90d / 26M rows) → R3 re-measured (300,508,170r, dup_7d=0). Final CONFIRMED/low.
- **C18**: R1 CONFIRMED (both legs fresh) → challenge: identical row counts suspicious, re-run separately → R2 (structural identity, distinct addrs differ) → R3 re-measured (in/out 20,645,855 each). Final CONFIRMED/low.
- **C19**: R1 CONFIRMED (386,581r, 0 zero-USD) → challenge: confirm NULL/negative also absent → R2 (0 NULL, 0 negative) → R3 re-measured (386,581r, 0 zero-USD). Final CONFIRMED/low.
- **C20**: R1 RESOLVED (token_transfer 0 → 534,184) → challenge: confirm edges well-formed not garbage → R2 (0 zero-weight, all distinct, 11,302 raw_volume=0 are legit) → challenge: confirm not stale-only backfill → R3 (max_seen 2026-06-20, 277,827 June edges). Final RESOLVED.
- **C21**: R1 CHANGED (latest cohort 2026-05-01 → 2026-06-01) → challenge: verify recent-cohort rates sensible → R2 CONFIRMED (window-immaturity vs C07 cold-start; 30/90/180 equality is C04 artifact) → challenge: separate diverging vs collapsed in the record → R3 CHANGED (9 diverge, 83 old collapse). Final CHANGED/low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| DROP | Empty-fct outage recovered — close the P0-09 propagation finding (40.1M rows, contiguous, 0 dup keys); keep monitoring the microbatch incident class | `models/execution/accounts/marts/fct_execution_account_token_movements_daily.sql`, `..._profile_latest.sql` |
| DROP | token_transfer counterparty edges repopulated (`534,184`, current to June) — finding closed | `models/execution/accounts/marts/fct_execution_account_counterparty_edges_latest.sql` |
| DROP | Counterparty graph render-blowup is mitigated at the api layer (source-filtered, `max_limit=250`/page) — no action needed beyond optionally adding an internal recency window | `api_execution_account_counterparty_graph.sql`, `fct_execution_account_counterparty_edges_latest.sql` |
| ESCALATE | Add FINAL (or a `final=true` ref / dedup) to both api_ views over RMT; the only uniqueness test is fct-side, 7d-windowed, and runs without FINAL — fix the test too | `api_execution_account_balance_history_daily.sql`, `api_execution_account_token_movements_daily.sql`, `schema.yml` (fct uniqueness test) |
| ESCALATE | Remove/raise the `today()-14` max(date) lookback and add an error-raising row-count / source-freshness guard so a >14-day upstream gap cannot silently rebuild the balances table empty | `fct_execution_account_token_balances_latest.sql`, `marts/schema.yml` |
| ESCALATE | Old-cohort 90d/180d retention is non-reproducible on rebuild (~89% of cohorts frozen) — either persist computed rates incrementally or source retention from a non-windowed activity table | `fct_execution_network_retention_monthly.sql`, `int_execution_transactions_daily_active_addresses.sql` |
| ESCALATE | Author semantic-layer coverage for the accounts unit (balance, first_seen_date, token movements, retention) — currently zero governed coverage, preflight falls back to raw SQL | `semantic/authoring/execution/accounts/` (to create) |
| KEEP | Add `SET join_use_nulls=1` pre-hook to the gpay LEFT JOIN so non-GPay rows return NULL not 0 (`22,849` rows currently ambiguous) | `fct_execution_gnosis_app_user_profile_latest.sql` |
| KEEP | Add an `api_execution_network_retention_monthly` view + metric registration, or document the fact as internal-only (currently unreachable via REST/MCP) | `fct_execution_network_retention_monthly.sql`, `marts/schema.yml`, semantic registry |
| KEEP | Deliver a real EOA-vs-contract split (no `is_contract`/`has_code` signal exists upstream); until then rename `eoa_or_contract` and document the binary safe/non-safe classification (`1,003,432` conflated) | `fct_execution_account_profile_latest.sql`, `fct_execution_address_resolver.sql` |
| KEEP | Include native xDAI (union `native_transfers`) in `int_execution_transfers_whitelisted_daily` or rename `native_or_wrapped_xdai_balance` to reflect WxDAI-only coverage; `total_balance_usd` understates native holders | `int_execution_transfers_whitelisted_daily.sql`, `fct_execution_account_token_balances_latest.sql`, `fct_execution_account_token_movements_daily.sql` |
| KEEP | Replace `AggregatingMergeTree()` with `ReplacingMergeTree()` (or emit real AggregateFunction states) on the resolver; the engine is misnamed and correctness rests solely on the api view's GROUP BY+max | `fct_execution_address_resolver.sql` |
| KEEP | Align the in/out movement siblings — give `out_daily` the same `incr_end`/microbatch wiring as `in_daily`, or drop microbatch from both, to prevent coverage divergence on a slice | `int_execution_account_token_movements_in_daily.sql`, `int_execution_account_token_movements_out_daily.sql` |
| KEEP (low) | Add a `granularity:monthly` tag to the retention model for granularity-routed tooling | `fct_execution_network_retention_monthly.sql` |
| KEEP (low) | Document the Gnosis App relayer floor (`since_date=2025-11-12`, 0 pre-floor users) as a known limitation on GA cohort/profile counts | `seeds/gnosis_app_relayers.csv`, `fct_execution_gnosis_app_user_profile_latest.sql` |
| KEEP (low) | Drop or comment the cosmetic `max(date) AS last_seen_date` no-op in the daily counterparty CTE | `fct_execution_account_counterparty_edges_daily.sql` |
| KEEP (low/latent) | Note the retention model's cold-start NULL-collapse — guard the self-ref lookback against an empty `{{this}}` to prevent an all-zero cold full-rebuild | `fct_execution_network_retention_monthly.sql` |
