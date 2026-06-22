# Model review (revisit 2026-06-21): execution/Circles

Re-verification of baseline `docs/model_review/execution-circles.md` (dated `2026-06-11`) over `3` rounds, covering all `28` cases plus `0` new findings: `3` resolved, `9` changed (mostly severity downgrades), and `16` still confirmed — the one `critical` (v1 transfers schema drift) and four of the five `high` cases hold, while most `medium` partition/engine concerns re-scoped down to `low` after data exhibits.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONCIRCLES-C01 | - | v1_transfers SELECT emits 9 cols, deployed/schema carry 13; full-refresh breaks v1_balance_diffs | critical | CONFIRMED | `critical` | high | none | 3 |
| EXECUTIONCIRCLES-C02 | - | wrapper_share_daily sawtooth: wrapped_supply=0 on no-event days via coalesce-to-0 | high | CONFIRMED | `high` | high | none | 3 |
| EXECUTIONCIRCLES-C03 | - | is_gnosis_app_tx NULL (not 0) via toUInt8 over nullable tx_hash on LEFT JOIN | high | CHANGED | `medium` | high | none | 3 |
| EXECUTIONCIRCLES-C04 | - | six v1 models dev-tagged, ~80d stale, excluded from prod tag gate | high | CONFIRMED | `high` | high | none | 3 |
| EXECUTIONCIRCLES-C05 | - | v1_trust_relations delete+insert on valid_from cannot close prior-partition open intervals | high | CHANGED | `medium` | medium | none | 3 |
| EXECUTIONCIRCLES-C06 | - | crc20_prices_daily missing api:/granularity:/tier: CI tags | high | CHANGED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C07 | - | human_avatars_distinct fact build-depends on api_ view (layer inversion) | high | CONFIRMED | `high` | high | none | 3 |
| EXECUTIONCIRCLES-C08 | - | three cnt_latest views divide by bare p.value, CROSS JOIN unguarded prior | medium | CONFIRMED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C09 | - | orgs_cnt_latest live view missing as_of_date column | medium | RESOLVED | `resolved` | high | none | 3 |
| EXECUTIONCIRCLES-C10 | - | full-rebuild facts approach CH 100-partition cap (error 252) | medium | CHANGED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C11 | - | two production intermediates (referrers, trust_pair_ranges) have zero schema.yml/tests | medium | CONFIRMED | `medium` | high | none | 3 |
| EXECUTIONCIRCLES-C12 | - | five snapshot fct_ tables declare table with no engine=/order_by= | medium | CHANGED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C13 | - | economically_active_avatars_weekly RMT on full-rebuild, dup risk | medium | CHANGED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C14 | - | avatars_current unguarded passthrough (no avatar IS NOT NULL / date<today / as_of_date) | medium | CHANGED | `medium` | high | none | 3 |
| EXECUTIONCIRCLES-C15 | - | tokens_supply_daily documented as table but materialized view | medium | CHANGED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C16 | - | crc20_prices fct_ RMT read without FINAL, unweighted avg vs api VWAP | low | CONFIRMED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C17 | - | avatar_trusts/active_trusts fct_ calendars extend to today() | low | CONFIRMED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C18 | - | human_avatars_distinct lacks expose_to_mcp/privacy_tier (overlaps C26) | low | CONFIRMED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C19 | - | four 7d KPI tiles disagree by one day (>=/< vs >/<=) | high | CONFIRMED | `high` | high | none | 3 |
| EXECUTIONCIRCLES-C20 | - | three semantic models bind non-existent columns; MCP query fails at bind | high | CONFIRMED | `high` | high | none | 3 |
| EXECUTIONCIRCLES-C21 | - | groups_overview_daily header documents n_groups_total it never emits | medium | CHANGED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C22 | - | mint_kind migration=10,808 contradicts "4 migrate() calls" comment | medium | RESOLVED | `resolved` | high | none | 3 |
| EXECUTIONCIRCLES-C23 | - | active_avatars_weekly omits UpdateMetadataDigest + StreamCompleted, undercounts WAU | medium | CONFIRMED | `medium` | high | none | 3 |
| EXECUTIONCIRCLES-C24 | - | mint_events personal-classifier doc-vs-code contradiction | medium | RESOLVED | `resolved` | high | none | 3 |
| EXECUTIONCIRCLES-C25 | - | crc20_prices price_median_usd is median-of-pool-medians (biased), undocumented | medium | CHANGED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C26 | - | human_avatars_distinct pseudonymizes into cross-sector space, MCP-discoverable, no privacy controls | medium | CONFIRMED | `medium` | high | none | 3 |
| EXECUTIONCIRCLES-C27 | - | relayer-launch floor 2025-11-12 hardcoded in two models | low | CONFIRMED | `low` | high | none | 3 |
| EXECUTIONCIRCLES-C28 | - | invite funnel mixes 30-day vs lifetime horizons without doc caveat | low | CONFIRMED | `low` | high | none | 3 |

Rollup: `13` confirmed, `3` resolved, `9` changed, `0` unverifiable, `0` new. No incident attribution on any case.

## Delta vs baseline

### RESOLVED (3)
- **C09** — `models/execution/Circles/marts/api_execution_circles_v2_orgs_cnt_latest.sql`: deployed view now returns `as_of_date` (live: `total=2096, change_pct=0.9, as_of_date=2026-06-21`, 3 columns). The missing-column defect is genuinely deployed-fixed; residual bare-`p.value` divide is theoretical (org count is monotonic cumulative, never 0). No incident.
- **C22** — `models/execution/Circles/intermediate/int_execution_circles_v2_mint_events.sql`: the "4 historical migrate() calls" comment was removed and the header rewritten to describe migration mints as transfer-derived from `from=0x0` legs whose operator is the single Circles V2 Migration contract. Live `mint_kind`: migration `10,816` over `3,705` distinct recipients (plausible multi-leg-per-avatar; exactly one Migration contract `0xd44b8dcfbadfc78ea64c55b705bfc68199b56376` in the registry, so the join is not over-broad). Code-vs-comment contradiction gone.
- **C24** — `models/execution/Circles/intermediate/int_execution_circles_v2_mint_events.sql`: the personal-mint classifier is now event-sourced (`event_name='PersonalMint'`); both the model header and `schema.yml` describe the PersonalMint-event basis. No residual `avatar_type='Human'` or `token_address=to_address` personal-classifier language remains. Doc-vs-code contradiction fully resolved.

### CHANGED (9, mostly severity downgrades after data exhibits)
- **C03** (high -> `medium`) — `int_execution_circles_v2_inviter_fees.sql`: NULL-propagation defect intact (`102,315` flagged 1, `1,274` NULL, `0` flagged 0). But the baseline's WEAU-drop mechanism is wrong: the downstream uses `max(is_gnosis_app_tx) AS any_in_app_tx`, not `IN (0,1)`, so WEAU is unaffected. Realized harm sized at `82` earner-weeks across `79` distinct avatars dropped from the in-app earner subset via `int_execution_gnosis_app_weekly_earners.sql:44` (`any_in_app_tx=1` over a Nullable column).
- **C05** (high -> `medium`) — `int_execution_circles_v1_trust_relations.sql`: the open-interval inflation is real in CODE for incremental delete+insert builds but LATENT in the current contiguously-built dev table (the `lead()` chaining is correct across month boundaries). The deployed table instead shows a separate RMT-read-without-FINAL ~`1.77x` inflation (`no_final=2,153,699` vs `with_final=1,217,769` vs `distinct_pairs=1,086,505`). Both consumers are v1/dev-tagged and out of prod scope (C04).
- **C06** (high -> `low`) — `api_execution_circles_v2_crc20_prices_daily.sql`: the "CI flags 3 rules" claim is false. `check_api_tags.py:56-57` does `if not api: continue`, so a model with no `api:` tag is skipped entirely (0 rules fire). Re-scoped to an unguarded tagging gap: an `api_*`-named production model not registered as an api endpoint, with no name-prefix CI rule to catch it.
- **C10** (medium -> `low`) — `fct_execution_circles_v2_avatar_balances_daily.sql`: `21` monthly partitions `<< 100`, so error 252 is not imminent (~79 months runway at ~1 partition/month). `int_execution_circles_v2_trust_pair_ranges` is partitionless (`engine MergeTree()` no `partition_by`), so a single full-rebuild insert lands in 1 partition — rows-per-insert, not a 252 risk at all. Residual is full-rebuild cost only.
- **C12** (medium -> `low`) — five snapshot fct_ models declare `materialized='table'` with no `engine=`/`order_by=`. CH dbt adapter default is `MergeTree ORDER BY tuple()` (deterministic, non-duplicating, NOT ReplacingMergeTree), so no dup hazard — a consistency/style gap vs siblings, not a correctness risk.
- **C13** (medium -> `low`) — `fct_execution_circles_v2_economically_active_avatars_weekly.sql`: RMT on a full-rebuild table whose final SELECT already `GROUP BY`s `(week, earning_kind)`. `GROUP BY week, earning_kind HAVING count()>1` returns `0` rows — RMT is pointless on an already-unique grain, not an active duplication.
- **C14** (medium -> `medium`, now proven ACTIVE) — `api_execution_circles_v2_avatars_current.sql`: unguarded passthrough confirmed in code. Live: `null_avatars=0, max_ts=2026-06-21 06:31:55, today_rows=4, total=27,593`. `today_rows>0` means the missing `date<today()` guard is ACTIVELY leaking `4` in-flight same-day registration rows that every other snapshot api_ view excludes. NULL-avatar leg currently latent.
- **C15** (medium -> `low`) — `fct_execution_circles_v2_tokens_supply_daily.sql`: still `materialized='view'`, but `schema.yml` (`marts/schema.yml:879`) now explicitly documents it as a "Compatibility view ... column-level tests and shape live on the intermediate model". Doc-vs-materialization contradiction resolved; residual is the convention deviation (fct_ as a view).
- **C21** (medium -> `low`) — `int_execution_circles_v2_groups_overview_daily.sql`: the baseline's "MCP column-not-found" mechanism is wrong. The api_ view emits `n_groups_total` via `sum(n_new_groups) OVER (...UNBOUNDED PRECEDING...)` and the semantic entity binds the api_ view, so MCP does not error. Real defect is only the intermediate header documenting `n_groups_total` while its SELECT omits it (doc-drift).
- **C25** (medium -> `low`) — `api_execution_circles_v2_crc20_prices_daily.sql`: `price_median_usd = median(price_median_usd)` across pools (unweighted) confirmed at line 18. For the 3-pool token the gap vs the VWAP is only ~`0.5%` (`0.008179` median-of-medians vs `0.008138` VWAP), so the statistical-bias concern is real in principle but negligible in magnitude. Documentation-only.

### STILL CONFIRMED (16)
- **C01** (`critical`) — `int_execution_circles_v1_transfers.sql:52-62` final SELECT emits `9` columns; live `describe_table` shows `13`; `schema.yml:341` documents `13`; `int_execution_circles_v1_balance_diffs.sql:21,24,27` SELECTs `batch_index`/`token_id`/`transfer_type`. A full-refresh recreates the 9-col table and breaks balance_diffs (`UNKNOWN_IDENTIFIER`) plus the schema column test. Gated behind the same dev exclusion as C04 (balance_diffs tagged `dev` at line 9), so latent until v1 is promoted — but the defect is intact and severity holds.
- **C02** (`high`) — `api_execution_circles_v2_wrapper_share_daily.sql`: `57` of `615` days have `wrapped_supply=0`, all with `total_supply>0`. Direct exhibit for 2024-10-19..25 shows interior dips (`10-21`, `10-23`, `10-25` each `=0` sandwiched between `14.99`), proving true no-event-day sawtooth, not leading pre-first-wrap zeros. No forward-fill present.
- **C04** (`high`) — six v1 models tagged `dev`; `scripts/run_dbt_observability.sh:217,222` run `--select tag:production` and the batch array (lines 90-103) is exclusively `tag:production,path:...`, so the dev-only v1 stack is never built/tested in prod. Load-bearing gate; v1_transfers frozen at `2026-04-02` / balances at `2026-04-01`.
- **C07** (`high`) — `fct_execution_circles_human_avatars_distinct.sql:43` `FROM ref('api_execution_circles_v2_avatar_metadata')`. The api_ view is a passthrough over `int_execution_circles_v2_avatar_metadata` and does not ref the fct_ (no cycle) — a clean-but-inverted layer edge: a semantic-layer fact build-depends on a dashboard-facing view.
- **C08** (`low`) — three cnt_latest views divide by bare `p.value` (no nullIf), CROSS JOIN prior keyed on exact `(max-7)` date. Live each returns one finite row (`groups 0.5, humans 3.5, active_trusts 7.1`). Failure mode is zero-rows-on-gap (latent under contiguous data); cumulative counts mean inf-divide is theoretical.
- **C11** (`medium`) — `int_execution_circles_v2_referrers` and `int_execution_circles_v2_trust_pair_ranges` have zero `schema.yml` entries; both production-tagged and feed ARRAY JOIN-exploding downstream facts. trust_pair_ranges grain is clean (`count=uniqExact(truster,trustee)=511,129`), so the missing unique test is latent, not masking dupes.
- **C16** (`low`) — `fct_execution_circles_v2_crc20_prices_daily.sql:4` RMT, `:21` `price_avg_in_backing=avg(...)` (unweighted); api_ view reads it without FINAL. Quantified ~`33%` naive-cross-pool-mean divergence vs the api VWAP for a 3-pool token (near-zero outlier pool `0.000077` drags the mean to ~`0.00545` vs VWAP `0.008138`). Low because the api_ VWAP is the published headline.
- **C17** (`low`) — both fct_ calendars use `today()` upper bound (`avatar_trusts_daily.sql:80`, `active_trusts_daily.sql:47`); api_ views filter `date<today()`. active_trusts_daily carries a partial `2026-06-21` row (`415,387` vs `414,746` yesterday) a direct fct_ consumer would misread.
- **C18** (`low`) — `fct_execution_circles_human_avatars_distinct` carries no `expose_to_mcp:false`/`privacy_tier`, sits under `models/execution/Circles/marts/` outside the `dbt_project.yml` mixpanel_ga exclusion subtree (lines 70-114), yet is semantic-layer discoverable (see C26). Low here; C26 carries the substantive medium.
- **C19** (`high`) — `kpi_mints_7d.sql:18-19` and `kpi_new_trusts_7d.sql:17-18` use `date>=today()-7 AND date<today()`; `kpi_new_backers_7d.sql:15` and `kpi_new_groups_7d.sql:17` use `date>today()-7 AND date<=today()`. The `>=/<` pair includes `2026-06-14` the `>/<=` pair excludes — genuine one-day span mismatch between same-named tiles (live: mints_7d `44,152`, new_trusts_7d `29,298`, new_backers_7d `23`, new_groups_7d `3`; as_of dates `06-20` vs `06-19` also diverge).
- **C20** (`high`) — `semantic/authoring/execution/Circles/semantic_models.yml`: live `describe_table` proves `int_execution_circles_v1_avatars` lacks `user_address`/`inviter_address`; `int_execution_circles_v2_avatars` lacks `date,event_name,from_address,to_address,registration_event_id,source_table,cnt` (has `avatar_type,invited_by,avatar,name` instead); `int_execution_circles_backing` lacks `date,cnt`. Any compiled metric query fails at bind time. Live runtime error masked by `manifest_hash_mismatch`, but column-absence is schema-proven.
- **C23** (`medium`) — `int_execution_circles_v2_active_avatars_weekly.sql` UNION omits `StreamCompleted` and `UpdateMetadataDigest`; the header still claims they are "not yet exposed" though both ARE decoded (`StreamCompleted=403,429`, `UpdateMetadataDigest=58,238` rows). Quantified WAU undercount of `3.2%`-`5.1%` (`245`-`356` net-new avatars/week over the last 6 weeks). Stale rationale + now-quantified undercount.
- **C26** (`medium`) — `fct_execution_circles_human_avatars_distinct.sql:34` `pseudonymize_address('avatar')` (sipHash64 into the shared cross-sector pseudonym space), mixpanel-tagged, no `expose_to_mcp:false`/`privacy_tier`, outside the mixpanel_ga exclusion. `discover_metrics` returns `circles_distinct_human_users` and `circles_humans_with_ipfs_profile` (root_model this fct, `quality_tier=approved`) — an ACTIVE cross-sector re-identification exposure via the semantic layer.
- **C27** (`low`) — `toDateTime('2025-11-12')` hardcoded at `int_execution_circles_v2_inviter_fees.sql:56` and `int_execution_circles_v2_referrers.sql:43` (exactly 2 occurrences). A matching var `gnosis_app_wau_floor_date='2025-11-12'` already exists (`dbt_project.yml:20`, used by gnosis_app models) but not these two; right fix is to point them at it.
- **C28** (`low`) — `api_execution_circles_v2_invite_funnel_cohort_monthly.sql:32-33`: stages 2-4 use `countIf(n_mint_days_first_30d>=k)` (first-30-day window), stage 5 uses `countIf(became_active_minter_at IS NOT NULL)` (lifetime). Live inversion exhibit: 2026-05 cohort `n_minted_14_days=176` but `n_active_minter=426`; 2026-04 `56` vs `273` — impossible for a true monotonic funnel, visibly exposing the misreadable horizon mix. No schema.yml caveat. Doc-only.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None. All `28` cases reached >= 3 rounds with sufficient evidence.

## Evidence appendix

**C01** — `describe_table int_execution_circles_v1_transfers` -> 13 cols (incl `batch_index UInt8, operator, token_id, transfer_type`); SQL final SELECT (`int_execution_circles_v1_transfers.sql:52-62`) -> 9 cols; `schema.yml:341` documents 13; `int_execution_circles_v1_balance_diffs.sql:21,24,27` SELECTs `batch_index`/`token_id`/`transfer_type`; balance_diffs tagged `dev` (line 9).

**C02** — `SELECT countIf(wrapped_supply=0), count(), max(date), countIf(wrapped_supply=0 AND total_supply>0) FROM dbt.api_execution_circles_v2_wrapper_share_daily` -> `57, 615, 2026-06-20, 57`. Direct: `... WHERE date>='2024-10-10' AND date<='2024-10-25'` -> `10-20=14.99, 10-21=0, 10-22=14.99, 10-23=0, 10-24=14.99, 10-25=0`.

**C03** — `SELECT countIf(is_gnosis_app_tx IS NULL), countIf(=1), countIf(=0), count() FROM dbt.int_execution_circles_v2_inviter_fees` -> `1,274 / 102,315 / 0 / 103,589`. Subset impact query (GA users joined to economically_active_avatars_weekly, qualifying vs dropped) -> `82` earner-weeks across `79` distinct avatars fully dropped from the in-app earner subset.

**C04** — grep `tags=['dev',...]` on six v1 model configs (transfers.sql:9, balance_diffs.sql:9, trust_relations.sql:10, etc.); `scripts/run_dbt_observability.sh:217,222` `--select tag:production`; batch array lines 90-103 exclusively `tag:production,path:...`. `SELECT toDate(max(block_timestamp)) FROM int_execution_circles_v1_transfers` -> `2026-04-02`.

**C05** — `int_execution_circles_v1_trust_relations.sql:4` `incremental_strategy=('append' if start_month else 'delete+insert')`, `:7` `unique_key='(transaction_hash, log_index)'`, `:8` `partition_by=toStartOfMonth(valid_from)`, engine RMT; `valid_to` via `lead() OVER (PARTITION BY truster,trustee)` (lines 28-31). Data: `no_final=2,153,699`, `with_final=1,217,769`, `distinct_pairs=1,086,505`.

**C06** — `api_execution_circles_v2_crc20_prices_daily.sql:7` `tags=['production','execution','circles_v2','prices']` (no api:/granularity:/tier:); `scripts/checks/check_api_tags.py:56-57` `if not api: continue`. No name-prefix rule in `scripts/checks/`.

**C07** — `fct_execution_circles_human_avatars_distinct.sql:43` `FROM ref('api_execution_circles_v2_avatar_metadata')`; api_ view is a passthrough over `int_execution_circles_v2_avatar_metadata`, no ref back (no cycle).

**C08** — `groups_cnt_latest.sql:23` `round((c.value-p.value)/p.value*100,1)` (bare p.value), CROSS JOIN current/prior, prior keyed on exact `(max-7)` date (line 18); same in humans/active_trusts. Live: `groups 0.5, humans 3.5, active_trusts 7.1` (one row each, as_of `2026-06-21`).

**C09** — `SELECT toInt64(total), change_pct, toString(as_of_date) FROM dbt.api_execution_circles_v2_orgs_cnt_latest` -> `2096, 0.9, 2026-06-21` (3 columns); SQL `:8` wraps subquery emitting `as_of_date`.

**C10** — `SELECT uniqExact(toStartOfMonth(date)), count() FROM dbt.fct_execution_circles_v2_avatar_balances_daily` -> `21, 25,244,036`; `int_execution_circles_v2_trust_pair_ranges.sql:1-9` `materialized='table' engine MergeTree()` no `partition_by`; `count()` -> `511,129`.

**C11** — grep of all `*.yml` under `models/execution/Circles/` for `int_execution_circles_v2_referrers` / `int_execution_circles_v2_trust_pair_ranges` -> 0 entries; both production-tagged (referrers.sql:7, trust_pair_ranges.sql:7); `SELECT count(), uniqExact(truster,trustee) FROM dbt.int_execution_circles_v2_trust_pair_ranges` -> `511,129, 511,129`.

**C12** — read config() of five snapshot fct_ (avatar_balances_latest, avatar_token_distribution, avatar_tokens_held_count, avatar_trusts_summary, avatar_personal_token_supply_latest): all `materialized='table'`, no `engine=`/`order_by=`.

**C13** — `SELECT week, earning_kind, count() c FROM dbt.fct_execution_circles_v2_economically_active_avatars_weekly GROUP BY week, earning_kind HAVING c>1` -> 0 rows; SQL `:4` `engine='ReplacingMergeTree()'`, final SELECT GROUP BYs `(week, earning_kind)`.

**C14** — `SELECT countIf(avatar IS NULL), toString(max(block_timestamp)), countIf(toDate(block_timestamp)=today()), count() FROM dbt.api_execution_circles_v2_avatars_current` -> `0, 2026-06-21 06:31:55, 4, 27,593`. SQL `:8-21` selects directly from int_ model, no WHERE/date guard/as_of_date.

**C15** — `fct_execution_circles_v2_tokens_supply_daily.sql:3` `materialized='view'`; header (`:8-13`) labels it a compatibility view; `marts/schema.yml:879` documents the same with tests on the intermediate.

**C16** — `SELECT crc20_token, pool_address, price_avg_in_backing, crc_volume FROM dbt.fct_execution_circles_v2_crc20_prices_daily WHERE date=max` for token `0x159e...` -> per-pool `{0.008186, 0.008073, 0.000077}`; api `price_vwap_usd=0.008138`; naive 3-pool mean ~`0.00545` (~33% below VWAP).

**C17** — `SELECT max(date) FROM dbt.fct_execution_circles_v2_active_trusts_daily` -> `2026-06-21` (value `415,387` vs `414,746` on `06-20`); `avatar_trusts_daily.sql:80` `today() AS max_day`, `active_trusts_daily.sql:47` `today() AS max_date`.

**C18 / C26** — `fct_execution_circles_human_avatars_distinct.sql:34` `pseudonymize_address('avatar')`; config tags include `mixpanel`, no `expose_to_mcp`/`privacy_tier`; `dbt_project.yml:70-114` mixpanel_ga exclusion scoped to the mixpanel_ga path; `discover_metrics` -> `circles_distinct_human_users`, `circles_humans_with_ipfs_profile` (root_model=this fct, `quality_tier=approved`).

**C19** — grep of WHERE predicates: `kpi_mints_7d.sql:18-19` & `kpi_new_trusts_7d.sql:17-18` `date>=today()-7 AND date<today()`; `kpi_new_backers_7d.sql:15` & `kpi_new_groups_7d.sql:17` `date>today()-7 AND date<=today()`. Live values: `44,152 / 29,298 / 23 / 3`.

**C20** — `describe_table` on three int_ models: `int_execution_circles_v1_avatars` = `{block_number,block_timestamp,transaction_hash,transaction_index,log_index,avatar_type,avatar,token_id}` (no user_address/inviter_address); `int_execution_circles_v2_avatars` has `avatar_type,invited_by,avatar,token_id,name` (none of the 7 bound cols); `int_execution_circles_v2_backing` lacks `date,cnt`.

**C21** — `int_execution_circles_v2_groups_overview_daily.sql:14` header lists `n_groups_total`; final SELECT (`:71-78`) emits only `date, n_new_groups, n_collateral_events, n_distinct_groups_acting`; api_ view emits `n_groups_total` via `sum(...) OVER(...)`.

**C22** — `SELECT mint_kind, count() cnt, uniqExact(to_address) FROM dbt.int_execution_circles_v2_mint_events GROUP BY mint_kind` -> personal `383,488`/`18,905`, group `180,843`/`3,131`, migration `10,816`/`3,705`; grep for `migrate()`/`4 times`/`four` -> 0 hits; registry Migration contract `0xd44b8dcfbadfc78ea64c55b705bfc68199b56376` (1 contract).

**C23** — `contracts_circles_v2_Hub_events` StreamCompleted -> `403,429` rows (max `2026-06-21`); `contracts_circles_v2_NameRegistry_events` UpdateMetadataDigest -> `58,238` rows. Net-new WAU last 6 weeks: `245 / 356 / 241 / 292 / 255 / 245` (~3.2%-5.1% uplift). Header still says "not yet exposed".

**C24** — `int_execution_circles_v2_mint_events.sql` header (`:15-37`) + personal CTE (`:65`, `event_name='PersonalMint'`) both describe the PersonalMint classifier; grep for `avatar_type='Human'`/`token_address = to_address` personal-classifier -> 0 hits; schema.yml agrees.

**C25** — `api_execution_circles_v2_crc20_prices_daily.sql:17` `price_vwap_usd` (volume-weighted), `:18` `price_median_usd=median(price_median_usd)`; latest 3-pool token: `price_median_usd=0.008179` vs `price_vwap_usd=0.008138` (~0.5% gap).

**C27** — grep `toDateTime('2025-11-12')` in models -> 2 hits (inviter_fees.sql:56, referrers.sql:43); `dbt_project.yml:20` `gnosis_app_wau_floor_date: '2025-11-12'`; consumed by `int_execution_gnosis_app_weekly_earners.sql:28` but not the two Circles models.

**C28** — `SELECT toString(cohort_month), n_invited, n_minted_14_days, n_active_minter FROM dbt.api_execution_circles_v2_invite_funnel_cohort_monthly ORDER BY cohort_month DESC LIMIT 3` -> 2026-05 `n_minted_14_days=176, n_active_minter=426`; 2026-04 `56, 273`; 2026-06 `0, 5`. SQL `:32-33`.

## Review log (>= 3 rounds per case)

- **C01**: R1 CONFIRMED critical (SQL 9 cols vs live 13) -> challenge: prove balance_diffs breaks + check schema.yml col count -> R2 CONFIRMED (schema.yml documents 13; balance_diffs UNKNOWN_IDENTIFIER + col test would fail) -> R3 CONFIRMED critical (confirmed balance_diffs dev-tagged, gated behind C04 exclusion; latent until v1 promoted, severity holds).
- **C02**: R1 CONFIRMED high (57/615 zero days) -> challenge: prove interior dips not leading zeros -> R2 CONFIRMED (all 57 have total_supply>0) -> R3 CONFIRMED high (direct 2024-10-19..25 exhibit shows interior dips between 14.99 neighbours).
- **C03**: R1 CONFIRMED high -> challenge: trace NULLs into WEAU filter -> R2 CHANGED medium (downstream uses max()/any_in_app_tx not IN(0,1); WEAU unaffected) -> R3 CHANGED medium (sized: 82 earner-weeks/79 avatars dropped from in-app subset).
- **C04**: R1 CONFIRMED high (six dev tags, ~80d stale) -> challenge: prove CI gate excludes dev -> R2 CONFIRMED (run_dbt_observability.sh:217 --select tag:production) -> R3 CONFIRMED high (challenge closed; gate load-bearing).
- **C05**: R1 CONFIRMED high code-only -> challenge: exhibit one un-closed open-interval row -> R2 CHANGED medium (deployed table doesn't exhibit inflation; lead()-chaining correct; surfaced RMT-no-FINAL 1.77x instead) -> R3 CHANGED medium (open-interval code-latent; RMT duplication is the realized artifact; consumers out of prod scope).
- **C06**: R1 CONFIRMED high (no api:/granularity:/tier:) -> challenge: run check_api_tags.py rule logic -> R2 CHANGED low (checker skips models with no api: tag) -> R3 CHANGED low (no separate name-prefix CI rule exists; unguarded tagging gap).
- **C07**: R1 CONFIRMED high (fct refs api_ view) -> challenge: confirm build-time edge, check for cycle -> R2 CONFIRMED (true ref edge, no cycle) -> R3 CONFIRMED high (challenge closed; clean-but-inverted edge).
- **C08**: R1 CONFIRMED medium -> challenge: reconcile zero-rows vs inf-divide, run live -> R2 CONFIRMED low (prior keys exact max-7; zero-rows-on-gap latent; live returns finite rows) -> R3 CONFIRMED low (challenge closed).
- **C09**: R1 CHANGED low (as_of_date now present, divide half persists) -> challenge: confirm fix deployed + can p.value be 0 -> R2 RESOLVED low (live 3 cols, monotonic so never 0) -> R3 RESOLVED (challenge closed; deployed-fixed).
- **C10**: R1 CONFIRMED medium -> challenge: 21 partitions << 100, re-frame imminence -> R2 CHANGED low (~79mo runway; trust_pair_ranges partitionless not a 252 risk) -> R3 CHANGED low (challenge closed; residual full-rebuild cost).
- **C11**: R1 CONFIRMED medium (zero schema entries) -> challenge: prove untested + grain-uniqueness -> R2 CONFIRMED medium (grain clean count=uniqExact=511,129; missing test latent) -> R3 CONFIRMED medium (challenge closed).
- **C12**: R1 CONFIRMED medium -> challenge: describe live ENGINE -> R2 CONFIRMED low (system.tables blocked; adapter default MergeTree ORDER BY tuple(), no dup) -> R3 CHANGED low (style gap, no correctness risk).
- **C13**: R1 CONFIRMED medium code-only -> challenge: prove/disprove dups with data -> R2 CHANGED low (HAVING count()>1 = 0 rows) -> R3 CHANGED low (RMT pointless on already-unique grain).
- **C14**: R1 CONFIRMED medium (unguarded passthrough) -> challenge: run live, active vs latent -> R2 CONFIRMED low (code clear, not run within budget) -> R3 CHANGED medium (today_rows=4 active leak; null_avatars=0 latent).
- **C15**: R1 CONFIRMED medium -> challenge: confirm live object-type mismatch + schema.yml table tests -> R2 CHANGED low (schema.yml now documents compatibility view) -> R3 CHANGED low (contradiction resolved; convention deviation only).
- **C16**: R1 CONFIRMED low -> challenge: quantify VWAP divergence -> R2 CONFIRMED low (~33% naive-mean gap on 3-pool token) -> R3 CONFIRMED low (challenge closed).
- **C17**: R1 CONFIRMED low -> challenge: prove today() row is partial + identical calendar bound -> R2 CONFIRMED low (today row 415,387 rising cumulative; both use today()) -> R3 CONFIRMED low (challenge closed).
- **C18**: R1 CONFIRMED low -> challenge: settle exclusion path + semantic entry -> R2 CONFIRMED low (outside mixpanel_ga subtree; approved semantic entry) -> R3 CONFIRMED low (challenge closed; C26 carries the medium).
- **C19**: R1 CONFIRMED high (>=/< vs >/<=) -> challenge: run all four live + effective spans -> R2 CONFIRMED high (one-day skew + as_of divergence) -> R3 CONFIRMED high (challenge closed).
- **C20**: R1 CONFIRMED high (bound cols absent) -> challenge: paste live runtime bind error -> R2 CONFIRMED high (live error blocked by manifest_hash_mismatch; absence schema-proven) -> R3 CONFIRMED high (challenge closed).
- **C21**: R1 CONFIRMED medium (claimed MCP column-not-found) -> challenge: mechanism wrong, api_ view emits n_groups_total, re-scope -> R2 CHANGED low (api_ view supplies column; intermediate doc-drift only) -> R3 CHANGED low (challenge closed).
- **C22**: R1 CONFIRMED medium (10,816 vs "4 calls") -> challenge: comment no longer exists, validate migration volume -> R2 RESOLVED low (comment gone; 1 Migration contract, plausible) -> R3 RESOLVED (challenge closed).
- **C23**: R1 CHANGED medium (events now decoded, rationale stale) -> challenge: quantify undercount -> R2 CONFIRMED medium (omission + stale header confirmed, unquantified) -> R3 CONFIRMED medium (quantified 3.2%-5.1% weekly uplift).
- **C24**: R1 RESOLVED (PersonalMint classifier) -> challenge: confirm header AND schema.yml fixed -> R2 RESOLVED low (both describe PersonalMint, no residual language) -> R3 RESOLVED (challenge closed).
- **C25**: R1 CONFIRMED medium -> challenge: quantify bias magnitude on multi-pool token -> R2 CHANGED low (~0.5% gap) -> R3 CHANGED low (documentation-only).
- **C26**: R1 CONFIRMED medium -> challenge: settle MCP-reachability definitively -> R2 CONFIRMED medium (semantic entry approved; live discovery blocked by infra) -> R3 CONFIRMED medium (discover_metrics returns both approved metrics; active exposure).
- **C27**: R1 CONFIRMED low -> challenge: confirm exactly 2 occurrences + existing var -> R2 CONFIRMED low (2 hits; gnosis_app_wau_floor_date var exists, unused here) -> R3 CONFIRMED low (challenge closed).
- **C28**: R1 CONFIRMED low -> challenge: demonstrate inversion with numbers -> R2 CONFIRMED low (asymmetry code-clear, data exhibit deferred) -> R3 CONFIRMED low (live inversion 426>176, 273>56).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (KEEP) | Align `int_execution_circles_v1_transfers` final SELECT, `schema.yml` (13 cols), and `int_execution_circles_v1_balance_diffs` references before any v1 promotion — a full-refresh today breaks the build twice (`UNKNOWN_IDENTIFIER` + schema column test). Latent only because dev-gated. | `int_execution_circles_v1_transfers.sql`, `int_execution_circles_v1_balance_diffs.sql`, `intermediate/schema.yml` |
| P1 (KEEP) | Unify the four `*_7d` KPI window predicates to a single convention (`>=/<` or `>/<=`) so same-named tiles count identical spans. | `api_execution_circles_v2_kpi_mints_7d.sql`, `..._new_trusts_7d.sql`, `..._new_backers_7d.sql`, `..._new_groups_7d.sql` |
| P1 (KEEP) | Fix the three semantic candidate models to bind columns that exist (or drop them) — every MCP metric query fails at bind time. | `semantic/authoring/execution/Circles/semantic_models.yml` (v1_avatars, v2_avatars, backing) |
| P1 (KEEP) | Decide v1 stack disposition: either promote the six dev-tagged v1 models to production (after the C01 schema fix) or formally retire them; staleness is ~80 days. | six `int_execution_circles_v1_*` models |
| P1 (KEEP) | Refactor `fct_execution_circles_human_avatars_distinct` to read `int_` sources, not the `api_` view (layer inversion). | `fct_execution_circles_human_avatars_distinct.sql` |
| P1 (KEEP) | Add forward-fill (`last_value IGNORE NULLS`) to `wrapper_share_daily` so wrapped_supply does not collapse to 0 on no-event days (57/615). | `api_execution_circles_v2_wrapper_share_daily.sql` |
| P2 (KEEP) | Add `expose_to_mcp:false`/`privacy_tier` to the pseudonymized cross-sector bridge fact, or bring it under an API-exclusion path — it is approved + discoverable via `discover_metrics`. | `fct_execution_circles_human_avatars_distinct.sql` (covers C18 + C26) |
| P2 (KEEP) | Fix `is_gnosis_app_tx` NULL-propagation via `isNotNull` / `join_use_nulls` so the in-app earner subset stops dropping 82 earner-weeks. | `int_execution_circles_v2_inviter_fees.sql`, `int_execution_circles_v2_referrers.sql` |
| P2 (KEEP) | Add a `date<today()` guard (and `WHERE avatar IS NOT NULL`) to `avatars_current` — actively leaking 4 in-flight rows today. | `api_execution_circles_v2_avatars_current.sql` |
| P2 (KEEP) | Add `schema.yml` entries + uniqueness tests for the two undocumented production intermediates (grain currently clean, test latent). | `int_execution_circles_v2_referrers.sql`, `int_execution_circles_v2_trust_pair_ranges.sql` |
| P2 (KEEP) | Either include `StreamCompleted`/`UpdateMetadataDigest` in active_avatars_weekly (now decoded) or refresh the stale "not yet exposed" header; undercount is 3.2%-5.1%/week. | `int_execution_circles_v2_active_avatars_weekly.sql` |
| P3 (KEEP, low) | Register `crc20_prices_daily` as an api endpoint (`api:`/`granularity:`/`tier:` tags) so the convention guard covers it. | `api_execution_circles_v2_crc20_prices_daily.sql` |
| P3 (KEEP, low) | Point the two Circles models at the existing `gnosis_app_wau_floor_date` var instead of hardcoding `2025-11-12`. | `int_execution_circles_v2_inviter_fees.sql`, `int_execution_circles_v2_referrers.sql` |
| P3 (KEEP, low) | Document the horizon mix on the invite funnel (stages 2-4 = 30-day, stage 5 = lifetime) to prevent misreading as drop-off. | `api_execution_circles_v2_invite_funnel_cohort_monthly.sql` |
| P3 (KEEP, low) | Add `nullIf` guard / document the median-of-pool-medians caveat; consider FINAL on direct fct_ reads (~33% naive-mean divergence). | `api_execution_circles_v2_crc20_prices_daily.sql`, `fct_execution_circles_v2_crc20_prices_daily.sql`, three cnt_latest views |
| P3 (KEEP, low) | Style cleanup: add explicit `engine=`/`order_by=` to the five snapshot fct_ tables; drop pointless RMT on `economically_active_avatars_weekly`; align fct_ calendars or document the partial-today row. | five snapshot fct_, `..._economically_active_avatars_weekly.sql`, `..._active_trusts_daily.sql`, `..._avatar_trusts_daily.sql` |
| DROP | `as_of_date` missing on orgs_cnt_latest — deployed-fixed (live returns 3 cols). | `api_execution_circles_v2_orgs_cnt_latest.sql` |
| DROP | mint_kind migration vs "4 calls" comment — comment removed, volume plausible (1 Migration contract). | `int_execution_circles_v2_mint_events.sql` |
| DROP | PersonalMint classifier doc-vs-code contradiction — header + schema.yml now match the code. | `int_execution_circles_v2_mint_events.sql` |

Note: the partition-cap recommendation from the baseline (C10) is effectively dropped as imminent — re-scoped to a low-priority full-rebuild-cost concern (`21` partitions << 100, ~79 months runway; trust_pair_ranges partitionless).
