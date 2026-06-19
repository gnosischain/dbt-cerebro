# Model review: mixpanel_ga

**Convergence:** converged in 1 round — both inspector shards (staging/intermediate and marts) produced mutually consistent, code-evidenced findings with no material contradictions; all critical and high findings were confirmed independently.

---

## Scope and inventory

| Layer | Model count | Notes |
|---|---|---|
| Staging | 1 | `stg_mixpanel_ga__events` — monthly-partitioned insert_overwrite |
| Intermediate | 15 | 8 near-identical daily rollup models + 7 specialised models |
| Marts — fct_ | 13 | Daily, weekly, and per-user fact tables |
| Marts — api_ | 17 | Thin view wrappers over fct_ (aggregate) + 1 blocked per-user view |
| Seeds | 1 | `mixpanel_conversion_events.csv` (3 rows) |
| Semantic models | 13 | Registered in `semantic/authoring/mixpanel_ga/semantic_models.yml` |

The unit is the web and product analytics pipeline for Gnosis App (`app.gnosis.io`) and Gnosis Pay, built on raw Mixpanel event data. All models carry `privacy:mixpanel_ga` and `exclude_from_api: true` at the `dbt_project.yml` level; aggregate views are MCP-accessible while per-user grain views carry an additional `expose_to_mcp: false` guard on a per-model basis. No warehouse queries were executed — the privacy tier blocks direct data access via MCP tools; all findings are code-based.

---

## Business context

The unit answers six classes of questions:

1. **Gnosis App product analytics** — daily active users, total events, new/cumulative users, unique devices, geographic and browser/OS distribution, temporal usage patterns, per-page traffic, modal engagement.
2. **Gnosis App onboarding and feature funnels** — /welcome → ModalJoin → Passkey login → identified; /swap → SelectSwapAsset → swap; /circles → mint.
3. **UTM campaign attribution and growth funnel** — weekly first/last-touch attribution for client-side conversions (`card_ordered`, `crc_minted`, `circles_created` via seed) and on-chain conversions (funded, first_payment, topup, swap_filled, marketplace_buy, etc.).
4. **GP cross-domain identity bridge** — daily aggregate of how many Mixpanel DAU match GP Safe accounts via three identity roles (initial_owner, delegate, safe_self), with on-chain activity flags.
5. **GA cross-domain identity bridge** — per-user fact listing every heuristic-identified Gnosis App user with a `matched_mp` diagnostic flag; Mixpanel is supplementary, not authoritative.
6. **GP per-campaign retention and engagement** — cohort retention (funded cohort × weekly card payments), per-campaign payment volume and cashback; k-anonymity floor of 5 enforced.

**Key canonical definitions:**

- **DAU (web):** `uniqExact(user_id_hash)` per production day. Source: `int_mixpanel_ga_users_daily` → `fct_mixpanel_ga_overview_daily`. Semantic quality tier: approved.
- **user_id_hash:** `sipHash64(concat(unhex(CEREBRO_PII_SALT), lower(distinct_id)))` — same salt applied to on-chain addresses, enabling cross-domain joins without raw address exposure.
- **is_production:** 1 iff `current_domain = 'app.gnosis.io'` (~89% of raw Mixpanel traffic).
- **is_identified:** 1 if `distinct_id` does not start with `$device:` — user has called `mixpanel.identify(walletAddress)`.
- **first_touch / last_touch campaign:** derived post-hoc by `argMinIf`/`argMaxIf` over `event_time`; UTM parameters ride only on entry/landing hits (~3.6% of events).
- **matched_mp (GA sector):** diagnostic flag — documented as 70-83% true coverage of heuristic-identified addresses; currently broken (see critical findings).
- **match_rate_pct (GP crossdomain):** `matched_users_any / mp_dau * 100`; activity flags currently broken (see critical findings).
- **k-anonymity floor:** campaigns with fewer than 5 total signups or funded accounts are bucketed into `_small_campaigns`; enforced in `fct_mixpanel_ga_campaign_funnel_weekly` and `fct_mixpanel_ga_gpay_campaign_metrics_weekly`.
- **authoritative: false** on all models — Mixpanel is positioned as diagnostic relative to on-chain sources of truth.

No contract addresses are hardcoded in the `mixpanel_ga` SQL files. All cross-domain joins use pseudonymised columns; contract dependencies are mediated through upstream seed-backed intermediate models from the execution sector.

---

## Implementation assessment

### HIGH — Jinja variable evaluation order bug in `int_execution_gnosis_app_events_mixpanel_unified`

`models/mixpanel_ga/intermediate/int_execution_gnosis_app_events_mixpanel_unified.sql`

Line 4 references `start_month` inside `config(incremental_strategy=('append' if start_month else 'delete+insert'))`, but `{% set start_month = var('start_month', none) %}` appears on line 15. dbt renders the config block top-to-bottom; `start_month` is Jinja `Undefined` (falsy) at config eval time. Result: the model always uses `delete+insert` regardless of `--vars start_month`, silently defeating the intended append path for backfill/MTA runs. Every other model in the repo correctly sets the variable above the config block. Fix: move both `{% set %}` lines above the config block.

### MEDIUM — Missing `join_use_nulls` pre/post hooks on `int_mixpanel_ga_client_first_events`

`models/mixpanel_ga/intermediate/int_mixpanel_ga_client_first_events.sql`

The model LEFT JOINs `int_mixpanel_ga_user_acquisition` on `user_id_hash`, then applies `coalesce(a.first_touch_campaign, 'unknown')`. Without `join_use_nulls=1`, ClickHouse returns `''` (empty String) for unmatched rows; `coalesce('', 'unknown')` returns `''`, so unmatched users receive blank attribution rather than `'unknown'`. Analogous models (`gpay_first_events`, `gnosis_app_first_events`) correctly use the pre/post hook pattern.

### MEDIUM — Three user-pseudonym row-level intermediate models missing `privacy:tier_internal` and `expose_to_mcp: false`

`models/mixpanel_ga/intermediate/int_mixpanel_ga_user_lifecycle.sql`, `int_mixpanel_ga_users_daily.sql`, `int_execution_gnosis_app_events_mixpanel_unified.sql`

All three output `user_id_hash`/`user_pseudonym` at row level but carry only the project-level `privacy:mixpanel_ga` tag. Peer models (`user_acquisition`, `client_first_events`, `gpay_first_events`, `gnosis_app_first_events`) explicitly add `internal_only`, `privacy:tier_internal`, and `expose_to_mcp: false`. The inconsistency creates a gap if the semantic registry or MCP layer relies on those flags for access control.

### MEDIUM — `int_mixpanel_ga_traffic_daily` groups by `initial_referrer_domain` (lifetime user super-property), creating a combinatorial grain

`models/mixpanel_ga/intermediate/int_mixpanel_ga_traffic_daily.sql`

`$initial_referring_domain` is assigned once per Mixpanel user (first-ever session) and propagated to every subsequent event. Grouping by `(date, referrer_domain, initial_referrer_domain)` fans out a given `referrer_domain` on a given day into as many rows as there are distinct lifetime acquisition channels among that day's visitors. `api_mixpanel_ga_traffic_daily` exposes this grain directly; consumers summing `event_count` by referrer must also group by `initial_referrer_domain` or produce incorrect totals.

### MEDIUM — `int_execution_gnosis_app_events_mixpanel_unified` schema.yml missing 5 of 13 output columns

`models/mixpanel_ga/intermediate/schema.yml`, `int_execution_gnosis_app_events_mixpanel_unified.sql`

The SQL outputs 13 columns; the schema.yml documents only 8. Missing: `amount_usd`, `event_dedup_key`, `provenance_model`, `device_type`, `country_code`. With `contract.enforced: false` the build does not fail, but downstream MTA/event-union consumers have no documented contract for these columns.

### MEDIUM — `int_mixpanel_ga_client_first_events` and `int_mixpanel_ga_gpay_campaign_cohorts` absent from intermediate schema.yml

Both models have SQL files and are consumed downstream (campaign cohorts feeds GPay retention/funnel marts; client first events feeds client-conversion marts), but neither has an entry in `models/mixpanel_ga/intermediate/schema.yml`. No column-level descriptions, no dbt tests, no elementary monitoring. Particularly notable for `client_first_events` which carries `user_id_hash` at row level.

### MEDIUM — 8 mart models entirely undocumented in schema.yml and missing required tags

`fct_mixpanel_ga_campaign_funnel_weekly`, `fct_mixpanel_ga_client_conversions_weekly`, `fct_mixpanel_ga_gpay_campaign_metrics_weekly`, `fct_mixpanel_ga_gpay_campaign_retention_weekly`, and their four `api_` counterparts have no `schema.yml` entry — no column descriptions, no `data_type` declarations, no elementary tests. The four `api_` views also lack `granularity:weekly` and tier tags present in sibling weekly views (e.g. `api_mixpanel_ga_gpay_acquisition_weekly`), and bypass the `check_api_tags.py` CI guard because they carry no `api:` tag.

### MEDIUM — `fct_mixpanel_ga_gpay_users` and `fct_mixpanel_ga_gnosis_app_users` missing `expose_to_mcp: false` in schema.yml

Both fct_ tables expose per-user grain (`user_id_hash`+`gp_safe` and `user_pseudonym`+`matched_mp`). `api_mixpanel_ga_users_daily` correctly carries `expose_to_mcp: false` in both the `.sql` config and `schema.yml`. The two per-user fct_ tables have no such protection, and `dbt_project.yml` does not set a blanket `expose_to_mcp: false` for the sector.

### MEDIUM — Rolling `today()`-based windows in materialized tables produce stale metrics without a staleness warning

`models/mixpanel_ga/marts/fct_mixpanel_ga_gpay_users.sql`, `fct_mixpanel_ga_gpay_crossdomain_daily.sql`

`fct_mixpanel_ga_gpay_users` bakes `delay_txs_last_30d` and `spends_last_30d` using `WHERE date >= today()-30` at build time. `fct_mixpanel_ga_gpay_crossdomain_daily` computes `delay_active_safes_7d` and `allowance_changed_safes_30d` similarly. If these full-rebuild tables are not refreshed daily, the rolling-window columns become stale relative to the actual window with no staleness warning in `schema.yml`.

### LOW — Seed-driven conversion event list has only 3 rows with no test guard

`seeds/mixpanel_conversion_events.csv`

The seed powers `int_mixpanel_ga_client_first_events` via INNER JOIN; new event names added without updating the seed cause silently dropped conversions. No `not_null` or `accepted_values` tests exist on the `metric` column, and no row-count assertion guards against accidental truncation.

### LOW — No unique key tests on any mart table

`models/mixpanel_ga/marts/schema.yml`

`schema.yml` has only `elementary.schema_changes` and `elementary.volume_anomalies` on a subset of fct_ tables. No `dbt unique` or `dbt_utils.unique_combination_of_columns` tests exist for any model grain. Duplicate rows from ReplacingMergeTree background merges would go undetected.

### LOW — Staging model uses `ReplacingMergeTree()` without a version column

`models/mixpanel_ga/staging/stg_mixpanel_ga__events.sql`

The staging table uses `order_by=(project_id, event_name, event_time, insert_id)`. If the upstream source contains duplicate `insert_id`s ingested into different `event_time`s (a known Mixpanel SDK edge case), deduplication depends on background merge timing, and the stg uniqueness test may fail intermittently before a merge cycle completes.

---

## Business-logic assessment

### CRITICAL — `fct_mixpanel_ga_gnosis_app_users`: `matched_mp` is always 1 due to `join_use_nulls=0`

`models/mixpanel_ga/marts/fct_mixpanel_ga_gnosis_app_users.sql`

The model LEFT JOINs `mp_users` on `user_pseudonym = mp.user_id_hash` (both `UInt64` via `sipHash64`) and checks `if(mp.user_id_hash IS NOT NULL, 1, 0)`. In ClickHouse default mode (`join_use_nulls=0`), an unmatched LEFT JOIN returns the type default (`0` for `UInt64`), and `0 IS NOT NULL` evaluates to `TRUE`. Result: `matched_mp=1` and `mp_user_id_hash=0` for every row including users with no Mixpanel data. The documented 70-83% Mixpanel coverage diagnostic is completely invalidated — the model currently reports ~100% coverage. Fix: add `pre_hook=["SET join_use_nulls = 1"]` / `post_hook=["SET join_use_nulls = 0"]` and rebuild.

### CRITICAL — `fct_mixpanel_ga_gpay_crossdomain_daily`: activity flags always 1 due to `join_use_nulls=0`

`models/mixpanel_ga/marts/fct_mixpanel_ga_gpay_crossdomain_daily.sql`

The `mp_user_flags` CTE LEFT JOINs `delay_active_safes_7d` and `allowance_changed_safes_30d` (both on String `gp_safe`), then checks `if(da.gp_safe IS NOT NULL, 1, 0)`. With `join_use_nulls=0`, unmatched rows return `''` (empty String), which `IS NOT NULL` evaluates to `TRUE`. `users_with_delay_activity_7d` and `users_with_allowance_changes_30d` are inflated to the full matched-user count regardless of actual on-chain activity. Any downstream GP cardholder engagement reporting on delay-module queuing and allowance changes is incorrect. Same hook pattern fix required.

### HIGH — `fct_mixpanel_ga_overview_daily`: `unique_devices` is an additive sum of per-event-type exact counts

`models/mixpanel_ga/marts/fct_mixpanel_ga_overview_daily.sql`, `api_mixpanel_ga_overview_daily.sql`

`int_mixpanel_ga_events_daily` computes `uniqExact(device_id_hash)` per `(date, event_name, event_category)` grain. `fct_mixpanel_ga_overview_daily` uses `sum(unique_devices)` from that intermediate grouped to date only. Summing per-group exact counts is not equivalent to `uniqExact` over the union — a device firing N event types on the same day is counted N times. The schema.yml description "Distinct device_id_hash count" is factually incorrect. This metric is registered in the semantic layer at `quality_tier: approved` and flows to the public-facing overview API view. Fix: compute `uniqExact(device_id_hash)` per date directly from `stg_mixpanel_ga__events` in a dedicated CTE, then update the semantic model measure and schema.yml description.

### MEDIUM — `fct_mixpanel_ga_gnosis_app_daily`: cumulative_users description claims '2025-11-12 start' but no filter enforces it

`models/mixpanel_ga/marts/fct_mixpanel_ga_gnosis_app_daily.sql`

`schema.yml` states "Running total of distinct addresses since 2025-11-12" but there is no corresponding hard filter in the SQL — it accumulates from the earliest `int_execution_gnosis_app_user_events` row. If any heuristic events predate Nov 2025, `cumulative_users` will be inflated relative to the documented baseline.

### MEDIUM — Semantic layer registers `cumulative_accounts` with `agg: sum`, which over-counts running totals across weeks

`semantic/authoring/mixpanel_ga/semantic_models.yml`

The acquisition weekly models store a running `cumulative_accounts` per `(event_type/conversion_kind, attribution_model, utm_campaign)` partition. The semantic model registers this measure with `agg: sum`. Any MCP query summing `cumulative_accounts` over multiple weeks produces a multiplied total. The schema.yml description warns "do NOT sum across periods" but this constraint is not enforced at the semantic measure level. Correct aggregation for a running-total measure is `agg: max`.

### LOW — Onboarding funnel conversion rate can exceed 100%

`models/mixpanel_ga/marts/fct_mixpanel_ga_funnel_daily.sql`

`onboarding_conversion_rate = passkey_logins / greatest(welcome_visitors, 1)`. A user can complete "Login with Passkey" without visiting `/welcome` (bookmark, direct navigation, session resumption). On low-traffic days this yields rates above 1.0. No cap or caveat exists in schema.yml.

---

## Data findings

No warehouse queries were executed. The `privacy:mixpanel_ga` tier blocks direct table access via MCP tools. All findings are code-based. The following data-state conclusions follow deterministically from type analysis and ClickHouse engine behaviour:

- **`matched_mp` in `fct_mixpanel_ga_gnosis_app_users`** is `1` for all rows currently stored. The documented 70-83% match rate cannot be observed from the current materialized data; the table must be rebuilt after applying the `join_use_nulls=1` fix.
- **`users_with_delay_activity_7d` and `users_with_allowance_changes_30d` in `fct_mixpanel_ga_gpay_crossdomain_daily`** are inflated to the full matched-user set for all historical dates. GP cardholder engagement metrics derived from these columns are unreliable until the table is rebuilt.
- **`unique_devices` in `fct_mixpanel_ga_overview_daily`** is over-counted by approximately N× the average number of distinct event types fired per device per day. The exact inflation factor cannot be quantified without warehouse access, but any day with diverse event mixes will materially exceed the true distinct device count.

---

## Pros / Cons

**Pros**

- Privacy architecture is coherent: two-tier exclusion (`cerebro-api` blanket ban via `dbt_project.yml` + per-model MCP flags) is consistently applied across the majority of models, with explicit documentation in schema.yml comments explaining the reasoning.
- Pseudonymisation is correctly and consistently applied via the salted `sipHash64` macro at ingestion, enabling cross-domain joins without raw address exposure throughout the pipeline.
- Staging layer design is solid: unconditional `today()` guard on raw CTE, `insert_overwrite`/monthly-partition incremental strategy, comprehensive schema.yml coverage, and well-structured JSON extraction.
- The eight daily intermediate models follow a uniform, tested pattern with `dbt_utils.unique_combination_of_columns` and elementary anomaly tests on grain keys.
- Attribution sparsity (UTM ~3.6% coverage) and double-counting risk from stacked first_touch/last_touch rows are explicitly documented in schema.yml, warning consumers before misuse.
- k-anonymity floor of 5 is enforced in campaign funnel and GPay campaign metrics weekly models, limiting re-identification risk in growth reporting.
- The GP identity bridge uses a three-role union (initial_owner, delegate, safe_self) with an additional GA-controller-wins coalesce for app-onboarded Safes — a thorough and well-documented identity resolution strategy.
- `authoritative: false` is set across all models, correctly positioning Mixpanel data as supplementary/diagnostic relative to on-chain sources of truth.

**Cons**

- Two critical `join_use_nulls=0` bugs produce silently wrong metrics that are currently stored in materialized tables and served via the semantic layer.
- `unique_devices` in `fct_mixpanel_ga_overview_daily` and its semantic model (quality_tier: approved) is methodologically incorrect and over-counts true distinct devices.
- The Jinja evaluation-order bug in `int_execution_gnosis_app_events_mixpanel_unified` silently defeats the append backfill path.
- Eight mart models (four fct_ and four api_) are entirely absent from schema.yml with no tests, no column documentation, and API tag convention violations.
- Three intermediate models carrying `user_id_hash` at row level lack the `privacy:tier_internal`/`expose_to_mcp: false` tags applied consistently to peer models.
- Rolling `today()`-based windows baked into materialized tables go stale without any schema.yml staleness warning.
- The traffic_daily model's combinatorial grain (date, referrer_domain, initial_referrer_domain) is a silent footgun for consumers summing event counts by referrer.
- The semantic layer's `agg: sum` on running-total cumulative_accounts measures will cause incorrect totals for any multi-week MCP query.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| CRITICAL | Add `pre_hook=["SET join_use_nulls = 1"]` / `post_hook=["SET join_use_nulls = 0"]` to `fct_mixpanel_ga_gnosis_app_users`; full rebuild required — stored `matched_mp=1` for all rows invalidates the GA sector health diagnostic | `fct_mixpanel_ga_gnosis_app_users.sql` |
| CRITICAL | Add `join_use_nulls=1` hooks to `fct_mixpanel_ga_gpay_crossdomain_daily`; rebuild required — `users_with_delay_activity_7d` and `users_with_allowance_changes_30d` are currently inflated to 100% of matched users | `fct_mixpanel_ga_gpay_crossdomain_daily.sql` |
| HIGH | Fix `unique_devices` in overview: replace `sum(unique_devices)` from the per-event-type intermediate with a direct `uniqExact(device_id_hash)` per-date CTE from `stg_mixpanel_ga__events`; update semantic model `agg` and schema.yml description | `fct_mixpanel_ga_overview_daily.sql`, `api_mixpanel_ga_overview_daily.sql`, `semantic_models.yml` |
| HIGH | Move both `{% set start_month %}` / `{% set end_month %}` lines above the `config()` block in the unified model so the `incremental_strategy` ternary evaluates correctly | `int_execution_gnosis_app_events_mixpanel_unified.sql` |
| MEDIUM | Add `join_use_nulls=1` pre/post hooks to `int_mixpanel_ga_client_first_events` — LEFT JOIN on `user_id_hash` returns `''` for unmatched String columns; `coalesce('', 'unknown')` silently returns `''` | `int_mixpanel_ga_client_first_events.sql` |
| MEDIUM | Add schema.yml entries with `elementary.schema_changes`, `elementary.volume_anomalies`, and `dbt_utils.unique_combination_of_columns` tests for `int_mixpanel_ga_client_first_events`, `int_mixpanel_ga_gpay_campaign_cohorts`, and all eight undocumented mart models; add `granularity:weekly` and tier tags to the four weekly `api_` views | `intermediate/schema.yml`, `marts/schema.yml` |
| MEDIUM | Add `privacy:tier_internal` / `expose_to_mcp: false` to `int_mixpanel_ga_user_lifecycle`, `int_mixpanel_ga_users_daily`, `int_execution_gnosis_app_events_mixpanel_unified`; add `expose_to_mcp: false` to `fct_mixpanel_ga_gpay_users` and `fct_mixpanel_ga_gnosis_app_users` schema.yml meta | Multiple intermediates and fct_ schema.yml |
| MEDIUM | Change `cumulative_accounts` semantic measure `agg` from `sum` to `max` (or equivalent period_last) in the acquisition weekly semantic models to prevent naive multi-week queries over-counting running totals | `semantic/authoring/mixpanel_ga/semantic_models.yml` |
| LOW | Add a `row_count` or `accepted_values` test on the `metric` column of `seeds/mixpanel_conversion_events.csv` to guard against accidental truncation or missed event name additions | `seeds/mixpanel_conversion_events.csv` |
| LOW | Add `dbt_utils.unique_combination_of_columns` tests on the grain keys of all mart tables; document rolling-window staleness risk for `delay_txs_last_30d`, `spends_last_30d`, `delay_active_safes_7d`, `allowance_changed_safes_30d` in schema.yml descriptions | `marts/schema.yml`, `fct_mixpanel_ga_gpay_users.sql`, `fct_mixpanel_ga_gpay_crossdomain_daily.sql` |

---

## Open disagreements

None. The review converged in round 1.

---

## Review log

| Round | Shard | Challenges issued | Outcome |
|---|---|---|---|
| 1 | staging/intermediate | None issued | No challenges; findings accepted |
| 1 | marts | Three responses to potential challenges (join_use_nulls determinism, unique_devices methodology, stale rolling window) pre-emptively documented in inspector report | All confirmed by type analysis and engine behaviour; no rebuttals required |
| 1 | Final verdict | No inter-shard conflicts found; convergence declared | Both shards accepted; report finalised |
