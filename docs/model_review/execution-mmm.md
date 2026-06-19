# Model review: execution/mmm

**Convergence:** converged in 2 rounds — the two round-1 critical findings (53-week staleness, live ctrl_eth_price duplicate bug) were fully retracted by round-2 warehouse evidence; all remaining findings are agreed.

---

## Scope and inventory

| Layer | Files | Materialization |
|---|---|---|
| Intermediates | `int_execution_mmm_kpis_weekly`, `int_execution_mmm_media_weekly`, `int_execution_mmm_controls_weekly` | incremental (ReplacingMergeTree) |
| Marts — fact | `fct_execution_mmm_spine_weekly`, `fct_execution_mmm_baseline_latest`, `fct_execution_mmm_collinearity_latest` | table |
| Marts — API | `api_execution_mmm_spine_weekly` | view over `fct_execution_mmm_spine_weekly` |
| Seeds | `mmm_kpi_registry.csv`, `mmm_media_registry.csv`, `mmm_control_registry.csv`, `mmm_hardfork_steps.csv`, `mmm_holiday_weeks.csv` | — |

7 SQL models total. Two `schema.yml` files cover all layers. No semantic-layer metrics; no macros beyond the shared `weekly_spine` and `get_incremental_filter`.

---

## Business context

The unit builds the weekly time-series dataset consumed by the Cerebro MCP three-persona pipeline (`mmm_analyst` → `mmm_causal_reviewer` → `mmm_simulator`). Its business questions are: which on-chain incentive programmes caused changes in TVL, DEX volume, and active-user counts; what is the marginal ROI of each incentive stream; and what budget reallocation maximises a chosen KPI subject to a ±30 %/period cap and a 1.5× max-historical-spend ceiling.

**Canonical KPIs (dependent variables):** seven implemented — `pools_tvl_usd` (last-value), `pools_volume_usd`, `dex_volume_usd_dedup` (sum; known multi-hop overcount, `is_dedup_safe=false`), `ga_active_users` (last-value), `ga_new_users`, `gpay_topups_count`, `gpay_topups_volume_usd`. Six registered but unimplemented (`gno_staked`, `gpay_active_users`, `gpay_payment_volume_usd`, `chain_tx_count`, `bridge_inflow_usd`, `bridge_outflow_usd`).

**Canonical media variables (incentive / outlay):** three implemented — `validator_proposer_rewards_gno`, `ga_token_offer_emissions_usd`, `pools_lp_fee_apr_avg`. Five registered but unimplemented; two explicitly flagged `_scaffold_pending_data_source`.

**Control variables:** nine, all implemented — GNO/ETH/WXDAI prices, gas price, block count, ISO week-of-year, week index, `is_holiday_week`, `hardfork_step`.

**Spine:** 730-day trailing window, ISO Monday-start weeks, every cell materialised — sum methods fill 0, last/avg/weighted_avg methods fill NULL. Missing-week semantics confirmed correct by live data (no false zeros).

**Adstock:** geometric decay, lambda=0.5, 8-week window. Response curves: concave power and Hill S-curve; curve selection by holdout MAE. No smart-contract addresses are referenced in the MMM SQL layer; only protocol-event seeds (Cancun 2024-03-13, Pectra 2025-05-07) appear.

---

## Implementation assessment

### High

**Collinearity diagnostic emits NaN for ~18 of 28 media pairs; `is_high_collinearity` silently 0 for NaN**
(`models/execution/mmm/marts/fct_execution_mmm_collinearity_latest.sql`)

Five of eight media columns are all-zero or all-NULL (zero variance), so `corr()` returns NaN. The `is_high_collinearity` flag the `mmm_analyst` SOP reads to merge/drop/segment pairs (per Hakuhodo Guidebook p.38) evaluates to 0 for NaN comparisons — not 1, not NULL. The persona cannot distinguish "not collinear" from "undefined". Fix: filter out zero-variance columns before building pairs, or explicitly emit `is_high_collinearity=NULL` with a `status='insufficient_variance'` column for uncomputable pairs.

**Baseline adstock window has no PARTITION BY and source CTE has no ORDER BY**
(`models/execution/mmm/marts/fct_execution_mmm_baseline_latest.sql`)

`groupArray(...) OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)` treats the entire CTE as one partition. The CTE itself has no `ORDER BY`, so row delivery order is non-deterministic in ClickHouse. Adstock correctness requires strict week-ascending order per media series. Fix: add `PARTITION BY media_name` and compute from an explicitly `ORDER BY week` subquery.

### Medium

**`api_execution_mmm_spine_weekly` missing `window:` tag required by CI guard**
(`models/execution/mmm/marts/api_execution_mmm_spine_weekly.sql`)

The mart carries `api:mmm_spine` and `granularity:weekly` but omits `window:`. `scripts/checks/check_api_tags.py` enforces all four tag families (`api:`, `granularity:`, `window:`, `tier:`) on production `api:` endpoints. Fix: add `window:trailing_730d` (or agreed value) and confirm `tier:`.

**`fct_execution_mmm_baseline_latest` and `fct_execution_mmm_collinearity_latest` carry no `api:/granularity:/tier` tags despite direct persona consumption**
(`models/execution/mmm/marts/fct_execution_mmm_baseline_latest.sql`, `models/execution/mmm/marts/fct_execution_mmm_collinearity_latest.sql`)

Both are `table`-materialised and read directly by `mmm_analyst`, but tagged only `['production','mmm','execution','mart']`. Per `project_api_tag_convention.md`, consumer-facing marts should declare `api:/granularity:/tier` for the CI guard and MCP exposure control.

### Low

**`int_execution_mmm_media_weekly` incremental_strategy ternary is dead at parse time**
(`models/execution/mmm/intermediate/int_execution_mmm_media_weekly.sql`)

`incremental_strategy=('append' if start_month else 'delete+insert')` sits inside `config()` before `{% set start_month = var(...) %}` resolves, so the ternary is always falsy and always yields `delete+insert`. Round-2 run results confirm the model compiles and runs correctly with `insert_overwrite` (the effective strategy), so there is no live data defect — but the ternary is dead code and inconsistent with the other two intermediates' unconditional `insert_overwrite`. Fix: remove the ternary and use `insert_overwrite` unconditionally.

**`ctrl_eth_price` WHERE clause includes dead `'ETH'` symbol branch (latent duplicate-row risk)**
(`models/execution/mmm/intermediate/int_execution_mmm_controls_weekly.sql`)

`int_execution_token_prices_daily` contains only WETH (3,054 rows); ETH has zero rows. The filter `WHERE symbol IN ('WETH', 'ETH')` matches only WETH today — no duplicate `eth_usd_price_avg` rows are produced. Latent risk: if a future token-list migration introduces an ETH-symbol row, the `(week, symbol)` group would yield two rows and the spine LEFT JOIN would pick one arbitrarily. Fix: replace `IN ('WETH', 'ETH')` with `= 'WETH'` and add a comment on the canonical-symbol choice.

**`int_execution_mmm_media_weekly` lacks `unique_combination_of_columns` test on `(week, media_name)`**
(`models/execution/mmm/intermediate/schema.yml`)

The KPIs and controls intermediates both test grain uniqueness on `(week, kpi_name)` and `(week, control_name)`. The media intermediate omits the equivalent `(week, media_name)` test.

**`hardfork_step` cannot distinguish multiple distinct fork windows**
(`models/execution/mmm/intermediate/int_execution_mmm_controls_weekly.sql`)

The step logic counts forks with `fork_week <= current week` and maps `n > 0 → 1`. With two seeded forks (Cancun, Pectra), every post-Cancun week is 1 regardless — distinct structural breaks collapse into one step. Adequate for a single permanent-step control; unsuitable for segmented multi-fork analysis. Document the limitation, or emit per-fork step columns if segmentation is needed.

---

## Business-logic assessment

### High

**~Half the declared KPI/media universe is permanently empty, silently narrowing the model**
(`models/execution/mmm/intermediate/int_execution_mmm_kpis_weekly.sql`, `models/execution/mmm/intermediate/int_execution_mmm_media_weekly.sql`, `models/execution/mmm/marts/fct_execution_mmm_spine_weekly.sql`)

Five of thirteen KPIs and five of eight media are registered in seeds but have no source aggregator CTE (two media are explicitly `_scaffold_pending_data_source`). They land as NULL/0 across all 104 spine weeks. The registry advertises a scope the spine cannot deliver; an external consumer reading the registry would overestimate model coverage. Either implement the aggregators or remove the unwired columns from the published mart and registry until they are wired, recording an owner and ticket for the backlog.

**Primary engagement KPI under-powered: `ga_active_users` NULL 98/104 weeks; GA/GP below directional floor**
(`models/execution/mmm/intermediate/int_execution_mmm_kpis_weekly.sql`)

`kpi_ga_active_users` is NULL for 98 of 104 spine weeks. GA/GP source tables hold only ~21–24 non-NULL weeks as of April 2026 — below the 30-week directional-only floor documented in `mmm-user-guide.md`. Any MMM run targeting these KPIs is below the guidebook minimum. The persona's pre-flight non-NULL count is the correct guard; the limitation should additionally be surfaced in the mart (e.g., a per-KPI `non_null_weeks` / `readiness_tier` column) so it is impossible to fit silently on insufficient history.

### Medium

**`validator_apr_proxy` media is a documented reverse-causation trap**
(`models/execution/mmm/intermediate/int_execution_mmm_media_weekly.sql`)

`validator_apr_proxy` (`is_outlay=0`, sourced from `api_consensus_info_apy_latest`) is registered as a media variable, yet `mmm-user-guide.md` section 4 explicitly lists it as a "Bad pair" and reverse-causation trap (APR is derived from deposit volume; regressing deposits on APR produces circular attribution). The variable is currently empty, but its presence in the registry invites misuse once wired. Remove it from the registry or attach a prominent in-row/schema warning and exclude it from default media sets.

**`fct_execution_mmm_baseline_latest` is materially incomplete relative to its advertised scope**
(`models/execution/mmm/marts/fct_execution_mmm_baseline_latest.sql`)

The table yields ~18 rows (3 implemented media × 7 KPIs passing the `n_low_spend_weeks > 5` HAVING threshold) versus the intended 8 × 13. The five empty media produce no baseline rows at all — all-zero adstock fails the `NOT NULL` filter — with no status marker distinguishing "media not implemented" from "insufficient low-spend weeks". Add an explicit per-media coverage/status flag, and confirm whether the table auto-extends when additional media are wired.

### Low

**`dex_volume_usd_dedup` multi-hop overcount has no in-row magnitude bound**
(`models/execution/mmm/intermediate/int_execution_mmm_kpis_weekly.sql`)

`is_dedup_safe=false` correctly flags the overcount (per-tx dedup OOMs at the 10 GiB cluster cap) and is surfaced per row. Acceptable for the MCP-only internal persona. If external API exposure is contemplated, document or bound the expected overcount magnitude.

**No semantic-layer metrics for the unit**
(`models/execution/mmm/marts/api_execution_mmm_spine_weekly.sql`)

`semantic/authoring/execution/` has no `mmm` folder. MMM data cannot be queried via `query_metrics` / `quick_metric_chart` — all consumption is raw SQL. This is a documented design choice for the persona workflow, but it creates a discoverability gap and is not governable via the standard metric-governance surface.

---

## Data findings

Queries run across both rounds (18 total):

| Finding | Value |
|---|---|
| Spine rows / min week / max week | 104 / 2024-06-10 / 2026-06-01 |
| Last successful run | 2026-06-09 (all 7 models, 7–13 ms each) |
| Today vs spine end lag | 10 days (intentional: `today() - INTERVAL 7 DAY` trailing exclusion) |
| All upstream sources current within | 2 days of 2026-06-11 |
| `kpi_ga_active_users` NULL weeks | 98 / 104 (correctly NULL, not 0) |
| `kpi_ga_active_users` zero-valued weeks | 0 (confirmed: sumIf returns NULL for all-NULL groups) |
| `kpi_pools_tvl_usd` NULL / zero rows | 0 / 0 (fully populated) |
| WETH rows in price source | 3,054; ETH rows = 0 |
| Collinearity NaN pairs | ~18 / 28 (zero-variance placeholder media) |
| Baseline rows (live) | ~18 of intended ~104 |

The round-1 inspector erroneously read day-ordinal 20605 as 2025-06-09 (53-week stale); the correct ClickHouse resolution is `2026-06-01`. The "critical staleness" finding and the "live duplicate ETH price bug" were both fully retracted in round 2.

---

## Pros / Cons

**Pros**

- Sound architecture for purpose: long-form intermediates pivoted to a wide weekly spine, consumed by a gated three-persona pipeline that structurally enforces DAG causal review before attribution numbers are published.
- Continuous 730-day ISO-weekly spine materialises every cell; missing-week semantics (0 for sum methods, NULL for last/avg) are deliberate, documented, and confirmed correct in live data.
- Freshness is healthy: pipeline is current, last run 2 days ago, all upstream sources within 2 days.
- sumIf NULL semantics are correct and verified: last-value KPIs preserve NULL for missing weeks; no false-zero rows exist in the materialized spine.
- Honest about limits: `is_dedup_safe=false` flagged per row; data-sufficiency tiers (>=104 / 60–103 / 30–59 / <30 non-NULL weeks) documented in the user guide.
- Grain integrity tested on two of three intermediates (unique_combination on (week,kpi_name) and (week,control_name)), backed by ReplacingMergeTree order keys.
- Adstock and response-curve toolkit (lambda=0.5, concave power, Hill S-curve, marginal/avg ROI) clearly specified with simulator guardrails (±30 %/period, no zero-out, no extrapolation beyond 1.5× historical max).

**Cons**

- ~Half the spine carries no signal: 5 of 13 KPIs and 5 of 8 media are permanently empty because source aggregators were never wired; the registry advertises coverage the spine cannot deliver.
- Collinearity diagnostic is unreliable for ~64% of media pairs (NaN corr; flag silently 0 rather than NULL/status).
- Baseline table covers ~18 of ~104 intended rows — materially incomplete and silently so.
- Primary engagement proxy (`ga_active_users`) is NULL 98/104 weeks; GA/GP below the 30-week directional floor.
- Adstock window in `fct_execution_mmm_baseline_latest` lacks `PARTITION BY` and the source CTE has no `ORDER BY`, so geometric decay depends on undocumented ClickHouse row order.
- API-tag convention partially applied: `window:` tag missing from the API mart; two consumer-facing `fct_` marts carry no `api:/granularity:/tier` tags.
- Schema extension requires manual edits in three places (registry seed, intermediate UNION, spine sumIf clause) with no code-generation macro, inviting registry-vs-implementation drift.
- No semantic-layer metrics; unit is invisible to the metric-discovery surface.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| 1 | Fix `fct_execution_mmm_collinearity_latest`: filter zero-variance columns before pairing; emit `is_high_collinearity=NULL` with `status='insufficient_variance'` for uncomputable pairs so persona cannot mistake NaN/0 for "not collinear". | `fct_execution_mmm_collinearity_latest.sql` |
| 2 | Wire or quarantine the empty half: implement the 5 KPI and 5 media aggregator CTEs, or remove/quarantine their registry rows and spine columns until wired, and record owner + ticket for the backlog. | `int_execution_mmm_kpis_weekly.sql`, `int_execution_mmm_media_weekly.sql`, `fct_execution_mmm_spine_weekly.sql` |
| 3 | Fix adstock window in `fct_execution_mmm_baseline_latest`: add `PARTITION BY media_name` and compute from an explicitly `ORDER BY week` subquery to guarantee deterministic geometric decay. | `fct_execution_mmm_baseline_latest.sql` |
| 4 | Add `window:trailing_730d` (and confirm `tier:`) to `api_execution_mmm_spine_weekly`; add `api:/granularity:/tier` tags to both `fct_` persona-consumed marts. | `api_execution_mmm_spine_weekly.sql`, `fct_execution_mmm_baseline_latest.sql`, `fct_execution_mmm_collinearity_latest.sql` |
| 5 | Surface readiness on the mart: add per-KPI/per-media `non_null_weeks` and a derived `readiness_tier` column to `fct_execution_mmm_spine_weekly` so under-powered fits (e.g., `ga_active_users` at 6 non-NULL weeks) cannot be run silently. | `fct_execution_mmm_spine_weekly.sql` |
| 6 | Remove `validator_apr_proxy` from the default media registry, or attach a prominent reverse-causation warning and exclude from default media sets, per `mmm-user-guide.md` "Bad pair" guidance. | `mmm_media_registry.csv`, `int_execution_mmm_media_weekly.sql` |
| 7 | Replace the `int_execution_mmm_media_weekly` incremental_strategy ternary with unconditional `insert_overwrite` to remove dead parse-time branch and match the other two intermediates. | `int_execution_mmm_media_weekly.sql` |
| 8 | Change `ctrl_eth_price` `WHERE symbol IN ('WETH', 'ETH')` to `= 'WETH'` with a comment documenting the canonical-symbol choice, eliminating the latent duplicate-row path. | `int_execution_mmm_controls_weekly.sql` |
| 9 | Add `dbt_utils.unique_combination_of_columns` on `(week, media_name)` to the media intermediate schema for grain parity with KPI and control intermediates. | `intermediate/schema.yml` |
| 10 | Add an explicit per-media `status` flag to `fct_execution_mmm_baseline_latest` distinguishing "media not implemented" from "insufficient low-spend weeks"; confirm the table auto-extends when new media are wired. | `fct_execution_mmm_baseline_latest.sql` |

---

## Open disagreements

None. Both agents converged.

---

## Review log

| Round | Agent | Challenge | Outcome |
|---|---|---|---|
| 1 | Inspector | Reported max(week)=2025-06-09, ~53-week staleness (critical) | Retracted in round 2: day-ordinal 20605 = 2026-06-01; spine is current |
| 1 | Inspector | Reported ctrl_eth_price produces live duplicate-week rows (critical) | Downgraded to low in round 2: ETH symbol = 0 rows in price source; bug is latent only |
| 1 | Inspector | Reported sumIf turns last-value NULL weeks to 0 in spine (high) | Retracted by context agent in round 2: ClickHouse sumIf over all-NULL group returns NULL, not 0; confirmed by live query (ga_zeros=0) |
| 2 | Context | Challenged inspector on freshness and sumIf semantics with warehouse evidence | Both challenges accepted; inspector retracted both round-1 findings accordingly |
| 2 | Inspector | Issued no further challenges to context | N/A |
