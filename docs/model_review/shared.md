# Model review: shared

**Convergence:** converged in 1 round — both agents independently confirmed identical row counts, staleness figures, and structural findings with no conflicting claims.

---

## Scope and inventory

The shared sector contains exactly three SQL models, all under `models/shared/marts/`:

| Model | Grain | Rows | Purpose |
|---|---|---|---|
| `dim_time_spine_daily` | Day | 4,575 | Primary calendar axis, genesis to today+5yr |
| `dim_time_spine_weekly` | ISO Monday week | 654 | Derived from daily via `toMonday()` |
| `dim_time_spine_monthly` | Month (1st of month) | 151 | Derived from daily via `toStartOfMonth()` |

These are pure reference dimension tables. There are no business metrics, no protocol logic, and no API endpoints. Their role is to provide a dense, gap-free, grain-aligned calendar axis for the MetricFlow semantic planner and for any intermediate model that requires gap-filling via `CROSS JOIN`.

---

## Business context

The unit answers one infrastructure question: what is the complete, gap-free, Monday-anchored calendar for Gnosis Chain history and the next five years, at day, week, and month granularity?

**Canonical definitions:**

- `dim_time_spine_daily`: one row per calendar day from `2018-10-08` (Gnosis Chain / xDAI mainnet launch, cross-verified in `seeds/tokens_whitelist` and `cerebro-docs/docs/reference/dune-queries.md`) through `today() + 5 years`. Column `day` is `Date`, NOT NULL, UNIQUE.
- `dim_time_spine_weekly`: `SELECT DISTINCT toMonday(day) FROM dim_time_spine_daily`. Column `week` is `Date`, Monday-anchored, NOT NULL, UNIQUE. Monday-anchor is the project-wide convention enforced across all weekly marts.
- `dim_time_spine_monthly`: `SELECT DISTINCT toStartOfMonth(day) FROM dim_time_spine_daily`. Column `month` is `Date`, first-of-month, NOT NULL, UNIQUE. Starts `2018-10-01` (not `2018-10-08`) to avoid a partial October 2018 row — this is intentional and documented in SQL comments.

**Contract context:** no protocol contracts, addresses, seeds, or token whitelists are involved. The only external dependency is the ClickHouse `numbers()` table function. The genesis anchor `2018-10-08` is a hardcoded string literal. All three models are declared as semantic models in `semantic/authoring/shared/semantic_models.yml` with `quality_tier: approved` and `metrics: []` (intentionally zero metrics). Thirteen cross-sector relationships in `semantic/relationships/time_spines.yml` bind these spines to every weekly- and monthly-grain analyst-facing mart on the platform.

---

## Implementation assessment

**Medium — All three spines are 55 days short of their declared 5-year future horizon**

`dim_time_spine_daily` max = `2031-04-17` (4,575 rows); expected max if rebuilt today is `2031-06-11` (4,630 rows). The weekly and monthly derivatives inherit the same shortfall (weekly max `2031-04-14`, monthly max `2031-04-01`). Root cause: `materialized='table'` produces a static snapshot, and the models do not appear to be included in any regular pipeline run. Any forward-looking metric or gap-fill joining beyond `2031-04-17` will silently return no rows.
Affected: `models/shared/marts/dim_time_spine_daily.sql`, `dim_time_spine_weekly.sql`, `dim_time_spine_monthly.sql`

**Medium — No `order_by` or `engine` configured; ClickHouse falls back to `ORDER BY (tuple())`**

None of the three models carry an `engine` or `order_by` in `config()`. dbt-clickhouse 1.9.1 silently emits `ORDER BY (tuple())` as the default, meaning no primary-key index. Safe at the current row count (~4,600 rows) but MetricFlow cross-grain semantic joins will full-scan rather than range-scan. Fix: add `order_by=['day']`, `order_by=['week']`, `order_by=['month']` to each model's `config()` block.
Affected: all three `models/shared/marts/` SQL files

**Low — No dbt `tags:` defined on any of the three models**

Neither `schema.yml` nor `config()` blocks carry any tags. Without them, `dbt run --select tag:time_spine` is not possible and CI grep-based tooling cannot categorise these models without relying on the `dim_` naming prefix alone.
Affected: `models/shared/marts/schema.yml`

**Low — No rebuild cadence documented or enforced for table-materialized spines**

The 55-day staleness implies the spines are excluded from all regular pipeline runs. `refresh.py` has no explicit selection for `dim_time_spine_*`. Without a documented daily cron or pipeline inclusion, the horizon shortfall compounds by one day per day.
Affected: `models/shared/marts/dim_time_spine_daily.sql`

**Low — Genesis anchor hardcoded as a string literal rather than a `dbt_project.yml` var**

`2018-10-08` appears as a bare string literal in `dim_time_spine_daily.sql`. Other project-wide date anchors (`circles_target_group_start_date`, `gnosis_app_wau_floor_date`) are managed as `dbt_project.yml` vars. Promoting this to `gnosis_chain_genesis_date` would enable single-point updates and self-documentation.
Affected: `models/shared/marts/dim_time_spine_daily.sql`

---

## Business-logic assessment

**Medium — Semantic relationship key mismatch: `day` vs `date` on `daily_spine_to_consensus_validators_active`**

In `semantic/relationships/time_spines.yml`, the relationship `daily_spine_to_consensus_validators_active` maps `left_keys: [day]` to `right_keys: [date]`. All 12 other spine relationships use matching key names. Whether MetricFlow resolves this asymmetry correctly is undocumented. If it does not, cross-sector queries involving validator active counts joined to the daily spine will silently produce a cross-join or empty result rather than a build error.
Affected: `semantic/relationships/time_spines.yml`

**Low — Monthly spine starts `2018-10-01`, 7 days before chain genesis; October 2018 bucket includes phantom pre-genesis days**

The choice is intentional (month-boundary alignment, documented in SQL comments) and produces correct zero-padded behavior. However, any "sum since genesis" aggregate at monthly grain will over-count the calendar range by 7 days. The implication is not surfaced in `schema.yml` descriptions for consumers who do not read the SQL source.
Affected: `models/shared/marts/dim_time_spine_monthly.sql`

**Low — No CI check that new weekly/monthly marts register in `time_spines.yml`**

The platform convention requires every new weekly or monthly mart to add itself to `semantic/relationships/time_spines.yml`. There is no CI guard enforcing this. A mart added without a spine bridge will be queryable in isolation but will not participate in cross-sector semantic composition, producing silent coverage gaps rather than a build error.
Affected: `semantic/relationships/time_spines.yml`

---

## Data findings

Seven warehouse queries were run by the inspector:

| Check | Result |
|---|---|
| `dim_time_spine_daily` row count | 4,575 (expected 4,630 if rebuilt today) |
| `dim_time_spine_daily` min / max | `2018-10-08` / `2031-04-17` |
| `dim_time_spine_weekly` unique weeks | 654; max `2031-04-14` |
| `dim_time_spine_monthly` unique months | 151; max `2031-04-01` |
| Gap check on daily spine | 0 genuine gaps (single `gap_count=1` was null-default for first frame row, not a missing date) |
| Monday-alignment on weekly spine | All rows confirmed Monday-aligned |
| First-of-month check on monthly spine | All rows confirmed |

The 55-row shortfall is internally consistent across all three tables (55 days, ~7–8 weeks, 2 months), confirming a single last-rebuild date of approximately 2026-04-17.

---

## Pros / Cons

**Pros**

- Minimal, single-purpose unit with a clear and well-documented contract; no business logic to drift
- Genesis anchor `2018-10-08` is cross-verified against `seeds/tokens_whitelist` and external docs, not just asserted in SQL
- Monday-anchored week convention is consistently enforced project-wide and documented in `semantic/README.md`
- All three semantic model declarations are complete (`semantic_models.yml`), `quality_tier: approved`, `metrics: []` intentionally
- Thirteen cross-sector relationships in `time_spines.yml` anchor the planner to every registered weekly and monthly mart
- `not_null` and `unique` schema tests in place on all three grain columns
- `check_api_tags.py` CI correctly excludes these models (no `api:` tag needed for dimension tables)
- Monthly spine `2018-10-01` start is deliberate and documented inline

**Cons**

- All three spines are 55 days stale; table materializations are static and models appear absent from any pipeline run
- No `order_by` configured; ClickHouse silently emits `ORDER BY (tuple())`, losing primary-key index
- No dbt tags on any model; `dbt run --select tag:time_spine` is not possible
- No automated enforcement that new weekly/monthly marts register themselves in `time_spines.yml`
- Genesis date hardcoded as a string literal rather than a `dbt_project.yml` var
- `day` vs `date` key mismatch in one semantic relationship is undocumented and potentially silently broken
- No hourly or quarterly spine; p2p and ESG metrics cannot participate in cross-sector semantic composition
- October 2018 monthly bucket covers 7 phantom pre-genesis days; implication not surfaced in `schema.yml`

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P1 | Run `dbt run --select shared` immediately to restore the 55-day horizon shortfall; verify max dates reach `2031-06-11` / `2031-06-09` / `2031-06-01` | all three `models/shared/marts/` |
| P1 | Add `dim_time_spine_*` to the regular daily pipeline run (or a dedicated lightweight cron) so the horizon never drifts more than 1 day | `models/shared/marts/dim_time_spine_daily.sql` |
| P2 | Add `order_by=['day']`, `order_by=['week']`, `order_by=['month']` to each model's `config()` block to obtain an explicit ClickHouse primary-key index | all three `models/shared/marts/` |
| P2 | Investigate and document the `left_keys: [day]` / `right_keys: [date]` mismatch in `daily_spine_to_consensus_validators_active`; align key names or add a comment confirming MetricFlow resolves it | `semantic/relationships/time_spines.yml` |
| P3 | Add `tags=['shared', 'time_spine']` to all three models to enable tag-based selection and CI categorisation | `models/shared/marts/schema.yml` |
| P3 | Promote `2018-10-08` to a `dbt_project.yml` var (`gnosis_chain_genesis_date`) consistent with other project-wide date anchors | `models/shared/marts/dim_time_spine_daily.sql` |
| P3 | Add a `schema.yml` description note on `dim_time_spine_monthly` calling out the `2018-10-01` start and the October 2018 partial-period implication | `models/shared/marts/schema.yml` |
| P4 | Add a CI check (analogous to `check_api_tags.py`) that every weekly- and monthly-grain mart has a corresponding entry in `semantic/relationships/time_spines.yml` | `semantic/relationships/time_spines.yml` |
| P4 | Document or formally defer the hourly spine gap; a GitHub issue or `docs/TODO` entry would prevent the gap from being rediscovered | `semantic/relationships/time_spines.yml` |
| P4 | Consider setting `expose_to_mcp: false` explicitly in `schema.yml` meta blocks to prevent accidental surface of spine models as analyst-facing endpoints | `models/shared/marts/schema.yml` |

---

## Open disagreements

None. Both agents converged fully in round 1.

---

## Review log

| Round | Agent | Challenge | Outcome |
|---|---|---|---|
| 1 | Inspector | Ran 7 warehouse queries to verify row counts, gaps, grain alignment, and staleness | Resolved — all checks passed; 55-day shortfall confirmed |
| 1 | Context | Cross-verified genesis anchor in seeds and external docs; validated semantic model declarations and relationship count | Resolved — no discrepancies found |
