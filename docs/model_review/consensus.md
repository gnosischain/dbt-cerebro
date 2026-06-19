# Model review: consensus

**Convergence:** converged in 1 round — all critical and high findings independently confirmed by both inspector shards via code reads and warehouse queries; one minor inspector discrepancy (claim that the `(date, status)` unique test was absent from `int_consensus_validators_status_daily` schema.yml when it is present at lines 171-176) does not affect any verdict.

---

## Scope and inventory

The consensus unit covers the full Gnosis Chain Beacon Chain analytics stack. It spans approximately 90 models.

| Layer | Count | Purpose |
|---|---|---|
| Staging | 10 SQL + 1 schema.yml (840 lines) | Thin views over `consensus.*` ClickHouse sources, all using `FINAL` for ReplacingMergeTree dedup |
| Intermediate | 24 SQL + 1 schema.yml (1,874 lines) | Core business logic: income/APY, snapshots, status, deposits, withdrawals, consolidations, entry queue, attestations, blocks, graffiti |
| Fact | ~10 SQL | Materialized aggregates serving the mart layer: `fct_consensus_validators_explorer_latest`, `fct_consensus_validators_status_latest`, `fct_consensus_info_latest`, `fct_consensus_validators_dists_last_30_days`, etc. |
| API marts | ~54 SQL + 1 schema.yml (2,366 lines) | Thin views and materialized tables serving the dashboard REST API, MCP semantic layer, and cross-sector joins |

Primary data source: a beacon-node crawler writing to `consensus.*` SharedReplacingMergeTree tables in ClickHouse Cloud.

---

## Business context

**Intended purpose.** The unit answers six classes of business questions: (1) staking health dashboard KPIs (active validators, staked GNO, network APY, daily deposit/withdrawal flows); (2) per-operator Validator Explorer drill-down (income, APY, balance distribution by withdrawal credential); (3) network performance monitoring (attestation inclusion delay, block production, blob commitments); (4) protocol upgrade tracking (fork history, 0x00/0x01/0x02 credential migration rate); (5) client diversity via graffiti parsing; and (6) cross-sector identity resolution linking validator withdrawal addresses to the `user_pseudonym` join space shared with Gnosis Pay, Circles, and Gnosis App.

**Canonical definitions (relevant to findings below).**

- _Active validator_: `status = 'active_ongoing'` (primary); counted from `int_consensus_validators_status_daily`. Excludes `exited_*`, `pending_*`, `withdrawal_*`.
- _Staked GNO (canonical)_: `SUM(effective_balance_gwei) / 1e9 / 32` across all validators, yielding a validator-slot-count proxy. Schema descriptions say "total GNO currently staked" but the warehouse value is ~334k vs 10.7M true GNO — a 32x discrepancy whose intentionality is disputed (see business-logic findings).
- _Network APY_: balance-weighted annualised yield from `fct_consensus_validators_apy_mean_daily`, filtered to `apy in (0, 200)` and `balance_prev > 0`. Gnosis constants: `BASE_REWARD_FACTOR=25`, `EPOCHS_PER_DAY=1080`, `SECONDS_PER_SLOT=5`.
- _Per-validator APY_: `daily_rate * 365 * 100`, spec-bounded by the base-reward cap. The old mod-32-GNO deposit-rounding trick was retired; `int_consensus_validators_per_index_apy_daily` is now a thin pass-through of `int_consensus_validators_income_daily.apy`.
- _Validator operator_: a unique 0x01-credential EVM withdrawal address, hashed via `sipHash64(lower(address))` to `user_pseudonym`. Currently 0x02 compounders are excluded (see findings).
- _EIP-7251 consolidation_: self-consolidation (credential switch, no balance transfer) or cross-consolidation (source exits, balance transfers to target). `transferred_amount_gno` uses the source's last real balance (v5 fix).

**Contract context.** No hardcoded contract addresses appear in SQL. The unit reads exclusively from `consensus.*` ClickHouse tables. Cross-sector integration: 0x01 withdrawal addresses project into the `user_pseudonym` space shared across GP/GA/Circles/Mixpanel. A `future-validator-gpay-modeling.md` design exists but is not active.

---

## Implementation assessment

### Critical

**apy_30d formula overstates APY by ~30x in two materialized tables**
`models/consensus/marts/fct_consensus_validators_explorer_latest.sql`, `models/consensus/marts/fct_consensus_validators_explorer_members_table.sql`

Both tables compute `SUM(consensus_income_amount_gno) / NULLIF(AVG(effective_balance_gno), 0) * 365 * 100` over a 30-day window. `SUM` accumulates 30 days of income per credential-group but `AVG` averages per-row (per-validator-per-day), making the ratio ~30x the correct daily rate before the `*365` multiplier. The fix is `SUM(income_30d) / 30 / AVG(eff_balance) * 365 * 100`, or equivalently `AVG(daily_income) / AVG(eff_balance) * 365 * 100` to match the compounding convention in `int_consensus_validators_income_daily.apy`. Warehouse-confirmed (see data findings).

**int_consensus_validators_labels bypasses ref() and carries dev tag, breaking production withdrawal_addresses pipeline**
`models/consensus/intermediate/int_consensus_validators_labels.sql`, `models/consensus/intermediate/int_consensus_validators_withdrawal_addresses.sql`

The model references `consensus.stg_consensus__validators` as a bare table name (not `{{ ref() }}`) in both the `FROM` clause and the `MAX(slot)` subquery. It carries `tags=['dev', ...]`, meaning it is excluded from production dbt runs. Its downstream consumer `int_consensus_validators_withdrawal_addresses.sql` uses `{{ ref('int_consensus_validators_labels') }}` and will fail in production or silently read a stale/absent table. This breaks `fct_consensus_validators_withdrawal_addresses_distinct` and the entire `user_pseudonym` cross-sector join surface for all 0x01 validators.

### High

**int_consensus_validators_status_daily SQL/schema drift: four documented columns not emitted**
`models/consensus/intermediate/int_consensus_validators_status_daily.sql`, `models/consensus/intermediate/schema.yml`

The SQL emits only `(date, status, cnt)`. The schema.yml additionally documents `total_validators`, `active_validators`, `exited_validators`, and `slashed_validators` with elementary anomaly tests — none of which exist in the `SELECT`. Note: the `unique_combination_of_columns` test on `(date, status)` is correctly present (lines 171-176). The four ghost columns will fail any catalog-driven schema test.

**stg_consensus__validators and stg_consensus__validators_all: unique test on validator_index is wrong grain**
`models/consensus/staging/stg_consensus__validators.sql`, `models/consensus/staging/stg_consensus__validators_all.sql`, `models/consensus/staging/schema.yml`

Both views SELECT from `consensus.validators FINAL` with no slot filter, returning all historical snapshot rows (one per slot per validator). A unique test on `validator_index` alone fails whenever the source contains more than one intraday snapshot — the normal operating mode. The correct uniqueness grain is `(slot, validator_index)`.

**stg_consensus__withdrawals: unique tests on block_hash and validator_index are wrong grain**
`models/consensus/staging/stg_consensus__withdrawals.sql`, `models/consensus/staging/schema.yml`

Multiple withdrawals exist per block and per validator. Warehouse-confirmed: 970,863 recent rows, 121,411 unique `block_hash`es, 98,130 unique `validator_index`es. Both unique tests fail on every run. The correct uniqueness grain is `(slot, withdrawal_index)`.

**stg_consensus__blocks: unique test on eth1_block_hash is wrong grain**
`models/consensus/staging/stg_consensus__blocks.sql`, `models/consensus/staging/schema.yml`

Multiple consecutive beacon slots reference the same eth1 block (eth1 view updates only every ~4 epochs). The unique test on `eth1_block_hash` fails on any multi-slot window. The true uniqueness grain is `slot`.

**fct_consensus_info_latest INNER JOIN silently drops KPI rows when a status class is absent exactly 7 days ago**
`models/consensus/marts/fct_consensus_info_latest.sql`

The final SELECT joins `info_latest` and `info_7d` with `INNER JOIN ON label`. Any validator status class with zero validators exactly 7 days before today is absent from `info_7d` and its row is silently dropped from the KPI card output. All five `api_consensus_info_*_latest` views inherit this gap. A `LEFT JOIN` with `COALESCE(t2.value, 0)` is the fix. Currently latent but will manifest during slashing events.

**0x02-credential validators silently excluded from withdrawal_addresses_distinct cross-sector join**
`models/consensus/intermediate/int_consensus_validators_withdrawal_addresses.sql`, `models/consensus/marts/fct_consensus_validators_withdrawal_addresses_distinct.sql`

`int_consensus_validators_withdrawal_addresses` only handles `0x01` credentials (`CASE WHEN startsWith '0x01'`). Post-Pectra EIP-7251 compounders (`0x02`) have no branch. Warehouse-confirmed: 6,712 active 0x02 validators with non-NULL `withdrawal_address` are entirely excluded from the `user_pseudonym` cross-sector join. This under-counts the addressable validator operator population and the gap grows as operators migrate to 0x02 post-Pectra.

**api_consensus_validators_performance_daily joins two ReplacingMergeTree sources without FINAL**
`models/consensus/marts/api_consensus_validators_performance_daily.sql`, `models/consensus/marts/api_consensus_validators_performance_latest.sql`

The views read `int_consensus_validators_income_daily` and `int_consensus_validators_proposer_rewards_daily` directly without `FINAL`. During CH Cloud background merge windows, a plain `SELECT` can return duplicate `(date, validator_index)` rows, doubling income, balance, and proposer-reward figures for affected rows. No uniqueness test guards these endpoints.

### Medium

**int_consensus_entry_queue_daily: schema.yml documents per-validator columns that the SQL does not emit**
`models/consensus/intermediate/int_consensus_entry_queue_daily.sql`, `models/consensus/intermediate/schema.yml`

SQL groups by `date` only, producing `(date, validator_count, q05..q95, mean)`. The schema.yml documents `validator_index` (with unique and not_null tests), `epoch_eligibility`, `epoch_activation`, and `activation_days` — none of which exist in the grouped output. The unique test on `validator_index` will fail constantly.

**int_consensus_deposits_withdrawals_daily: CTE typos and column name mismatch**
`models/consensus/intermediate/int_consensus_deposits_withdrawals_daily.sql`, `models/consensus/intermediate/schema.yml`

CTEs are named `deposists` and `deposists_requests` (typo for `deposits`). The final `SELECT` emits `total_amount` but the schema.yml documents the column as `amount`. Marts reading by column name receive NULLs or query errors.

**int_consensus_blocks_daily: schema.yml documents intermediate-computation columns as output columns**
`models/consensus/intermediate/int_consensus_blocks_daily.sql`, `models/consensus/intermediate/schema.yml`

The SQL uses `genesis_time_unix` and `seconds_per_slot` only as intermediate values inside a `CASE` subquery. The final `SELECT` emits `(date, blocks_produced, total_blob_commitments, blocks_with_zero_blob_commitments, blocks_missed)`. The schema.yml documents the two intermediate values as output columns and omits `total_blob_commitments` and `blocks_with_zero_blob_commitments`.

**int_consensus_validators_income_daily: INNER JOIN to network_state silently drops validator-days on gap dates**
`models/consensus/intermediate/int_consensus_validators_income_daily.sql`

The `daily_raw` CTE uses `INNER JOIN network_state n ON n.date = s.date`. If `int_consensus_validators_balances_daily` is missing a day due to an incremental run boundary or crawler outage, all validators for that day are silently dropped from `income_daily`, producing a complete gap in cumulative totals. Risk is elevated given the 6-day gap currently observed in the warehouse (see data findings).

**as_of_date in api_consensus_info_*_latest views hard-coded to deposits_withdrawals_daily max date**
`models/consensus/marts/api_consensus_info_apy_latest.sql`, `models/consensus/marts/api_consensus_info_staked_latest.sql`, `models/consensus/marts/api_consensus_info_active_ongoing_latest.sql`

All five `api_consensus_info_*_latest` views hard-code `as_of_date = max(date) FROM int_consensus_deposits_withdrawals_daily`. APY and staked rows derive from `int_consensus_validators_dists_daily` and `int_consensus_validators_balances_daily` respectively, which may have different max dates. The displayed `as_of_date` can overstate freshness for APY and staked KPIs.

**fct_consensus_info_latest schema.yml declares ghost columns cnt and total_amount**
`models/consensus/marts/fct_consensus_info_latest.sql`, `models/consensus/marts/schema.yml`

The schema.yml lists five columns: `label`, `cnt`, `total_amount`, `value`, `change_pct`. The actual SQL only projects `label`, `value`, `change_pct`. `describe_table` confirmed only three columns exist. The ghost declarations fail catalog-driven schema tests.

**stg_consensus__validators_all description copy-pasted from stg_consensus__validators**
`models/consensus/staging/stg_consensus__validators_all.sql`, `models/consensus/staging/schema.yml`

The schema.yml description reads "focusing on active validators with a positive balance" — identical to `stg_consensus__validators`. In reality `_all` includes all validators including exited and zero-balance (no `WHERE balance > 0`). Misleads consumers about the population scope.

**12 live consensus API endpoints exempt from column-schema enforcement via check_api_tags.allow**
`models/consensus/marts/api_consensus_consolidations_daily.sql`, `models/consensus/marts/api_consensus_validators_apy_dist_income_daily.sql`, `models/consensus/marts/api_consensus_validators_apy_mean_daily.sql`, `models/consensus/marts/api_consensus_validators_explorer_apy_dist_daily.sql`, `models/consensus/marts/api_consensus_validators_explorer_daily.sql`, `models/consensus/marts/api_consensus_validators_income_total_daily.sql` and 6 others

These endpoints are live but lack typed column schemas in `schema.yml`, disabling column-level CI coverage and preventing cerebro-api from publishing typed responses.

**api_consensus_validators_status_daily missing API meta block**
`models/consensus/marts/api_consensus_validators_status_daily.sql`

Unlike all other `api_*` endpoints, this view has no `meta={api:{...}}` config block, meaning no documented pagination limit, no `require_any_of` filter, and no `allow_unfiltered` declaration. The endpoint covers years of full status history; an unfiltered call could fetch the entire dataset. The CI tag guard does not check for meta block presence.

### Low

**int_consensus_graffiti_daily schema.yml contains garbage column entries from auto-generation**
`models/consensus/intermediate/int_consensus_graffiti_daily.sql`, `models/consensus/intermediate/schema.yml`

Columns named `in`, `precedence`, `separator-agnostic`, and `above` appear in the schema.yml with empty `data_types` and "not present in the provided SQL" descriptions. These are artifacts from parsing SQL comment text during schema auto-generation. They inflate the schema contract and confuse catalog tools.

**Tag typos produce non-canonical API endpoint paths**
`models/consensus/marts/api_consensus_deposits_withdrawls_volume_daily.sql`, `models/consensus/marts/api_consensus_validators_apy_dist_last_30_days.sql`, `models/consensus/marts/api_consensus_validators_balance_dist_last_30_days.sql`

`api_consensus_deposits_withdrawls_volume_daily` has tag `api: deposits_and_withdrawals_volume` with a space after the colon. Both `_apy_dist_last_30_days` and `_balance_dist_last_30_days` use `dississribution` (double `ti`) instead of `distribution`, generating non-canonical API paths that will not align with frontend routing expectations.

**api_consensus_forks uses today() for as_of_date**
`models/consensus/marts/api_consensus_forks.sql`

Fork data is static (hardcoded literal `arrayJoin` in `fct_consensus_forks.sql`). Using `today()` gives the false impression the data refreshes daily and may confuse freshness monitors.

---

## Business-logic assessment

### Critical

**Live dashboard serves apy_30d values ~30x true network APY**
`models/consensus/marts/fct_consensus_validators_explorer_latest.sql`, `models/consensus/marts/fct_consensus_validators_explorer_members_table.sql`

Warehouse-confirmed: median `apy_30d` = 229%, max = 3,322,693% vs true network APY ~10%. The Validator Explorer UI, the Explorer members table, and all MCP semantic candidates derived from these tables are currently serving these values. Any quarterly report or external communication citing per-operator APY from these tables is wrong.

### High

**Dashboard headline "Staked GNO" shows 334k instead of 10.7M GNO**
`models/consensus/marts/api_consensus_info_staked_latest.sql`, `models/consensus/marts/api_consensus_staked_daily.sql`, `models/consensus/marts/fct_consensus_info_latest.sql`

Warehouse: 111,478 active validators hold 10,713,273 GNO effective balance; `effective_balance / 32 = 334,790`. The schema description says "total GNO currently staked" but the value is 32x smaller. Whether the `/32` is intentional (a validator-slot-count proxy) or a longstanding error, the label and description are factually wrong to any external consumer interpreting the number as a GNO amount. Post-Pectra 0x02 validators with up to 2048 GNO EB will further inflate the misrepresentation.

**Dashboard KPI card APY reads from old unweighted path, not the canonical balance-weighted income-derived APY**
`models/consensus/marts/fct_consensus_info_latest.sql`, `models/consensus/marts/fct_consensus_validators_apy_mean_daily.sql`

`fct_consensus_info_latest` sources its APY label from `int_consensus_validators_dists_daily.avg_apy`. The canonical APY (per team memory and the context report) is the balance-weighted mean from `int_consensus_validators_income_daily` via `fct_consensus_validators_apy_mean_daily`. The two paths are not guaranteed to agree, and the dashboard KPI card is using the legacy unweighted path while the spec-bounded income-derived path exists and is used everywhere else.

**6,712 post-Pectra 0x02 validators missing from cross-sector user_pseudonym graph**
`models/consensus/intermediate/int_consensus_validators_withdrawal_addresses.sql`, `models/consensus/marts/fct_consensus_validators_withdrawal_addresses_distinct.sql`

Warehouse: 6,712 validators hold 0x02 credentials, all with non-NULL `withdrawal_address`. None are reachable via the `user_pseudonym` join space. Cross-sector analyses (validator operators who also use Gnosis Pay, Circles, etc.) silently under-count the addressable population, with the gap growing post-Pectra.

**4-day data lag across all physical tables with no error-severity freshness alert**
`models/consensus/intermediate/int_consensus_validators_snapshots_daily.sql`, `models/consensus/intermediate/int_consensus_validators_income_daily.sql`

All physical tables show `max_date = 2026-06-07` (lag = 4 days as of 2026-06-11). A 6-day gap exists in `snapshots_daily` (2026-06-01 through 2026-06-06 entirely absent; 2026-06-07 partial with 58,313 rows vs the expected ~558k). All elementary `freshness_anomalies` tests are `severity: warn`. A multi-day silent pipeline outage does not fail the build or trigger a pager alert.

### Medium

**fct_consensus_info_latest change_pct returns -100% for new-from-zero status classes**
`models/consensus/marts/fct_consensus_info_latest.sql`

Formula: `IF(t1=0 AND t2=0, 0, ROUND((COALESCE(t1/NULLIF(t2,0), 0) - 1)*100, 1))`. When `t2=0` and `t1>0`, `COALESCE` returns 0, yielding -100% instead of undefined/new. Currently latent (all 9 labels have non-zero 7-day values) but will flash on any slashing wave or status-class emergence.

**fct_consensus_validators_explorer_daily silently drops validators absent from the latest status snapshot**
`models/consensus/marts/fct_consensus_validators_explorer_daily.sql`

`INNER JOIN` to `fct_consensus_validators_status_latest` (current snapshot only) against `int_consensus_validators_income_daily` (historical). Any validator in income history but absent from the latest snapshot is omitted from per-credential daily rollups. Possible after a crawler gap or re-org.

**fct_consensus_validators_dists_last_30_days is a single-row snapshot described as a 30-day distribution**
`models/consensus/marts/fct_consensus_validators_dists_last_30_days.sql`

Confirmed: 1 row, `date = 2026-06-07`. Both `api_consensus_validators_apy_dist_last_30_days` and `api_consensus_validators_balance_dist_last_30_days` surface the same single row. Schema descriptions say "distribution over the last 30 days", suggesting a time series to new developers and MCP consumers.

**fct_consensus_forks hardcoded digest array will silently miss future forks**
`models/consensus/marts/fct_consensus_forks.sql`

The `fork_digests` CTE is a literal array of 7 tuples ending at Fulu. Any future fork not manually added will return zero rows with no error or warning.

---

## Data findings

Eight warehouse queries were run across the two inspector shards (six in staging/intermediate, eight in marts) plus one uniqueness check. Key numbers:

| Query | Result |
|---|---|
| `fct_consensus_validators_explorer_latest` apy_30d distribution | avg = 31,403%, median = 229%, max = 3,322,693% across 759 credentials |
| True network APY (from `int_consensus_validators_income_daily`) | ~10.07% |
| `int_consensus_validators_snapshots_daily` row counts, 2026-05-30 to 2026-06-11 | 558,297 rows (2026-05-30), 558,302 (2026-05-31), 0 rows 2026-06-01 to 2026-06-06, 58,313 partial rows (2026-06-07) |
| `int_consensus_validators_income_daily` max_date | 2026-06-07 (4-day lag) |
| `int_consensus_validators_status_daily` grain check | 4,921 rows = 4,921 unique `(date, status)` pairs; clean |
| stg_consensus__withdrawals unique check (last 7 days) | 970,863 rows, 121,411 unique block_hashes, 98,130 unique validator_indices; confirms wrong unique tests |
| `fct_consensus_validators_status_latest` dedup check | 558,313 rows = 558,313 unique validators at single slot; clean |
| Credential prefix distribution | 0x01: 550,767; 0x02: 6,712 (all with non-NULL withdrawal_address); 0x00: 834 |
| `api_consensus_info_staked_latest` vs active_ongoing | 334,875.9 staked value vs 111,478 active validators (ratio ~3.0); 10,713,273 true GNO |
| `api_consensus_credentials_daily` pct sum check | 0 of 1,643 dates deviate more than 0.1% from 100%; clean |
| `fct_consensus_validators_dists_last_30_days` row count | 1 row confirmed |
| `int_consensus_blocks_daily` negative subtraction check | 0 negative rows across 1,640 daily rows |
| `fct_consensus_validators_explorer_daily` grain/freshness | 334 rows, 334 unique `(withdrawal_credentials, date)` pairs in last 7d; max_date = 2026-06-07 |

---

## Pros / Cons

**Pros**

- Staging `FINAL` convention is correctly enforced across all 10 staging views; the schema.yml preamble explicitly documents why, protecting all downstream models from ReplacingMergeTree read-time duplicates.
- Spec-bounded APY/income formula in `int_consensus_validators_income_daily` is rigorously documented with explicit Gnosis Chain constants and handles post-Pectra effective-deposit-credit accounting (EIP-7002/7251 queue drain) correctly.
- ReplacingMergeTree + `insert_overwrite` incremental strategy with `append` fallback for microbatch paths is architecturally sound and avoids the `ALTER...DELETE` mutation that caused code-341 OOM at 451M rows.
- `FAR_FUTURE_EPOCH` sentinel (2^64-1) is explicitly NULL-guarded in `fct_consensus_validators_explorer_members_table` before timestamp conversion, preventing year-292-billion rendering in the UI.
- EIP-7251 consolidation accounting is deeply thought-through: v5 switched to real balance to eliminate residual; dedup on `is_self_consolidation` prevents 8x inflation on resubmission events.
- `fct_consensus_validators_status_latest` dedup is confirmed correct: 558,313 rows = 558,313 unique validators at a single slot; the foundation table for the Explorer and search endpoints is clean.
- API `meta` blocks with `require_any_of` and pagination are properly configured on per-validator time-series endpoints, preventing unfiltered full-table scans on tier-1 performance endpoints.
- `api_consensus_credentials_daily` percentage calculation is confirmed clean: 0 of 1,643 dates deviate more than 0.1% from 100%.

**Cons**

- The worst live data quality issue in this codebase: `apy_30d` is being served to the Validator Explorer dashboard and MCP at median 229% and max 3.3M% while true network APY is ~10%.
- The `dev`-tagged `int_consensus_validators_labels` with bare table references makes `int_consensus_validators_withdrawal_addresses` a broken production view, severing the `user_pseudonym` cross-sector join for all 0x01 validators in production.
- At least six schema.yml files document columns that do not exist in their SQL output; the schema contract is systematically unreliable for catalog-driven consumers.
- Four wrong unique-test grains across staging will fail on every `dbt test` run against production data, generating constant alert noise and masking real failures.
- The dashboard KPI card APY reads from the legacy unweighted `avg_apy` path rather than the canonical balance-weighted `fct_consensus_validators_apy_mean_daily`, silently diverging from the team's own canonical definition.
- A 4-day pipeline lag with a 6-day snapshot gap and all freshness tests at `severity: warn` means a multi-day outage produces no build failure and no pager alert.
- 6,712 post-Pectra 0x02 validators are entirely absent from the `user_pseudonym` cross-sector join surface, with the gap growing as operators migrate to 0x02.
- 12 live API endpoints are on the `check_api_tags.allow` exemption list for missing or untyped schemas, disabling column-level CI coverage.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Fix `apy_30d` formula: divide by window length (`SUM(income_30d) / 30 / AVG(eff_balance) * 365 * 100`); decide simple vs compound annualization to match `int_consensus_validators_income_daily.apy` convention; rebuild both tables; add a sanity-check test asserting `apy_30d < 50` at the 99th percentile for `active_ongoing` validators | `fct_consensus_validators_explorer_latest`, `fct_consensus_validators_explorer_members_table` |
| P0 | Remove the `dev` tag from `int_consensus_validators_labels` and replace its bare `stg_consensus__validators` table reference with `{{ ref('stg_consensus__validators') }}`; if the model is intentionally retired, update `int_consensus_validators_withdrawal_addresses` to read withdrawal addresses directly from `fct_consensus_validators_status_latest` (which already extracts both 0x01 and 0x02 `withdrawal_address`) | `int_consensus_validators_labels`, `int_consensus_validators_withdrawal_addresses` |
| P1 | Add 0x02 credential handling to `int_consensus_validators_withdrawal_addresses`: extend the `CASE` statement with `WHEN startsWith(withdrawal_credentials, '0x02')` using the same substring offset as 0x01; propagates automatically to `fct_consensus_validators_withdrawal_addresses_distinct` and the `user_pseudonym` join space | `int_consensus_validators_withdrawal_addresses`, `fct_consensus_validators_withdrawal_addresses_distinct` |
| P1 | Fix all wrong unique-test grains: (a) `stg_consensus__validators` and `stg_consensus__validators_all`: change to `unique_combination_of_columns` on `(slot, validator_index)`; (b) `stg_consensus__withdrawals`: replace `block_hash` unique with `(slot, withdrawal_index)` and remove the `validator_index` unique test; (c) `stg_consensus__blocks`: replace `eth1_block_hash` unique with `slot` unique | `staging/schema.yml` |
| P1 | Fix `fct_consensus_info_latest`: (a) change the `info_latest`/`info_7d` join from `INNER JOIN` to `LEFT JOIN` with `COALESCE(t2.value, 0)` to prevent silent KPI-row drops; (b) migrate the APY label source from `int_consensus_validators_dists_daily.avg_apy` to `fct_consensus_validators_apy_mean_daily` to align the dashboard KPI card with the canonical balance-weighted spec-bounded APY; (c) fix `change_pct` formula to handle `t2=0, t1>0` as `NULL` or `+inf` rather than -100% | `fct_consensus_info_latest` |
| P1 | Elevate at least one elementary freshness test per physical consensus table from `severity: warn` to `severity: error` (or add a `dbt source freshness` test with error threshold > 2 days) so a pipeline outage of 4+ days fails the build | all physical consensus fact/mart tables |
| P1 | Correct the "Staked GNO" schema descriptions across `fct_consensus_info_latest`, `api_consensus_staked_daily`, and `api_consensus_info_staked_latest`: either rename to `validator_slot_count` with explicit documentation of the `/32` normalisation, or remove the `/32` to surface true GNO staked; document the decision in schema.yml and the semantic model | `api_consensus_staked_daily`, `api_consensus_info_staked_latest`, `fct_consensus_info_latest` |
| P2 | Add `FINAL` to the `int_consensus_validators_income_daily` and `int_consensus_validators_proposer_rewards_daily` references in `api_consensus_validators_performance_daily` and `api_consensus_validators_performance_latest` | `api_consensus_validators_performance_daily`, `api_consensus_validators_performance_latest` |
| P2 | Fix `int_consensus_validators_status_daily` schema.yml: remove the four ghost columns (`total_validators`, `active_validators`, `exited_validators`, `slashed_validators`) or add them to the SQL `SELECT` | `int_consensus_validators_status_daily`, `intermediate/schema.yml` |
| P2 | Fix schema.yml for `int_consensus_entry_queue_daily` (remove `validator_index`, `epoch_eligibility`, `epoch_activation`, `activation_days`), `int_consensus_deposits_withdrawals_daily` (rename `amount` to `total_amount`; fix `deposists` CTE typos), `int_consensus_blocks_daily` (remove `genesis_time_unix` and `seconds_per_slot`; add `total_blob_commitments` and `blocks_with_zero_blob_commitments`), and `int_consensus_graffiti_daily` (remove garbage columns `in`, `precedence`, `separator-agnostic`, `above`) | various `intermediate/schema.yml` entries |
| P2 | Add API `meta` block to `api_consensus_validators_status_daily` with `require_any_of` filter and appropriate pagination limit to prevent unfiltered full-history fetches | `api_consensus_validators_status_daily` |
| P3 | Fix tag typos: (a) remove space after colon in `api: deposits_and_withdrawals_volume`; (b) correct `dississribution` to `distribution` in both APY and balance dist last-30d tags | `api_consensus_deposits_withdrawls_volume_daily`, `api_consensus_validators_apy_dist_last_30_days`, `api_consensus_validators_balance_dist_last_30_days` |
| P3 | Work down the 12 live endpoints on the `check_api_tags.allow` exemption list by filling their typed column schemas in schema.yml and removing the allow entries; prioritise the Explorer endpoints given they already have active consumers | 12 endpoints in `marts/schema.yml` |
| P3 | Replace `today()` with a data-derived `max(date)` in `api_consensus_forks` (or add a row-count schema test asserting 7 known forks are present) to prevent freshness monitor confusion and detect future forks silently missing from the hardcoded `fork_digests` array | `api_consensus_forks`, `fct_consensus_forks` |

---

## Open disagreements

None. Review converged in round 1.

---

## Review log

Round 1 — three parallel shards: staging/intermediate inspector, marts-1 inspector (files 1-27), marts-2 inspector (files 28-54), and a context agent.

- All inspectors independently converged on the `apy_30d` overstatement; marts-2 shard confirmed with a direct warehouse query (`avg_per_validator_apy = 10.07%`, formula result = 26M%+).
- The staging inspector initially claimed the `(date, status)` unique test was absent from `int_consensus_validators_status_daily` schema.yml; marts-1 found it at lines 171-176. The finding was corrected in the final verdict without affecting any other conclusion.
- Challenges were issued on the `staked_daily` `/32` finding (could be intentional design) and the `INNER JOIN` risk in `fct_consensus_info_latest` (latent, not currently visible in data). Both were rebutted with data evidence (`334,876` vs `10.7M GNO`; all 9 labels currently present but mechanism confirmed).
- The `api: tag` space-after-colon typo was verified directly from the source file by grep, not inferred.
- No unresolved challenges remain.
