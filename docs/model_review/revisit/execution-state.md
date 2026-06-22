# Model review (revisit 2026-06-21): execution/state

Re-verification of baseline `docs/model_review/execution-state.md` (dated `2026-06-11`); all 11 cases re-checked over 4 rounds. Headline: 0 resolved, 0 changed status, 11 still CONFIRMED — two severities were downgraded (C04 high->medium, C07 medium->low) and one sub-claim within C05 was retired (dedup not needed), but every defect persists in code today and the Tier1 API still serves a `70.710707424 GB` figure that is both stale (`142` days) and structurally overstated (~`2.2x`).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONSTATE-C01 | P0-17 | `IF(to_value!=0,32,-32)` ignores `from_value` -> overwrites count as +32, ~2.2-2.5x overcount of cumulative state size | critical | CONFIRMED | critical | high | none | 4 |
| EXECUTIONSTATE-C02 | - | Whole pipeline frozen at `2026-01-30`; cause is upstream cryo-indexer source stall; Tier1 API serves a stale number with no effective page | high | CONFIRMED | high | high | none | 4 |
| EXECUTIONSTATE-C03 | - | `unique` test on `transaction_hash` but true grain is `(transaction_hash, address, slot)` | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONSTATE-C04 | - | `bytes_diff` documented `UInt64` but materializes `Int64` with real negatives | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONSTATE-C05 | - | Staging view drops documented `chain_id`/`insert_version`; `slot` doc `UInt64` vs actual `String` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONSTATE-C06 | - | Partitions by `toStartOfMonth` (~88 months) nearing CH Cloud code-252 100-partition full-refresh cap | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONSTATE-C07 | - | Two `.sqlxxx` dead-code files: off-convention `delete+insert` + correlated subquery in window | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONSTATE-C08 | - | `fct` window `SUM` over a ReplacingMergeTree without `FINAL` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONSTATE-C09 | - | 27 missing source days (20 contiguous `2025-12-17`..`2026-01-05`); cumulative jumps across gaps | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONSTATE-C10 | - | Semantic measures `agg:sum` over already-cumulative columns; fct+api both registered; all `candidate` | medium | CONFIRMED | medium | medium | none | 3 |
| EXECUTIONSTATE-C11 | - | `bytes/1e9` (32 B/slot) served as literal GB with no proxy/Merkle caveat | low | CONFIRMED | low | high | none | 3 |

No incident attribution on any case. No NEW cases surfaced during re-verification.

## Delta vs baseline

RESOLVED (0): none. Every defect reproduces in code today.

CHANGED status (0): no case changed CONFIRMED/RESOLVED status. One sub-claim within a still-confirmed case was retired (see below under C05).

STILL CONFIRMED (11):

- **C01 (critical, the highest-impact defect)** — `models/execution/state/intermediate/int_execution_state_size_full_diff_daily.sql` line 19 still uses `IF(to_value!=zeros,32,-32)` with no `from_value` guard. Jan-2026 flow re-measured to the digit: `current=1,960,957,184` vs `corrected=887,981,216` = `2.208x`; overwrites `33,530,499/63,784,382 = 52.57%`. The API still serves exactly `70.710707424 GB`; corrected should be ~`32 GB` (`70.71/2.21`), inside the asserted `28-32 GB` band.
- **C02 (high)** — staleness grew exactly `+10` days (`132` at baseline -> `142` today), proving no new data arrived. Raw `execution.storage_diffs` all-time max `block_timestamp = 2026-01-30`; `int`/`fct`/`api` all max `date = 2026-01-30`, `count = 2645` each. Mechanism corrected in Round 4: source freshness IS configured (inherited `error_after: 48h`) but is FILTER-MASKED — the `block_timestamp > now() - INTERVAL 7 DAY` filter window holds zero rows under the freeze, so `max_loaded_at` is NULL and the check cannot fire.
- **C03 (high)** — `unique` test still on `transaction_hash` alone. On `2026-01-30`: `rows=2,261,305`, `uniq(transaction_hash)=148,808` (`15.2x` violation), `uniq((transaction_hash,address,slot))=2,261,305 == count()` proving the composite triple is the true grain. Re-characterized from "perpetually failing" to "freshness-masked vacuous pass" (last-7-day window `count()=0`).
- **C04 (high -> medium)** — `bytes_diff` documented `UInt64` (`intermediate/schema.yml` line 17) but materializes `Int64`; `min(bytes_diff)=-35,868,640`. Downgraded because there is NO active corruption (fct cumulative `min=5,088`, zero negative rows), NO `contract: enforced: true` anywhere, and no unsigned cast path. Dormant doc drift.
- **C05 (medium)** — staging view emits 8 columns, omits documented `chain_id` and `insert_version`; `slot` documented `UInt64` but materializes `Nullable(String)` at both source (`execution_sources.yml` line 484-486) and view output (`describe_table`). The dedup sub-claim was retired (see CHANGED line below).
- **C06 (medium)** — `partition_by='toStartOfMonth(date)'` with `88` distinct monthly partitions (`2018-10` to `2026-01`). Code-252 (>100 partitions/insert) bites only on `--full-refresh`; ~`12` months headroom. Source frozen so no new months accrue.
- **C07 (medium -> low)** — both `.sqlxxx` files still present (off-convention `delete+insert` + correlated scalar subquery in a window SUM). Downgraded because `dbt_project.yml` `model-paths: ["models"]` only globs `.sql`, so they are invisible to dbt parse; zero live refs, zero materializations. Latent only on a deliberate rename.
- **C08 (low)** — `fct` view reads the int RMT with a window `SUM` and no `FINAL`. No current duplicate parts (`no-FINAL == FINAL == 70,710,707,424`, `cnt == cnt_final == 2645`); worst-case transient inflation `211,602,464/70,710,707,424 = 0.299%`, self-healing.
- **C09 (low)** — date-spine diff confirms exactly `27` missing days (`2,645` present / `2,672` span), 20 contiguous (`2025-12-17`..`2026-01-05`) + 7 scattered early. Largest gap step `9,138,400 bytes = 0.0133%` of cumulative — sub-1%, growth-indistinguishable.
- **C10 (medium)** — `semantic/authoring/execution/state/semantic_models.yml`: api `value` and fct `bytes` measures `agg:sum` over already-cumulative columns; Jan-2026 sum `1,743,103,777,760` (~`1.74 TB`) vs correct month-end MAX `70,710,707,424` = `24.6x` inflation. Both fct and api registered (`api.value = fct.bytes/1e9`, redundant); all 3 models `quality_tier: candidate`. Candidate-tier gating lives in the EXTERNAL MCP server, not this repo, so inflation is reachable absent a provable serve-gate.
- **C11 (low)** — `api_execution_state_full_size_daily.sql` line 10 `bytes/POWER(10,9)`; `marts/schema.yml` line 16 describes `value` as a literal "size of the execution state in gigabytes" with no proxy/Merkle caveat. Slot-key-inclusive floor >=`2x` (>=`141 GB`). Compounds with C01.

CHANGED (1 sub-claim, case stays CONFIRMED):

- **C05 dedup sub-claim retired** — baseline asserted `insert_version` dedup is "unavailable AND needed". Re-measured over the last source week: `count()=19,128,099 == uniqExact((transaction_hash,address,slot))=19,128,099` — zero duplicate triples, so `insert_version`-based dedup is NOT materially required. The phantom-column and slot-type defects stand; only the dedup necessity was downgraded.

NEW (0): none.

UNVERIFIABLE / UNRESOLVED (0 cases; 1 scan-limited sub-claim): the C01 whole-history EXACT corrected GB is the only sub-claim not directly measurable — even pre-2022 corrected-cumulative scans time out at the MCP/OOM limit. The mechanism is reproduced to the digit on Jan-2026 (`2.208x`) and bounded by early-history `1.89x` and recent `2.208x`, landing inside the asserted `2.2-2.5x` band; corrected ~`32 GB` vs `70.710707424 GB` served is sound. This is a genuine warehouse scan limit, not a verifier weakness.

## Evidence appendix

**C01 — overwrite overcount** (`int_execution_state_size_full_diff_daily.sql` line 19 unchanged):
```sql
SELECT
  sum(if(to_value!=zeros,32,-32)) AS current_flow,
  sum(multiIf(from_value=zeros AND to_value!=zeros,32, from_value!=zeros AND to_value=zeros,-32,0)) AS corrected_flow,
  sum(if(from_value!=zeros AND to_value!=zeros,1,0)) AS overwrite_rows,
  count()
FROM execution.storage_diffs
WHERE block_timestamp >= '2026-01-01' AND block_timestamp < '2026-02-01'
-- zeros = repeat('0',64) bare-hex zero sentinel the int model matches
```
Returned: `current=1,960,957,184`; `corrected=887,981,216`; `overwrites=33,530,499/63,784,382=52.57%`; ratio `2.208x`. Pre-2021 cumulative slice: `corrected=627,989,984` vs `current=1,187,173,792` (`1.89x`). API headline at `2026-01-30`: `value = 70.710707424 GB`, fct `bytes = 70,710,707,424`.

**C02 — staleness + freshness mechanism**:
```sql
SELECT toDate(max(block_timestamp)) FROM execution.storage_diffs;                 -- 2026-01-30
SELECT count() FROM execution.storage_diffs WHERE block_timestamp > now() - INTERVAL 7 DAY;  -- 0
SELECT max(date), count() FROM dbt.int/fct/api_execution_state_full_size_daily;   -- 2026-01-30, 2645 each
SELECT dateDiff('day', toDate('2026-01-30'), today());                            -- 142
```
`execution_sources.yml` lines 9-12 define source-level freshness `{warn_after: 26h, error_after: 48h, filter: "block_timestamp > now() - INTERVAL 7 DAY"}`; storage_diffs (lines 468-501) declares only `loaded_at_field` + columns, no per-table override -> inherits `error_after: 48h`. With `count()=0` in the filter window, the check is vacuous. The int `schema.yml` (lines 44-51) registers `elementary.freshness_anomalies` on `date` at `severity: warn` (warn-only, plateau-blind).

**C03 — wrong-grain unique test** (`staging/schema.yml` lines 22-25, `transaction_hash` `unique` with `where = toDate(block_timestamp) >= today()-7`):
```sql
SELECT count() FROM dbt.stg_execution__storage_diffs WHERE toDate(block_timestamp) >= today()-7;  -- 0 (vacuous pass)
-- on 2026-01-30:
SELECT count(), uniqExact(transaction_hash), uniqExact((transaction_hash,address,slot))
FROM dbt.stg_execution__storage_diffs WHERE toDate(block_timestamp)='2026-01-30';
```
Returned (on `2026-01-30`): `count()=2,261,305`; `uniq(transaction_hash)=148,808` (`15.2x`); `uniq(triple)=2,261,305 == count()`.

**C04 — type doc drift**:
```sql
-- describe_table dbt.int_execution_state_size_full_diff_daily -> bytes_diff type Int64
SELECT min(bytes_diff), max(bytes_diff) FROM dbt.int_execution_state_size_full_diff_daily;  -- -35,868,640 / 211,602,464
SELECT min(bytes), max(bytes) FROM dbt.fct_execution_state_full_size_daily;                  -- 5,088 / 70,710,707,424, 0 negatives
```
`intermediate/schema.yml` line 17 documents `data_type: UInt64`. Repo-wide grep: NO `contract: enforced: true` anywhere; api casts via `bytes/POWER(10,9)` (Float64, sign-preserving); semantic measures are plain `agg:sum` (no cast).

**C05 — phantom columns + slot type** (`stg_execution__storage_diffs.sql` lines 11-19, code-only + describe):
- View emits 8 cols: `block_number, transaction_index, CONCAT('0x',transaction_hash), CONCAT('0x',address), slot, from_value, to_value, block_timestamp`; no `chain_id`, no `insert_version`.
- `describe_table dbt.stg_execution__storage_diffs`: `slot` materializes `Nullable(String)`.
- `staging/schema.yml` documents `slot` `UInt64` (line 35), `chain_id` (line 45), `insert_version` (line 53); `transaction_hash`/`address` documented `String` (correct).
- Dedup check (retired sub-claim): `count()=19,128,099 == uniqExact((transaction_hash,address,slot))=19,128,099`.

**C06 — partition cap** (`int` model config lines 3-7, `insert_overwrite` + `toStartOfMonth(date)`):
```sql
SELECT uniqExact(toStartOfMonth(date)), min(toStartOfMonth(date)), max(toStartOfMonth(date))
FROM dbt.int_execution_state_size_full_diff_daily;
```
Returned: `88` distinct monthly partitions, `2018-10-01` through `2026-01-01`. `system.parts` blocked (database not allowed) — count derived from the partition key. A `--full-refresh` would emit 88 partitions vs the 100-partition cap -> ~`12` months headroom.

**C07 — dead code** (code-only + grep + list_tables):
- `ls models/execution/state/intermediate/` -> both `.sqlxxx` present (Aug 19 2025).
- grep across `models/`, `semantic/`, `macros/`, `dbt_project.yml` -> zero live `ref()`/`source()`/test/semantic hits.
- `list_tables dbt LIKE 'int_execution_state_size%'` -> only `int_execution_state_size_full_diff_daily` (2645 rows). No address/diff_address tables.
- `int_execution_state_size_address_daily.sqlxxx` line 4 `incremental_strategy='delete+insert'`; line 47 `+ (SELECT bytes FROM last_partition_value WHERE address = t1.address)` (correlated subquery in window). `dbt_project.yml` line 28 `model-paths: ["models"]` (only `.sql` globbed).

**C08 — RMT without FINAL** (`fct_execution_state_full_size_daily.sql` lines 8-11):
```sql
SELECT count() c, (SELECT count() FROM dbt.int_execution_state_size_full_diff_daily FINAL) cf;  -- 2645 == 2645
SELECT sum(bytes_diff) s, (... FINAL) sf;     -- 70,710,707,424 == 70,710,707,424
SELECT max(bytes_diff) FROM dbt.int_execution_state_size_full_diff_daily;  -- 211,602,464 (0.299% of cumulative)
```

**C09 — gap spine**:
```sql
-- date-spine 2018-10-08..2026-01-30 LEFT JOIN distinct int dates
-- expected=2,672; present=2,645; missing=27 (epoch 20439-20458 contiguous + 7 scattered 2018-2019)
SELECT bytes FROM dbt.fct_... WHERE date='2025-12-16';  -- 68,749,750,240
SELECT bytes FROM dbt.fct_... WHERE date='2026-01-06';  -- 68,758,888,640 (delta 9,138,400 = 0.0133%)
```

**C10 — semantic agg** (`semantic_models.yml`, code + demo SQL):
- 3 semantic models, all `quality_tier: candidate`. api measure `execution_state_full_size_daily__value_value` `agg:sum` over `value` (cumulative); fct `bytes_value` `agg:sum` over `bytes` (cumulative); int `bytes_diff_value` `agg:sum` over per-day `bytes_diff` (legitimate). All metrics advertise `supported_time_grains: [day..year]`.
```sql
SELECT SUM(bytes), MAX(bytes) FROM dbt.fct_... WHERE date >= '2026-01-01';
-- SUM = 1,743,103,777,760 (~1.74 TB) vs MAX = 70,710,707,424 (70.71 GB) = 24.6x
```
`scripts/semantic/build_registry.py` sets `APPROVED_STATUSES={'approved'}`, tags these `semantic_status:'candidate'` but does NOT hard-block; the actual serve-gate is in the external MCP server.

**C11 — proxy caveat** (code-only):
- `api_execution_state_full_size_daily.sql` line 10 `bytes/POWER(10,9) AS value`; `bytes = 32 * net_nonzero_slots`.
- `marts/schema.yml` line 16 describes `value` as "The size of the execution state in gigabytes" — no proxy/estimate/Merkle/slot-key caveat. Slot-key-inclusive floor >=`64 B/slot` -> >=`141.4 GB` vs `70.71 GB` served.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED critical (sample day 2026-01-29 `2.22x`, served `70.71 GB`) -> challenge "compute whole-history cumulative corrected vs served" -> R2 Jan flow `2.208x`, overwrites `52.6%` (whole-history scan timed out) -> challenge "close the loop day-by-day cumulative" -> R3 reproduced Jan to the digit (`887,981,216`/`1,960,957,184`), pre-2021 `1.89x`; whole-history exact GB un-measurable (OOM) -> R4 sufficient, critical held.
- **C02**: R1 CONFIRMED high (`142d` stale, `src_max==int_max==2026-01-30`) -> challenge "rule out dbt-window artifact, bucket by day" -> R2 zero rows for `2026-02+`, buckets only on 01-29/30/31 -> challenge "verify the failure-signal gap" -> R3 claimed (wrongly) no source freshness block -> challenge "you are wrong: storage_diffs INHERITS source-level 48h error_after; it is filter-masked" -> R4 correction accepted and verified against `execution_sources.yml` lines 9-12/468-501; high held.
- **C03**: R1 CONFIRMED high (code-only, `unique` on `transaction_hash`) -> challenge "quantify the violation at runtime" -> R2 `19,128,099` rows / `1,009,744` tx = `18.9x`, triple unique -> challenge "re-characterize mechanism: where-clause makes it a vacuous pass under the freeze" -> R3 confirmed vacuous pass (last-7-day `count()=0`) + grain real (`15.2x` on 2026-01-30); high held.
- **C04**: R1 CONFIRMED high (doc `UInt64`/actual `Int64`, `min=-35,868,640`) -> challenge "check downstream unsigned cast / blast radius" -> R2 fct cumulative non-negative (latent) -> challenge "argue high vs medium given no active corruption/enforcement" -> R3 recommend high->medium (no `enforced:true`, no unsigned cast path); medium accepted and held R4.
- **C05**: R1 CONFIRMED medium (phantom cols + slot type) -> challenge "demonstrate dedup is needed via duplicate-triple count" -> R2 CHANGED: zero duplicate triples -> dedup sub-claim retired, phantom/slot defects stand -> challenge "confirm slot materializes String at view; scope precisely" -> R3 `describe_table` confirms `Nullable(String)`; medium held.
- **C06**: R1 CONFIRMED medium (`toStartOfMonth`, ~88 months) -> challenge "confirm live partition count + headroom" -> R2 `uniqExact(toStartOfMonth)=88` (system.parts blocked), ~12 months headroom -> challenge "confirm trigger is --full-refresh only; reaffirm year-partition constraint" -> R3 confirmed `insert_overwrite` touches 1 month normally, code-252 only on full-refresh; medium held.
- **C07**: R1 CONFIRMED medium (both `.sqlxxx` present, off-convention) -> challenge "prove fully inert: grep + list_tables" -> R2 zero refs, zero materializations -> challenge "argue medium vs low given .sqlxxx invisible to dbt" -> R3 recommend medium->low (pure dead text, latent only on rename); low accepted and held R4.
- **C08**: R1 CONFIRMED low (no FINAL on RMT read) -> challenge "is the risk real: check current dupes + FINAL vs no-FINAL" -> R2 `no-FINAL==FINAL`, `cnt==cnt_final==2645` -> challenge "quantify theoretical blast radius from peak day" -> R3 `0.299%` worst-case, self-healing; low held.
- **C09**: R1 CONFIRMED low (`27` missing, `2,645/2,672`) -> challenge "quantify the cumulative discontinuity in GB and %" -> R2 gap step `9,138,400 bytes = 0.0133%` -> challenge "verify this is the ONLY material gap via date-spine diff" -> R3 exactly 27 missing (20 contiguous + 7 scattered), all steps sub-1%; low held.
- **C10**: R1 CONFIRMED medium (`agg:sum` over cumulative, 3 candidate models) -> challenge "demonstrate inflation per measure at month grain" -> R2 Jan sum `1.74 TB` vs MAX `70.71 GB` = `24.6x` -> challenge "confirm candidate-tier mitigation: does it hard-block serving?" -> R3 repo registry only tags `semantic_status`, hard-block is in external MCP server (unprovable here) -> inflation reachable; medium held.
- **C11**: R1 CONFIRMED low (`bytes/1e9` no caveat) -> challenge "quantify the understatement floor with slot key included" -> R2 floor >=`141 GB` vs `70.71 GB` (>=`2x`) -> challenge "frame the compounding with C01 and the fix ordering" -> R3 number is simultaneously ~`2.2x` over net-slot AND under true disk; documentation-only fix, write caveat AFTER C01; low held.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (KEEP/ESCALATE) | Fix the overwrite overcount: condition the byte delta on `from_value` — `multiIf(from=0 AND to!=0,+32, from!=0 AND to=0,-32, 0)`. This corrects the Tier1 headline from `70.71 GB` to ~`28-32 GB`. Rebuild via the incremental runner (avoid plain whole-month full rebuild). | `int_execution_state_size_full_diff_daily.sql`, `fct_execution_state_full_size_daily.sql`, `api_execution_state_full_size_daily.sql` |
| P1 (KEEP) | Restore upstream `storage_diffs` ingestion (cryo-indexer stalled at `2026-01-30`, now `142d` stale) and make the freshness signal effective: the inherited 48h `error_after` is filter-masked by `block_timestamp > now()-7d`. Add a max-`block_timestamp`-vs-`now()` absolute-age check (not filtered) and raise `elementary.freshness_anomalies` above `severity: warn`. | upstream cryo-indexer `execution.storage_diffs`, `execution_sources.yml`, `int`/`fct`/`api` |
| P1 (KEEP) | Correct the `unique` test grain to the composite `(transaction_hash, address, slot)` (single-column is a freshness-masked vacuous pass that would fail `15.2x` on fresh data). | `staging/schema.yml`, `stg_execution__storage_diffs.sql` |
| P2 (KEEP) | Fix semantic measures: change api `value` and fct `bytes` from `agg:sum` to `MAX`/`LAST` over the cumulative columns (`24.6x` inflation at month grain); deregister the redundant model (`api.value = fct.bytes/1e9`) keeping one; review the three `quality_tier: candidate` registrations. | `semantic/authoring/execution/state/semantic_models.yml` |
| P2 (KEEP) | Correct `bytes_diff` documented type `UInt64` -> `Int64` and note signedness on `fct.bytes` (dormant doc drift, no active corruption — contract-hygiene only). | `intermediate/schema.yml`, `marts/schema.yml` |
| P2 (KEEP) | Fix staging schema drift: drop phantom `chain_id`/`insert_version` from `schema.yml` (or pass them through the view) and correct `slot` documented type `UInt64` -> `String`. | `staging/schema.yml`, `stg_execution__storage_diffs.sql` |
| P3 (KEEP) | Before the next `--full-refresh`, repartition wide history to `toStartOfYear` — but ONLY if the `insert_overwrite` grain is moved to year simultaneously (otherwise a one-month run REPLACE-wipes the whole year; June incident pattern). ~`12` months headroom on the current `toStartOfMonth` 88-partition design. | `int_execution_state_size_full_diff_daily.sql` |
| P3 (KEEP) | Delete the two inert `.sqlxxx` dead-code files (off-convention `delete+insert` + ClickHouse-unsupported correlated subquery in a window) to remove the rename footgun. | `int_execution_state_size_address_daily.sqlxxx`, `int_execution_state_size_diff_address_daily.sqlxxx` |
| P3 (KEEP) | Add `FINAL` to the fct read of the int ReplacingMergeTree (or materialize a running total) for deterministic correctness; transient exposure is `<1%` and self-healing today. | `fct_execution_state_full_size_daily.sql` |
| P3 (KEEP) | Add a date-spine gap-fill / explicit null-marking so the `27` missing-day gaps surface as discontinuities rather than silent growth. | `int_execution_state_size_full_diff_daily.sql`, `fct_execution_state_full_size_daily.sql` |
| P3 (KEEP) | Add a proxy/lower-bound caveat (ideally a rename) noting `value` counts only 32 B per net non-zero slot and excludes slot-key + Merkle-trie overhead — write this AFTER C01 is fixed so it references the corrected ~`28-32 GB` base, not the inflated `70.71 GB`. | `api_execution_state_full_size_daily.sql`, `marts/schema.yml` |

No DROP recommendations — nothing was resolved.
