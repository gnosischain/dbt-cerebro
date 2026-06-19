# Model review: execution/state

**Convergence:** converged in 1 round — Inspector and Context reports are mutually consistent; all load-bearing findings confirmed in warehouse with no material disagreements.

---

## Scope and inventory

| Layer | Model | Type |
|---|---|---|
| Staging | `stg_execution__storage_diffs` | View |
| Intermediate | `int_execution_state_size_full_diff_daily` | Incremental (RMT, insert_overwrite, monthly partition) |
| Intermediate (disabled) | `int_execution_state_size_address_daily.sqlxxx` | Dead code |
| Intermediate (disabled) | `int_execution_state_size_diff_address_daily.sqlxxx` | Dead code |
| Fact | `fct_execution_state_full_size_daily` | View (cumulative SUM OVER) |
| Mart / API | `api_execution_state_full_size_daily` | View (bytes -> GB) |

Six models total; four active, two disabled via `.sqlxxx` extension. The pipeline is a thin straight-line transform: staging view -> incremental aggregate -> cumulative fact view -> GB API view. One public tier1 endpoint: `/v1/execution/state_size/daily`.

---

## Business context

This unit answers: "How large is the Gnosis Chain execution-layer state database, and how fast is it growing?" It is an infrastructure/operations metric for node operators, the Gnosis DevOps team, and external capacity-planning consumers. It is not a product or user-facing metric — no Circles, Gnosis Pay, or Safe data is involved.

**Canonical definitions:**

- **storage_diff (source):** One row per EVM storage-slot mutation in a transaction. Fields: contract address, 32-byte hex slot key, `from_value`, `to_value`, `block_number`, `transaction_index`, `block_timestamp`. Source table: `execution.storage_diffs` (cryo-indexer, ReplacingMergeTree, ORDER BY `(block_number, transaction_index, address, slot)`, partitioned monthly).
- **bytes_diff (intermediate):** Intended to be the net daily change in chain state size. Each non-zero storage slot occupies exactly 32 bytes; zeroing a slot reclaims 32 bytes. The correct formula counts only zero-to-nonzero transitions (+32) and nonzero-to-zero transitions (-32). The model currently uses `IF(to_value != zeros, +32, -32)` without conditioning on `from_value` — this is the dominant bug (see Business-logic assessment).
- **bytes (fact):** Cumulative running sum of `bytes_diff` from genesis (2018-10-08) to each date, via `SUM(bytes_diff) OVER (ORDER BY date ASC)`.
- **value (API):** `bytes / 1e9` (gigabytes), for human-readability.
- **State size:** Total storage footprint of all non-zero contract storage slots on Gnosis Chain as of a given date. This is a proxy for EVM state database size; it excludes transaction history, block headers, code storage, and Merkle trie node overhead.

No protocol contracts or seeds are hardcoded. The unit reads all contracts uniformly from `execution.storage_diffs`.

Three semantic models exist at `quality_tier: candidate` in `semantic/authoring/execution/state/semantic_models.yml` (auto-generated, unreviewed).

---

## Implementation assessment

**HIGH — unique test on `transaction_hash` contradicts slot-level grain**

`models/execution/state/staging/schema.yml` declares a `unique` test on `transaction_hash`. `storage_diffs` is one row per `(transaction_hash, address, slot)` mutation; a single transaction touching N slots emits N rows. The test is either perpetually failing or silently suppressed and provides false confidence. The correct uniqueness check is on the composite grain `(transaction_hash, address, slot)`.

**HIGH — `bytes_diff` type drift: schema.yml documents UInt64, actual is Int64**

`models/execution/state/intermediate/schema.yml` documents `bytes_diff` as `UInt64`. The expression `IF(..., 32, -32)` materialises as signed `Int64` in ClickHouse; negative daily values already exist in the table (confirmed via `describe_table`). A downstream consumer performing an unsigned cast would underflow negatives to huge positive values, corrupting the cumulative sum. The type must be corrected to `Int64`.

**MEDIUM — staging schema.yml documents absent columns and a wrong column type**

`stg_execution__storage_diffs` selects 8 columns and does not pass through `chain_id` or `insert_version`. Both are documented in `models/execution/state/staging/schema.yml`, making the docs misleading and deduplication via `insert_version` unavailable downstream. Additionally, `slot` is documented as `UInt64` but is a `String` (32-byte hex key) in both the source definition (`execution_sources.yml`) and the view output. Affected files: `models/execution/state/staging/schema.yml`, `models/execution/state/staging/stg_execution__storage_diffs.sql`.

**MEDIUM — monthly partition approaching CH Cloud 100-partition full-rebuild barrier**

`int_execution_state_size_full_diff_daily` partitions by `toStartOfMonth`. The table spans 2018-10 to 2026-01, already 88 months. Approximately 12 months of headroom remain before a full `--full-refresh` insert hits the CH Cloud code-252 limit (100 partitions per insert). Project convention for wide-history tables is `toStartOfYear`. Switch before the next rebuild cycle. See `models/execution/state/intermediate/int_execution_state_size_full_diff_daily.sql`.

**MEDIUM — disabled .sqlxxx address-level models are dead code with broken ClickHouse SQL**

`int_execution_state_size_address_daily.sqlxxx` and `int_execution_state_size_diff_address_daily.sqlxxx` use `delete+insert` (off-convention; project standard is `insert_overwrite`) and contain a correlated scalar subquery inside a window function (`+ (SELECT bytes FROM last_partition_value WHERE address = t1.address)`) which is unsupported in ClickHouse and would produce NULLs for new addresses. Neither has schema tests or semantic registration. These should be deleted or fully rewritten before any re-enable.

**LOW — fct cumulative view reads ReplacingMergeTree without FINAL**

`fct_execution_state_full_size_daily` performs a window `SUM` over `int_execution_state_size_full_diff_daily` (an RMT table) without `FINAL`. During background merges, transient duplicate rows for a date can inflate the cumulative. Exposure is brief but deterministic correctness requires `FINAL` or a materialized running total. See `models/execution/state/marts/fct_execution_state_full_size_daily.sql`.

**LOW — cumulative window silently bridges missing source days**

27 missing days (20 contiguous from 2025-12-17 to 2026-01-05) cause the `SUM OVER (ORDER BY date)` to jump on the next available day, which is indistinguishable from legitimate growth when viewed as a time series. A date-spine gap-fill or explicit null-marking would surface the discontinuities. Affected: `models/execution/state/intermediate/int_execution_state_size_full_diff_daily.sql`, `models/execution/state/marts/fct_execution_state_full_size_daily.sql`.

---

## Business-logic assessment

**CRITICAL — bytes_diff counts slot overwrites as new allocations, overstating cumulative state size ~2.5x**

`int_execution_state_size_full_diff_daily` uses `IF(to_value != zeros_64, +32, -32)`. A slot overwrite (both `from_value != 0` and `to_value != 0`) increments `bytes_diff` by +32 even though no new slot is allocated and no existing slot is freed. Net state size only changes when a zero slot becomes non-zero (+32) or a non-zero slot is zeroed (-32).

Verified in warehouse on 2026-01-20: 2,936,125 total rows, of which 1,701,977 (58%) are overwrites. Current `bytes_diff` = 90,484,384 bytes vs corrected = 36,021,120 bytes — a 2.51x overcount. The tier1 API endpoint `/v1/execution/state_size/daily` therefore serves a cumulative figure of approximately 70.7 GB when the corrected figure is approximately 28 GB.

Secondary precision consideration: 1.7M of 2.9M daily mutations re-touch the same `(address, slot)` within a single day. For full precision the model should resolve the end-of-day net value per `(address, slot)` via `argMax(to_value, (block_number, transaction_index))` before netting transitions. This is a refinement; the overwrite fix is the dominant correction.

Fix: condition on `from_value` — `CASE WHEN from_value = zeros_64 AND to_value != zeros_64 THEN 32 WHEN from_value != zeros_64 AND to_value = zeros_64 THEN -32 ELSE 0 END`. Full-refresh the int table and validate the corrected cumulative before the API resumes serving.

Affected: `models/execution/state/intermediate/int_execution_state_size_full_diff_daily.sql`, `models/execution/state/marts/fct_execution_state_full_size_daily.sql`, `models/execution/state/marts/api_execution_state_full_size_daily.sql`.

**HIGH — pipeline 132 days stale at the source**

`int`, `fct`, `api`, and the staging view all top out at 2026-01-30; today is 2026-06-11. The gap is confirmed at the staging view level, so the cause is upstream source ingestion (cryo-indexer `storage_diffs` feed), not the dbt incremental window. The tier1 API surfaces a 4-month-old number with no consumer-visible failure signal. Root cause (planned deprecation vs. crawler outage) is unknown. Requires an explicit source freshness alert so a gap of this duration cannot surface silently on a tier1 endpoint.

**MEDIUM — semantic layer sums a cumulative column and double-registers the surface**

The `fct_execution_state_full_size_daily` and `execution_state_full_size_daily` semantic models expose `SUM` over the already-cumulative `bytes`/`value` columns. Summing a running total across rows is semantically meaningless; the correct aggregation for a snapshot-at-date cumulative series is `MAX` or `LAST`. Additionally, both the fact model and the API mart are registered simultaneously in the semantic layer, creating redundancy — only the API surface plus the additive `bytes_diff` measure should be exposed. All three models carry `quality_tier: candidate` (unreviewed).

**LOW — GB figure is a slot-value-only proxy presented without an estimate caveat**

`value = bytes / 1e9` assumes 32 bytes per non-zero slot and excludes slot-key and Merkle-trie node overhead, understating real node disk requirements. The API `schema.yml` describes this as a literal size figure without noting it is a proxy. External capacity-planning consumers may misread it as the full node disk footprint. A documentation caveat should be added.

---

## Data findings

Warehouse queries confirmed the following (8 queries executed by Inspector):

| Check | Result |
|---|---|
| Max date in staging view | 2026-01-30 (132 days before audit date) |
| Rows on 2026-01-20 in staging | 2,936,125 |
| Overwrites on 2026-01-20 | 1,701,977 (58%) |
| Current `bytes_diff` (2026-01-20) | 90,484,384 bytes |
| Corrected `bytes_diff` (2026-01-20) | 36,021,120 bytes (2.51x overcount) |
| Reported cumulative (API, ~2026-01-30) | ~70.7 GB |
| Corrected cumulative estimate | ~28 GB |
| Missing days in int table | 27 total; 20 contiguous from 2025-12-17 to 2026-01-05 |
| Unique dates in int table | 2,645 vs 2,672 expected |

The staging view returns zero rows for any date after 2026-01-30, confirming the staleness is at the source ingestion level, not the dbt incremental filter.

---

## Pros / Cons

**Pros**

- Clean, thin, easy-to-reason-about four-model pipeline with a single clear public surface.
- Chain-wide, protocol-agnostic aggregate with no hardcoded addresses or seeds — no scoping or survivorship traps.
- Full history from 2018-10-08 (genesis-adjacent), giving a long cumulative trend useful for node capacity planning.
- Incremental `insert_overwrite` with monthly idempotent partitions follows the project's late-arriving-safe convention.
- Elementary volume/freshness/schema tests wired on the int model; staleness should surface as a warning.
- API endpoint is tier1 and api-tag compliant (`api:state_size`, `granularity:daily`); CI guard passes.
- Answers a genuinely useful infrastructure question (EVM state DB growth) not otherwise served by the platform.

**Cons**

- Headline metric is wrong by ~2.5x: overwrites counted as new allocations; served cumulative ~70.7 GB vs ~28 GB true.
- 132 days stale at the source — consumers see state growth only through January 2026 with no visible failure on the API.
- 27 missing days (20 contiguous from December 2025) silently bridged by the cumulative window, distorting the growth curve.
- Documentation drift: `schema.yml` types (`bytes_diff` UInt64, `slot` UInt64) and phantom columns (`chain_id`, `insert_version`) do not match actual model output.
- Unique test on `transaction_hash` contradicts the slot-level grain; either always-failing or silently suppressed.
- Semantic layer exposes `SUM` over a cumulative column and registers both `fct` and `api` — nonsensical aggregate and redundancy.
- No per-contract attribution: address-level breakdown disabled and left as broken `.sqlxxx` dead code.
- GB figure is a slot-value-only proxy (32B/slot) without trie/key overhead — understates true node disk and undocumented as an estimate.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Fix `bytes_diff` to condition on `from_value`: +32 only for zero->nonzero, -32 only for nonzero->zero, 0 for overwrites. Full-refresh the int table and validate corrected cumulative (~28 GB) before the API resumes serving. | `int_execution_state_size_full_diff_daily.sql` |
| P0 | Investigate and restore source freshness for the cryo-indexer `storage_diffs` feed (stale since 2026-01-30). Add an explicit source freshness alert so a 4-month gap cannot surface silently on a tier1 endpoint. | Source / infra — not a dbt model change |
| P1 | Correct `schema.yml` documentation: `bytes_diff` -> `Int64`; remove `chain_id`/`insert_version` from staging schema (or pass them through the view); `slot` -> `String`. | `staging/schema.yml`, `intermediate/schema.yml` |
| P1 | Remove the `unique` test on `transaction_hash`; if uniqueness is required, test the real grain `(transaction_hash, address, slot)`. | `staging/schema.yml` |
| P1 | For full precision on the corrected `bytes_diff`, resolve end-of-day net value per `(address, slot)` via `argMax` by `(block_number, transaction_index)` before netting transitions (secondary refinement after P0 fix). | `int_execution_state_size_full_diff_daily.sql` |
| P2 | Repartition the int model by `toStartOfYear` to stay clear of the CH Cloud 100-partition full-rebuild barrier before the next `--full-refresh`. | `int_execution_state_size_full_diff_daily.sql` |
| P2 | Fix the semantic layer: change `fct`/`api` measures from `SUM` to `MAX` (or `LAST`) over the cumulative column; drop the redundant `fct` semantic registration; keep only the API surface plus the additive `bytes_diff` measure. | `semantic/authoring/execution/state/semantic_models.yml` |
| P2 | Add a date-spine gap-fill or null-mark for missing source days so the cumulative window does not silently bridge gaps. Add `FINAL` to the `fct` view's RMT read for deterministic correctness. | `fct_execution_state_full_size_daily.sql`, `int_execution_state_size_full_diff_daily.sql` |
| P3 | Delete or fully rewrite the two `.sqlxxx` address-level models — the correlated subquery is invalid in ClickHouse and `delete+insert` is off-convention. Make an explicit roadmap decision on per-contract attribution. | `int_execution_state_size_address_daily.sqlxxx`, `int_execution_state_size_diff_address_daily.sqlxxx` |
| P3 | Add an estimate caveat to the API `schema.yml` clarifying that the GB value is slot-value storage only (32B/slot, excluding trie/key overhead) so external consumers do not read it as literal node disk size. | `marts/api_execution_state_full_size_daily.sql`, `marts/schema.yml` |

---

## Open disagreements

None. The review converged in one round with full agreement between Inspector and Context agents.

---

## Review log

| Round | Agent | Action | Outcome |
|---|---|---|---|
| 1 | Inspector | Full pipeline read + 8 warehouse queries; identified critical bytes_diff overcount, staleness, missing days, type drift, schema drift, unique-test grain mismatch, partition risk, dead .sqlxxx code | All findings confirmed |
| 1 | Context | Established intended purpose, canonical definitions, semantic layer coverage, and caveats | Consistent with Inspector; no contradictions |
| 1 | Analyst | No challenges issued to either agent; findings merged directly into verdict | Converged |
