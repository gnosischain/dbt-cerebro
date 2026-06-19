# Model review: execution/safe

**Convergence:** converged in 1 round — inspector and context reports were mutually consistent and complementary with no open disagreements; the final verdict independently surfaced one additional critical finding (v1.4.1 ABI indexed-flag bug) not caught by the inspector, which was folded in.

---

## Scope and inventory

The `execution/safe` sector catalogs every Gnosis Safe smart-account proxy deployed on Gnosis Chain, tracks the full lifecycle of each Safe's owner set and module/guard state, and exposes that data as REST API endpoints plus a semantic-layer pseudonym bridge. Safe transactions (ExecutionSuccess / ExecutionFailure) are not yet modeled; that family is documented as planned.

| Layer | Count | Purpose |
|---|---|---|
| Intermediate (`intermediate/`) | 6 | Safe catalog, owner events, current owner state, module/guard events, ABI registry |
| Marts — fact/helper (`marts/fct_*`) | 2 | Account-portfolio Safe summary, pseudonym bridge for semantic layer |
| Marts — API views (`marts/api_*`) | 3 | REST tier-2 endpoints: safe details, account safes, current owners |
| **Total** | **11** | |

Both `intermediate/schema.yml` and `marts/schema.yml` are present. A CI guard (`check_api_tags.py`) enforces `api:` / `granularity:` / `window:` / `tier:` / column `data_type` conventions; the mart endpoints currently violate several of these rules.

---

## Business context

The sector answers four classes of questions and serves three consumption channels.

**Business questions:**
1. Safe inventory — how many Safes exist on Gnosis Chain, which version, when were they deployed? Used for ecosystem health reporting and as input to every higher-level sector.
2. Current ownership state — who owns a given Safe right now, which Safes does an address own, what is the current signer threshold? Powers the Account Portfolio, address search, and account profile in the cerebro-api.
3. Module / guard topology — which modules are currently enabled on a Safe? Foundational for the Gnosis Pay sector (GP module discovery), Gnosis App sector (Circles InvitationModule heuristics), and the Zodiac sector.
4. Cross-sector user identity — which on-chain activity keyed to a Safe address belongs to an EOA, and where does that EOA appear in other sectors? Answered by `fct_execution_safe_owner_pseudonyms`, bridging Safe-keyed and EOA-keyed sectors in the semantic layer.

**Consumption channels:**
- cerebro-api (REST): three tier-2 API views — `api_execution_safe_details_latest` (safe summary card, filter by `safe_address`), `api_execution_account_safes_latest` (reverse lookup by `owner_address`), `api_execution_safes_current_owners` (bidirectional filter).
- MCP semantic layer: two semantic models — `execution_safes_current_owners` (candidate quality) and `execution_safe_owner_pseudonyms` (approved quality) — with four published metrics.
- Upstream dependency for: gpay, gnosis_app, zodiac, accounts sectors, and every cross-sector user-pseudonym relationship that crosses the Safe/EOA boundary.

**Canonical definitions:**

- `safe proxy`: a minimal proxy contract that delegatecalls all logic into a singleton (mastercopy). Identified in `execution.traces` by `action_call_type = 'delegatecall'` to a known singleton address with a matching setup selector. Source of truth: `seeds/safe_singletons.csv`.
- `creation_version`: the Safe contract version the proxy was set up against at deployment, derived from the singleton address match. Values: 0.1.0, 1.0.0, 1.1.0, 1.1.1, 1.1.1Circles, 1.2.0, 1.3.0, 1.3.0L2, 1.4.1, 1.4.1L2.
- `is_l2`: `UInt8` flag (1 = L2 singleton that emits additional per-transaction events). Affects singletons 0x3e5c63644e..., 0xfb1bffc9d7..., 0x29fcb43b46....
- `event_kind` (owner events): one of `safe_setup` (initial owner set, one row per owner), `added_owner`, `removed_owner`, `changed_threshold`.
- `current owner`: a `(safe_address, owner)` pair whose latest event by `(block_number, log_index)` is `safe_setup` or `added_owner`. Removed-owner rows are dropped in `int_execution_safes_current_owners`.
- `became_owner_at`: timestamp of the most recent event that added or set up this owner. A re-add after removal resets to the re-add time, not the original setup time.
- `current_threshold`: the required signer count, taken from the latest `safe_setup` or `changed_threshold` event per Safe, denormalized onto every owner row.
- `event_kind` (module events): one of `enabled_module`, `disabled_module`, `changed_guard`, `changed_module_guard`; `target_address` carries the module/guard address.
- `safe_user_pseudonym` / `owner_user_pseudonym`: salted `pseudonymize_address()` hashes used to join Safe-keyed and EOA-keyed sector mounts without exposing raw addresses. Exposed via `fct_execution_safe_owner_pseudonyms`.

**Contract context:** All 12 singleton addresses are verified in `seeds/safe_singletons.csv` and in `cerebro-docs/site/protocols/safe/index.html.md`. Three setup selectors are hardcoded: `0x0ec78d9e` (v0.1.0 only), `0xa97ab18a` (v1.0.0 only), `0xb63e800d` (v1.1.0 and later). GP Safes run on v1.3.0L2 (singleton `0x3e5c63644e...`).

---

## Implementation assessment

### Critical

**v1.4.1 / v1.4.1L2 AddedOwner and RemovedOwner have `indexed:false` in `seeds/event_signatures.csv` (should be `indexed:true`)**

Verified directly in the seed: singletons `0x41675c099f32341bf84bfc5382af534df5c7461a` (v1.4.1) and `0x29fcb43b46531bca003ddc8fcb67ffe91900c762` (v1.4.1L2) both carry `indexed:false` on the `owner` parameter for AddedOwner and RemovedOwner. The actual Solidity ABI has these parameters indexed. With the wrong flag the decoder mislocates the value and silently produces NULL or garbage for `owner` in `int_execution_safes_owner_events.owner_delta_rows`. These rows then drop out of `int_execution_safes_current_owners` at the `WHERE owner IS NOT NULL` guard and never reach the Account Portfolio. Estimated affected events: ~107k. GP Safes (v1.3.0L2) are unaffected. The fix is four rows in `seeds/event_signatures.csv`. This finding was not surfaced by the inspector; it was independently identified during final verdict synthesis.

Affected: `seeds/event_signatures.csv`, `models/execution/safe/intermediate/int_execution_safes_owner_events.sql`

**`int_execution_safes` served to API endpoints without FINAL — 8 duplicate Safes fan out in responses**

The table uses `ReplacingMergeTree(order_by=safe_address)` but no downstream query applies `FINAL`. Data confirms: `count() = 672,271` vs `uniqExact(safe_address) = 672,263` — 8 safe_address values with 2 rows each (example: `0xaf94a1179403645ec79b104775408e5d710fe735` has rows from 2025-11-26 and 2026-02-05, both v1.4.1L2). `api_execution_safe_details_latest` LEFT JOINs `int_execution_safes` on `safe_address`; duplicate deployment rows fan out the summary card. `fct_execution_account_safes_latest` and `contracts_safe_registry` inherit the same exposure.

Affected: `models/execution/safe/intermediate/int_execution_safes.sql`, `models/execution/safe/marts/api_execution_safe_details_latest.sql`, `models/execution/safe/marts/fct_execution_account_safes_latest.sql`

### High

**`int_execution_safes_module_events_v2` is a dead artifact still tagged production**

Schema.yml describes it as "Temporary; to be folded into the original once validated." Data shows 645 rows with `max block_timestamp = 2022-08-31`, compared to the live original at 553,373 rows with max `2026-06-08`. The v2 model has not been populated in roughly four years yet is tagged `production`, documented in schema.yml, and materialized as a real table — creating ongoing ambiguity about which module-events model is authoritative.

Affected: `models/execution/safe/intermediate/int_execution_safes_module_events_v2.sql`, `models/execution/safe/intermediate/schema.yml`

**`contracts_safe_registry`: ReplacingMergeTree table, no partition, no FINAL — feeds the `decode_logs` join**

`contracts_safe_registry` is materialized as a `table` using ReplacingMergeTree but has no partition and no FINAL guard on reads. `decode_logs` resolves the SafeProxy ABI by joining this table; if duplicates accumulate between merge cycles, the join can match multiple ABI rows per Safe and fan out decoded events into both `int_execution_safes_owner_events` and `int_execution_safes_module_events`.

Affected: `models/execution/safe/intermediate/contracts_safe_registry.sql`

**16.7% of current owner rows have NULL `current_threshold` — two distinct causes**

`127,722 / 766,897` rows in `int_execution_safes_current_owners` and `fct_execution_account_safes_latest` have `current_threshold IS NULL`, silently preventing the "M of N signatures" display in the Account Portfolio for ~17% of Safe/owner pairs. Two causes are now identified: (a) pre-v1.1.0 Safes (v0.1.0, v1.0.0) never emit SafeSetup or ChangedThreshold — expected behavior, a known floor; and (b) the v1.4.1 indexed-flag bug (see Critical section above) silently drops owner-delta and threshold events for v1.4.1 Safes — a fixable defect. Consumers cannot distinguish the two categories from the data today.

Affected: `models/execution/safe/intermediate/int_execution_safes_current_owners.sql`, `models/execution/safe/marts/fct_execution_account_safes_latest.sql`

### Medium

**API marts missing column `data_type` and `window:` tags — CI `check_api_tags.py` will fail**

In `marts/schema.yml`, only `as_of_date` columns carry `data_type`; `became_owner_at`, `current_threshold`, `current_owner_count`, `creation_version`, `is_l2`, `deployment_timestamp`, `deployment_tx_hash`, and several others are untyped across `api_execution_safes_current_owners`, `api_execution_safe_details_latest`, and `api_execution_account_safes_latest`. The endpoints also lack `window:` tags. Both violations are enforced by the CI guard and ship untyped contracts to API consumers.

Affected: `models/execution/safe/marts/schema.yml`

**`unique_key` on append-strategy `int_execution_safes_owner_events` is a no-op under dbt-clickhouse**

The model sets `incremental_strategy='append'` alongside `unique_key='(transaction_hash, log_index, owner)'`. Under the append strategy dbt-clickhouse never enforces `unique_key`; dedup relies solely on `ReplacingMergeTree` `order_by` and eventual background merge. The grain is unique by construction today (SafeSetup fan-out produces at most one row per `(tx_hash, log_index, owner)` tuple), but the constraint offers no query-time protection against re-processing artifacts.

Affected: `models/execution/safe/intermediate/int_execution_safes_owner_events.sql`

**No grain/uniqueness tests on the pseudonym bridge or fact marts**

`fct_execution_safe_owner_pseudonyms` declares `tests: []` at model level with no uniqueness test on its stated grain `(safe_user_pseudonym, owner_user_pseudonym)`. `fct_execution_account_safes_latest` likewise has no uniqueness test beyond `not_null`. Any `pseudonymize_address` collision or owner-churn duplicate would be invisible to CI and propagate into the approved-tier semantic metrics.

Affected: `models/execution/safe/marts/fct_execution_safe_owner_pseudonyms.sql`, `models/execution/safe/marts/schema.yml`

### Low

**`join_use_nulls` not set on LEFT JOINs expecting NULL on unmatched rows**

`fct_execution_account_safes_latest` and `api_execution_safe_details_latest` LEFT JOIN to owners and `int_execution_safes` without a `join_use_nulls` pre/post hook. ClickHouse may return default values (`0` / `''`) instead of NULL for unmatched joined columns, masking genuine misses (e.g. `current_threshold`). Project convention (`feedback_clickhouse_left_join_nulls.md`) prescribes `join_use_nulls` hooks over `coalesce`/`nullIf` workarounds.

Affected: `models/execution/safe/marts/fct_execution_account_safes_latest.sql`, `models/execution/safe/marts/api_execution_safe_details_latest.sql`

**`int_execution_safes_current_owners`: heavy resource constraints on a full-table rebuild**

The model forces `max_threads=1`, `max_memory_usage=2GB`, and external group-by at 20MB — heavier constraints than the incremental owner events model it reads from. The schema.yml comments this as "cheap to fully rebuild every run," which is inconsistent with the constraint level. Likely a prior OOM mitigation that was never revisited.

Affected: `models/execution/safe/intermediate/int_execution_safes_current_owners.sql`

---

## Business-logic assessment

### High

**Singleton-upgrade blind spot: `abi_source_address` frozen at deployment singleton**

`contracts_safe_registry.abi_source_address` is set permanently to the creation singleton and never updated after a `changeMasterCopy()` call. Events emitted post-upgrade decode against the stale ABI, silently yielding NULL decoded params for owner, threshold, and module fields. Context data indicates approximately 3,024 Circles Safes (v1.1.1Circles upgraded, likely to v1.3.0) and approximately 2,000 v1.3.0L2 Safes had upgraded as of April 2025. This biases current ownership, threshold, and module state for several thousand Safes.

Affected: `models/execution/safe/intermediate/contracts_safe_registry.sql`, `models/execution/safe/intermediate/int_execution_safes_owner_events.sql`, `models/execution/safe/intermediate/int_execution_safes_module_events.sql`

### Medium

**Privacy-tier review needed: pseudonym bridge tagged `mixpanel` with no `privacy_tier` / `expose_to_mcp` flag**

`fct_execution_safe_owner_pseudonyms` is tagged `mixpanel` and feeds approved-tier semantic metrics, yet carries no `privacy_tier` tag or `expose_to_mcp` gating of the kind used by `gnosis_app` and `gpay` identity models. Although addresses are pseudonymized (salted hash), shipping the owner-to-Safe linkage to Mixpanel may warrant the same governance posture as other identity bridges.

Affected: `models/execution/safe/marts/fct_execution_safe_owner_pseudonyms.sql`, `models/execution/safe/marts/schema.yml`

### Low

**`int_execution_safes` meta `authoritative:false` — schema not treated as system of record**

The foundational Safe catalog used by every downstream sector is marked `authoritative:false`, excluding it from schema-governance enforcement. Given that gpay, gnosis_app, zodiac, and accounts all depend on this table as their upstream source of Safe truth, confirm whether this is intentional or an oversight.

Affected: `models/execution/safe/intermediate/int_execution_safes.sql`

**All three API views are tier-2 despite the sector being a foundational catalog**

`api_execution_safe_details_latest`, `api_execution_account_safes_latest`, and `api_execution_safes_current_owners` are all tagged `tier2`. The underlying catalog (`int_execution_safes`) underpins gpay, gnosis_app, zodiac, and accounts. Confirm whether tier-2 is the intended external-exposure classification.

Affected: `models/execution/safe/marts/api_execution_safe_details_latest.sql`

---

## Data findings

All counts from live warehouse queries run during review (data as of 2026-06-11 review date).

| Model | Metric | Value |
|---|---|---|
| `int_execution_safes` | Total rows (without FINAL) | 672,271 |
| `int_execution_safes` | Distinct `safe_address` | 672,263 |
| `int_execution_safes` | Duplicate `safe_address` rows | 8 (confirmed fan-out risk) |
| `int_execution_safes` | Max `block_timestamp` | 2026-06-08T21:33:15 UTC (~3 days lag) |
| `int_execution_safes_owner_events` | Total rows | 1,246,094 |
| `int_execution_safes_owner_events` | Distinct grain `(safe_address, tx_hash, log_index)` | 1,246,094 (grain holds) |
| `int_execution_safes_owner_events` | Distinct `(safe_address, event_kind, log_index)` | 1,179,048 (gap of 67,046 expected for SafeSetup fan-out) |
| `int_execution_safes_current_owners` | Total rows | 766,897 |
| `int_execution_safes_current_owners` | Rows with NULL `current_threshold` | 127,722 (16.7%) |
| `int_execution_safes_module_events` | Total rows | 553,373 |
| `int_execution_safes_module_events` | Max `block_timestamp` | 2026-06-08 |
| `int_execution_safes_module_events_v2` | Total rows | 645 |
| `int_execution_safes_module_events_v2` | Max `block_timestamp` | 2022-08-31 (dead) |

The 3-day data lag on `int_execution_safes` is within typical dbt scheduling lag and should be tracked against SLA. The 67k SafeSetup fan-out gap in the owner events grain is expected behavior (multiple owners per SafeSetup share one `log_index`) but confirms the schema test grain of `(safe_address, block_timestamp, log_index)` is not unique per row; the test passes only because `block_timestamp` is included.

---

## Pros / Cons

**Pros**

- Core identification logic is robust: delegatecall filter + singleton seed + setup-selector pre-filter reliably enumerates every Safe proxy across v0.1.0 through v1.4.1, including the Circles 1.1.1 fork and all L2 variants.
- Foundational catalog for gpay, gnosis_app, zodiac, and accounts — single source of Safe truth, high leverage.
- Data is current (max 2026-06-08, ~3 days lag, within SLA).
- Backfill orchestration (3-month batches, `apply_monthly_incremental_filter`) is correctly scoped to stay under the ClickHouse Cloud 100-partition-per-insert limit.
- Canonical definitions (creation_version, current owner, became_owner_at, event_kind) are clear and consistent across schema.yml, seed, and docs.
- The pseudonym bridge is thoughtfully designed with documented coverage-cardinality guidance for downstream analysts.
- Approved-tier semantic metrics on the pseudonym bridge are well-specified with question synonyms.
- GP Safes (v1.3.0L2) — the most heavily consumed path — are unaffected by the v1.4.1 ABI indexed-flag bug.

**Cons**

- v1.4.1 AddedOwner/RemovedOwner decode silently NULLs `owner` for ~107k events, causing those owners to vanish from the Account Portfolio on v1.4.1 Safes.
- Three `ReplacingMergeTree` models (`int_execution_safes`, `int_execution_safes_current_owners`, `contracts_safe_registry`) are served to APIs without FINAL; 8 confirmed duplicate Safes can fan out in served responses.
- 16.7% of owner rows have NULL `current_threshold` — consumers cannot render "M of N" for a sixth of Safes, and the two root causes (pre-v1.1.0 expected floor vs v1.4.1 ABI defect) are not distinguished in the data.
- A dead validation artifact (`int_execution_safes_module_events_v2`, stale since 2022-08) is still tagged production and materialized.
- API marts lack column `data_type` and `window:` tags, failing the CI check_api_tags guard and shipping untyped contracts to consumers.
- Singleton-upgrade blind spot: `abi_source_address` frozen at deployment singleton, silently misrouting ABI decodes for ~5k upgraded Safes.
- No grain/uniqueness tests on the pseudonym bridge or fact marts; dedup churn would be invisible to CI and propagate into approved-tier semantic metrics.
- `unique_key` declared on an append-strategy model is a dbt-clickhouse no-op — false sense of dedup protection.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P1 | Fix 4 seed rows: set `indexed:true` on `owner` for AddedOwner and RemovedOwner on singletons `0x41675c099f...` (v1.4.1) and `0x29fcb43b46...` (v1.4.1L2) in `seeds/event_signatures.csv`; rebuild owner events and quantify recovered v1.4.1 owner/threshold rows | `seeds/event_signatures.csv`, `int_execution_safes_owner_events.sql` |
| P1 | Add FINAL (or a safe_address dedup boundary) wherever `int_execution_safes` and `contracts_safe_registry` are read by served marts — eliminates the 8 duplicate-Safe fan-out in API responses | `api_execution_safe_details_latest.sql`, `fct_execution_account_safes_latest.sql`, `contracts_safe_registry.sql` |
| P2 | Separate the NULL `current_threshold` population into (a) pre-v1.1.0 expected floor and (b) v1.4.1 ABI-bug rows; document (a) in schema.yml; confirm the seed fix from P1 clears (b) | `int_execution_safes_current_owners.sql`, `fct_execution_account_safes_latest.sql` |
| P2 | Deprecate and drop `int_execution_safes_module_events_v2` (or fold `event_name_filter` into the original and remove v2); remove its production tag and schema.yml entry | `int_execution_safes_module_events_v2.sql`, `intermediate/schema.yml` |
| P2 | Add `data_type` and `window:` tags to all columns in the three api_ endpoint definitions in `marts/schema.yml` to pass `check_api_tags.py` | `marts/schema.yml` |
| P3 | Add uniqueness/grain tests on `fct_execution_safe_owner_pseudonyms` `(safe_user_pseudonym, owner_user_pseudonym)` and on `fct_execution_account_safes_latest` `(owner_address, safe_address)` | `marts/schema.yml` |
| P3 | Confirm and explicitly tag the privacy posture of the mixpanel-tagged pseudonym bridge (`privacy_tier` / `expose_to_mcp`) consistent with gnosis_app/gpay identity models | `fct_execution_safe_owner_pseudonyms.sql`, `marts/schema.yml` |
| P3 | Add `join_use_nulls` pre/post hooks to the LEFT JOINs in `fct_execution_account_safes_latest` and `api_execution_safe_details_latest` so unmatched `current_threshold` / `owner_count` surface as NULL rather than `0` / `''` | `fct_execution_account_safes_latest.sql`, `api_execution_safe_details_latest.sql` |
| P4 | Scope a time-windowed singleton-history fix for the `changeMasterCopy` ABI-staleness issue affecting ~5k upgraded Safes, or document the affected cohort explicitly in schema.yml | `contracts_safe_registry.sql`, `intermediate/schema.yml` |
| P4 | Confirm whether `int_execution_safes` `authoritative:false` and the sector-wide tier-2 classification are intentional given the catalog's centrality as an upstream dependency for five other sectors | `int_execution_safes.sql`, `marts/schema.yml` |

---

## Open disagreements

None. The review converged in 1 round.

---

## Review log

| Round | Agent | Challenge / finding | Resolution |
|---|---|---|---|
| 1 | Inspector | Raised 10 implementation findings and 4 data findings including ReplacingMergeTree-without-FINAL, dead v2 artifact, NULL threshold rate, and api-tag gaps | Confirmed and carried forward in final verdict |
| 1 | Context | Provided canonical definitions, singleton registry, semantic coverage map, and known caveats including v1.4.1 indexed-flag caveat (documented in cerebro-docs) | Confirmed and carried forward in final verdict |
| 1 | Verdict | Independently identified the v1.4.1 AddedOwner/RemovedOwner `indexed:false` bug in `seeds/event_signatures.csv` not surfaced by inspector; promoted to Critical; folded into NULL threshold root-cause analysis | No challenge issued — inspector coverage gap resolved by verdict synthesis |
