# Model review: execution/zodiac

**Convergence:** Converged in 1 round — inspector and context reports were mutually consistent; the one apparent number conflict (290,414 vs 153,127 proxy counts) was immediately resolved as two different tables, not a contradiction.

---

## Scope and inventory

| Layer | Model count | Purpose |
|---|---|---|
| `intermediate/` | 4 SQL files + 1 schema.yml | Factory discovery, ABI registry, modifier event log, sub-module latest snapshot |
| `marts/` | 0 | None |
| `semantic/` | 0 | None |

Total: 4 intermediate models, no marts, no API endpoints, no semantic layer coverage. All models are marked `authoritative: false`.

| Model | Materialization | Strategy | Grain |
|---|---|---|---|
| `int_execution_zodiac_module_proxies` | Table (incremental) | append + ReplacingMergeTree | `(proxy_address)` |
| `contracts_zodiac_modules_registry` | Table | full rebuild | `(proxy_address)` |
| `int_execution_zodiac_modifier_module_events` | Table (incremental) | append + ReplacingMergeTree | `(transaction_hash, log_index)` |
| `int_execution_zodiac_modifier_submodules_latest` | Table | full rebuild | `(modifier_address, submodule_address)` |

---

## Business context

The zodiac unit is a chain-wide infrastructure layer for Zodiac Modifier module tracking on Gnosis Chain. It answers three ordered questions:

1. **Factory discovery (primary):** Which addresses are genuine Zodiac module proxies, and which mastercopy do they point at? `int_execution_zodiac_module_proxies` ingests `ModuleProxyCreation` events from the canonical `ModuleProxyFactory` (`0x000000000000addb49795b0f9ba5bc298cdda236`) — the sole source of truth for this. `contracts_zodiac_modules_registry` filters to the four tracked mastercopies and serves as the ABI resolution registry for `decode_logs`.

2. **Chain-wide Modifier sub-module topology (secondary):** Which sub-modules are currently enabled on Zodiac Delay or Roles proxies chain-wide, and which constitute the ERC-1271 exploit surface? `int_execution_zodiac_modifier_module_events` decodes `EnabledModule`/`DisabledModule` events emitted by Modifier proxies (non-indexed module address — distinct from Safe-emitted equivalents). `int_execution_zodiac_modifier_submodules_latest` snapshots the current state.

3. **ABI-resolution registry for chain-wide decoding (supporting):** `contracts_zodiac_modules_registry` provides the ABI lookup for all four covered mastercopy groups across the whole chain, not scoped to GP Safes.

**Tracked mastercopies (all verified against `seeds/event_signatures.csv`):**

- `0x000000000000addb49795b0f9ba5bc298cdda236` — ModuleProxyFactory (canonical, one instance on Gnosis Chain)
- `0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5` — DelayMod_v1 (primary GP mastercopy)
- `0xd54895b1121a2ee3f37b502f507631fa1331bed6` — DelayMod_v1_old1 (older; schema notes it "still custodies funds")
- `0xd62129bf40cd1694b3d9d9847367783a1a4d5cb4` — DelayMod_v1_old2 (older; adds `ChangedGuard` event)
- `0x9646fdad06d3e24444381f44362a3b0eb343d337` — RolesMod_v2

The unit is the upstream dependency for the `execution/gpay` unit: `contracts_gpay_modules_registry` JOINs `int_execution_zodiac_module_proxies` on `proxy_address` to distinguish real Zodiac proxies from arbitrary contracts. The zodiac unit intentionally excludes SpenderModule (`0x70db53617...`) and the global GP Spender router (`0xcff260...`), which are handled in or pending the gpay unit.

---

## Implementation assessment

**Medium**

**`contracts_zodiac_modules_registry` reads `int_execution_zodiac_module_proxies` (append+ReplacingMergeTree) without FINAL**
`models/execution/zodiac/intermediate/contracts_zodiac_modules_registry.sql`

The registry SELECTs directly from the proxies incremental without `FINAL`. If ClickHouse has not yet merged duplicate parts, pre-merge duplicates briefly inflate the registry and double-feed `decode_logs`. Currently data is clean (290,414 rows in proxies == 290,414 unique `proxy_address` values; registry: 153,127 across DelayModule 86,066 + RolesModule 67,061), so no current corruption. Correctness is merge-timing-dependent. Add `FINAL` to the subquery read, or dedup via `argMax`/`uniqExact` in a CTE.

**`int_execution_zodiac_modifier_submodules_latest` has no grain-uniqueness test**
`models/execution/zodiac/intermediate/int_execution_zodiac_modifier_submodules_latest.sql`, `models/execution/zodiac/intermediate/schema.yml`

The model is designed one row per `(modifier_address, submodule_address)` and currently satisfies it (warehouse-verified). Both upstream incrementals declare `dbt_utils.unique_combination_of_columns` tests (`schema.yml` lines 68 and 194 respectively), but the snapshot model does not. A future upstream reprocess or logic change could silently break the grain with no CI catch. Add the grain test to `schema.yml`.

**Low**

**`unique_key` declared on append-strategy incrementals is dead config**
`models/execution/zodiac/intermediate/int_execution_zodiac_modifier_module_events.sql`, `models/execution/zodiac/intermediate/int_execution_zodiac_module_proxies.sql`

Both models declare `unique_key` while using `incremental_strategy='append'`. In dbt-clickhouse, `unique_key` is ignored for `append`; dedup relies entirely on ReplacingMergeTree merges plus the `row_number` dedup applied inside `decode_logs` before appending. Both tables are currently duplicate-free. The declared `unique_key` misleads readers into assuming enforced dedup guarantees. Document or remove.

**3-day freshness lag across all zodiac tables**
`models/execution/zodiac/intermediate/int_execution_zodiac_module_proxies.sql`, `models/execution/zodiac/intermediate/int_execution_zodiac_modifier_module_events.sql`

Max `block_timestamp` is 2026-06-08 vs today 2026-06-11 in both incremental tables. The daily microbatch schedule should advance to T-1 at most. The latest-snapshot model inherits the same lag. This is likely a pipeline cadence issue rather than a logic bug, but should be confirmed as expected vs a stalled runner, and a freshness test added if T-1 is the SLA.

**`contracts_zodiac_modules_registry` `multiIf` 'Unknown' arm is unreachable**
`models/execution/zodiac/intermediate/contracts_zodiac_modules_registry.sql`

The proxies CTE `WHERE` clause restricts to exactly the four mastercopies fully covered by the `DelayModule`/`RolesModule` `multiIf` branches, so the `'Unknown'` fallback never fires (0 rows confirmed). Harmless today, but a maintenance trap: adding a new mastercopy to the `WHERE` filter without updating `multiIf` silently mislabels it. Keep the filter and classifier in sync, or add an assertion.

**Modifier-event decode uses `nullIf` workaround instead of `join_use_nulls` hook**
`models/execution/zodiac/intermediate/int_execution_zodiac_modifier_module_events.sql`

`decode_logs` uses internal `ANY LEFT JOIN` without `join_use_nulls=1`, so unmatched rows return `''` rather than `NULL`. The model compensates via `nullIf(decoded_params['module'], '')` for the Map-key lookup — a correct workaround for that specific case, not a LEFT-JOIN-column case, so no data error. Inconsistent with the project's `join_use_nulls` hook convention. The same pattern appears in the upstream `int_execution_safes_module_events`, making this a systematic project gap rather than zodiac-specific. Low priority; align for maintainer clarity.

---

## Business-logic assessment

**High**

**`is_erc1271_exploitable` is a byte-for-byte duplicate of `submodule_is_safe` — false analytical independence on a security surface**
`models/execution/zodiac/intermediate/int_execution_zodiac_modifier_submodules_latest.sql`, `models/execution/zodiac/intermediate/schema.yml`

Both columns resolve to `toUInt8(s.address IS NOT NULL)` (lines 76-77 of the model). They are always equal in every row. The schema even documents `is_erc1271_exploitable` as "mirrors" `submodule_is_safe`. This is a security/audit surface (the ERC-1271 revert-data bypass on Modifier mastercopies), and a column named `exploitable` implies an independent, audited predicate — for example, mastercopy-version gating, Roles-vs-Delay scoping, or block-height reachability checks. As written, every Roles module whose sub-module is a Safe is flagged exploitable with zero independent justification. For an incident-response reader or external auditor, a security flag that is definitionally identical to a topology flag both over-states the audit work done and potentially over- or under-states the real attack surface. Either collapse to one column or implement and document the real distinct precondition.

**Medium**

**Modifier-event history starts 2023-11-01 while proxy discovery starts 2021-01-01 — silent lookback gap in the "latest" snapshot**
`models/execution/zodiac/intermediate/int_execution_zodiac_modifier_module_events.sql`, `models/execution/zodiac/intermediate/int_execution_zodiac_modifier_submodules_latest.sql`, `models/execution/zodiac/intermediate/schema.yml`

`int_execution_zodiac_module_proxies` has `start_blocktime 2021-01-01`; `int_execution_zodiac_modifier_module_events` starts at `2023-11-01` (confirmed in schema). `EnabledModule`/`DisabledModule` events on Modifier proxies between 2021 and Nov-2023 are never decoded. Any modifier whose only sub-module activity predates Nov-2023 appears in the proxies model but shows an incorrect or empty sub-module/avatar state in `int_execution_zodiac_modifier_submodules_latest`. The gap is not documented in `schema.yml`. For a model named "latest" this is a real survivorship/lookback bias, not merely a freshness lag.

**Low**

**Chain-wide ERC-1271 view has zero downstream consumers — built but never surfaced**
`models/execution/zodiac/intermediate/int_execution_zodiac_modifier_submodules_latest.sql`

The richest model in the unit (`is_gp`, `submodule_is_safe`, `is_erc1271_exploitable`, `avatar_address`, `modifier_address`) has no downstream dbt consumer, no mart, no `api_*` tag, and no semantic coverage (`semantic_paths: []`). 157,845 rows of chain-wide Modifier topology with an ERC-1271 exploit surface flag are computed on every run and read by nothing in the current model graph. Either wire it to a mart/API/security dashboard, or document the non-dbt runbook that consumes it. As-is it is dead or undocumented compute.

**`is_gp` creates an undocumented cross-unit dependency with no freshness linkage**
`models/execution/zodiac/intermediate/int_execution_zodiac_modifier_submodules_latest.sql`

`is_gp` is derived by joining `int_execution_gpay_wallets`, which is downstream of Dune labels (`stg_gpay__wallets`). The zodiac unit is therefore not self-contained. The `is_gp` flag inherits any staleness or incompleteness from the Dune source — recently onboarded GP Safes may be misclassified as non-GP. No freshness SLA links the two units. Acceptable for an `authoritative: false` intermediate, but must be flagged before `is_gp` is surfaced to any consumer.

**~137k factory proxies silently dropped by the hardcoded four-mastercopy filter**
`models/execution/zodiac/intermediate/contracts_zodiac_modules_registry.sql`, `models/execution/zodiac/intermediate/int_execution_zodiac_module_proxies.sql`

`int_execution_zodiac_module_proxies` holds 290,414 deployed proxies; `contracts_zodiac_modules_registry` covers 153,127 (the four hardcoded Delay/Roles mastercopies). The remaining ~137,287 proxies — potentially other Zodiac module types (ConnextModule, OzGovernor, Reality) or undiscovered Delay/Roles mastercopy variants — accumulate in the proxies table but are silently excluded from the registry with no alerting. Intentional scoping today, but a maintenance trap: a new mastercopy added to the `WHERE` filter without updating `multiIf` silently mislabels it as 'Unknown' (which today fires 0 rows).

---

## Data findings

All findings based on warehouse queries run during inspection:

| Query | Result |
|---|---|
| Row count + unique `proxy_address` in `int_execution_zodiac_module_proxies` | 290,414 rows; 290,414 unique (no duplicates) |
| Row count in `contracts_zodiac_modules_registry` by `contract_type` | 153,127 total: DelayModule 86,066; RolesModule 67,061; Unknown 0 |
| Row count in `int_execution_zodiac_modifier_module_events` (grain check) | Duplicate-free on `(transaction_hash, log_index)` |
| Row count in `int_execution_zodiac_modifier_submodules_latest` | 157,845 total; 1,304 rows with `avatar_address IS NULL` (~0.8%) |
| Max `block_timestamp` in incremental tables | 2026-06-08 (both); 3-day lag vs today 2026-06-11 |
| Modifiers with multiple avatar addresses (argMax Tuple check) | 0 |

The 1,304 NULL `avatar_address` rows are modifiers that appear in modifier events but have no corresponding Safe-side `EnabledModule` entry. Probable causes: the modifier was enabled on a non-Safe avatar (EOA or arbitrary contract not in `contracts_safe_registry`), or the Safe-side event predates Safe registry coverage (2020-05-21). Small fraction; expected for non-Safe avatars.

The ~137,287 unregistered proxies may include other Zodiac module types using the same factory. They are not currently consumed by any downstream model.

---

## Pros / Cons

**Pros**

- Sound core SQL: `argMax`-based event dedup is correct (Tuple lexicographic ordering is valid: `block_number` dominates, `log_index` breaks ties); no division-by-zero; no timezone issues; clean grain in all three materialized tables (warehouse-verified).
- Clear architectural separation: zodiac is chain-wide (security/audit scope) while gpay is GP-scoped — deliberate, well-reasoned boundary supported by the context analysis.
- Canonical contract addresses verified against `seeds/event_signatures.csv`; non-indexed Modifier `EnabledModule` decoding distinction is correctly handled (distinct from Safe-emitted equivalents).
- Both upstream incrementals carry `dbt_utils.unique_combination_of_columns` grain tests.
- Provides genuinely novel infrastructure: the canonical factory-level proxy registry that `execution/gpay` depends on — no other source distinguishes real Zodiac module addresses from arbitrary contracts.
- Correctly marked `authoritative: false` with no `api_*` tags; CI tag rules correctly do not apply.

**Cons**

- `is_erc1271_exploitable` is definitionally identical to `submodule_is_safe` — a security flag with false analytical independence.
- Undocumented 2021-vs-Nov-2023 window mismatch between proxy discovery and modifier events biases the "latest" snapshot.
- The most analytically rich model (`int_execution_zodiac_modifier_submodules_latest`) has zero downstream consumers, no mart, no API tag, no semantic coverage.
- Cross-unit dependency on `gpay`/Dune-sourced `is_gp` with no freshness SLA; zodiac is not self-contained as its docs imply.
- Registry silently drops ~137k factory proxies via hardcoded four-mastercopy filter with no alerting on new mastercopies.
- Dead config: `unique_key` declared on append-strategy incrementals has no enforcement in dbt-clickhouse.
- No grain-uniqueness test on `int_execution_zodiac_modifier_submodules_latest` despite both upstreams carrying one.
- `contracts_zodiac_modules_registry` reads an append+ReplacingMergeTree upstream without `FINAL` — row counts are merge-timing-dependent.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| 1 | Resolve `is_erc1271_exploitable`: collapse into `submodule_is_safe` (remove the duplicate column) OR implement and document the real independent predicate (mastercopy-version gating, Roles-vs-Delay scoping). Do not ship a security flag that is definitionally identical to a topology flag. | `int_execution_zodiac_modifier_submodules_latest.sql`, `schema.yml` |
| 2 | Document the 2021-vs-Nov-2023 lookback gap in `schema.yml`. Decide whether the modifier-events model must be backfilled to 2021 for a correct "latest" snapshot; if Nov-2023 is intentional, state the rationale explicitly. | `int_execution_zodiac_modifier_module_events.sql`, `int_execution_zodiac_modifier_submodules_latest.sql`, `schema.yml` |
| 3 | Add `dbt_utils.unique_combination_of_columns` on `(modifier_address, submodule_address)` to `int_execution_zodiac_modifier_submodules_latest` in `schema.yml`, matching the pattern of both upstream models. | `schema.yml` |
| 4 | Add `FINAL` (or an `argMax`/`uniqExact` CTE dedup) to `contracts_zodiac_modules_registry`'s read of `int_execution_zodiac_module_proxies` to remove the RMT merge-timing race. | `contracts_zodiac_modules_registry.sql` |
| 5 | Decide the fate of `int_execution_zodiac_modifier_submodules_latest`: wire it to a mart/API/semantic surface (it is the chain-wide security view), or document the non-dbt runbook that consumes it. Otherwise it is dead compute run daily. | `int_execution_zodiac_modifier_submodules_latest.sql` |
| 6 | Confirm the 3-day freshness lag (expected pipeline cadence vs stalled runner). Add a dbt `freshness` test if T-1 is the SLA. | `int_execution_zodiac_module_proxies.sql`, `int_execution_zodiac_modifier_module_events.sql` |
| 7 | Document the cross-unit `zodiac -> gpay -> Dune` dependency for `is_gp` in `schema.yml` and establish a freshness linkage before `is_gp` is surfaced to any consumer. | `int_execution_zodiac_modifier_submodules_latest.sql` |
| 8 | Remove or clearly comment the dead `unique_key` config on both append-strategy incrementals to avoid misleading readers about dedup guarantees. | `int_execution_zodiac_modifier_module_events.sql`, `int_execution_zodiac_module_proxies.sql` |
| 9 | Add a guard or assertion so the registry's `WHERE` mastercopy filter and the `multiIf` classification arms stay in sync, and surface/alert on the ~137k unclassified factory proxies. | `contracts_zodiac_modules_registry.sql` |
| 10 | Add a comment on the `argMax(safe_address, (block_number, log_index))` Tuple ordering in `int_execution_zodiac_modifier_submodules_latest` explaining why lexicographic ordering is correct here. | `int_execution_zodiac_modifier_submodules_latest.sql` |

---

## Open disagreements

None. Both agents converged in round 1 with no contradictions.

---

## Review log

Round 1: Inspector and context agents ran in parallel on all 5 files + 8 warehouse queries. No challenges were issued — the one apparent numerical discrepancy (290,414 proxy rows vs 153,127 registry rows) was resolved by inspection (two different tables) before the verdict. All findings were cross-confirmed between reports.
