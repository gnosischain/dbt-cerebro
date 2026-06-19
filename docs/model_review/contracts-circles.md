# Model review: contracts/Circles

**Convergence:** converged in 2 rounds — both material challenges resolved with direct warehouse evidence; all findings agreed across both inspector shards.

---

## Scope and inventory

`models/contracts/Circles` contains 47 SQL files: one registry view, two v1 Hub decode models (dev-tagged), and 44 v2 decode models covering 30+ distinct Circles smart contracts. All decode models are thin wrappers around the `decode_logs` and `decode_calls` macros materialised as ClickHouse ReplacingMergeTree tables with monthly partitioning and an append incremental strategy. The registry view (`contracts_circles_registry`) unifies statically-declared addresses (33 rows in `seeds/contracts_circles_registry_static.csv`) with factory-discovered child contracts via the `resolve_factory_children` macro.

| Layer | Count | Notes |
|---|---|---|
| Registry view | 1 | Unifies static + dynamic children |
| v1 Hub (events + calls) | 2 | dev-tagged, 58 days stale |
| v2 singleton events models | 18 | Hub, NameRegistry, ERC20Lift, etc. |
| v2 singleton calls models | 18 | Mirror of events set |
| v2 multi-address events models | 4 | BaseGroup, GroupLBPFactory, ERC20TokenOfferCycle, PaymentGateway |
| v2 multi-address calls models | 4 | Mirror of above |
| Total SQL files | 47 | |

All models are registered in `schema.yml` (557 lines) with `elementary.schema_changes` and `dbt_utils.unique_combination_of_columns` tests — except `BaseGroup_calls` (missing the grain test, one model).

---

## Business context

This unit is the foundational decode layer for all Circles UBI protocol analytics on Gnosis Chain. It does not compute business metrics — its sole purpose is reliable ABI decoding and address discovery. The `execution/Circles` sector (~129 models) depends on it exclusively for all business-metric computation: avatar registrations, trust-graph density, token supply, demurrage-adjusted balances, group collateral, invite funnel, WEAU composite metric, backing lifecycle, payment flows, CRC20 pricing, and IPFS avatar metadata.

**Key canonical definitions (from cerebro-docs and economic_concepts.md):**

- **Avatar:** a Circles protocol account — Human (mints 1 CRC/hour, event `RegisterHuman`), Group (issues group-CRC from collateral, event `RegisterGroup`), or Organization (no mint, event `RegisterOrganization`).
- **CRC:** ERC-1155 token, one per avatar, 18 decimals. Amount always normalised to demurrage units in analytics (annual decay 7%, gamma = 0.9998013320085989..., day-zero Unix 1602720000).
- **Static (inflationary) unit:** ERC-20 wrapper representation with no time decay; circlesType=1. Conversion: `amount_demurrage = amount_static * gamma^(day(block_timestamp))`.
- **Trust:** directed social relation stored as Hub event `Trust(truster, trustee, expiryTime)`. Active v2 trust: SCD2 with `valid_to IS NULL` or `valid_to > now()`.
- **PersonalMint:** Human collects accumulated CRC via `PersonalMint(human, amount, startPeriod, endPeriod)`, capped at 14-day backlog.
- **Active Minter (canonical KPI):** minted on each of last 14 consecutive days AND cumulative 14-day total >= 268.8 CRC (80% of 336 CRC max). Defined in `fct_execution_circles_v2_active_minters_daily`.
- **Economically Active Avatar (WEAU):** earned >= 1 gCRC cashback from `circles_v2_cashback_wallet` OR >= 1 CRC inviter fee in the calendar week. `is_gnosis_app_tx` flag enables in-app GA WEAU filtering.
- **Group collateral:** personal CRC deposited via `groupMint()`, tracked via `CollateralLockedSingle`/`CollateralLockedBatch`/`GroupRedeemCollateral*` events. 1:1 invariant with group-token supply.
- **V1 to V2 migration:** `migrate(avatars, amounts)` via `Migration` contract (0xd44b8dcfbadfc78ea64c55b705bfc68199b56376). Mint emissions surface in Hub as `TransferSingle` from 0x0 with operator = Migration address.
- **Backer:** address currently trusted by `circles_target_group_address` (0x1aca75e38263c79d9d4f10df0635cc6fcfe6f026, start 2025-04-25). Defined in `int_execution_circles_v2_backers_current`.

**Contract topology:** 25 singleton static contracts in the seed, 6 dynamic child families (BaseGroupRuntime, ERC20Wrapper, PaymentGatewayRuntime, ERC20TokenOfferRuntime, ERC20TokenOfferCycleRuntime, CirclesBackingOrderRuntime). Three key operational addresses — `circles_v2_cashback_wallet`, `circles_v2_gcrc_token`, and `circles_target_group_address` — exist only as `dbt_project.yml` vars with no seed-level entry or schema test.

---

## Implementation assessment

### Critical

**1. Five calls models use `execution.transactions` instead of `execution.traces` — permanently 0 rows**

`models/contracts/Circles/contracts_circles_v2_StandardTreasury_calls.sql`, `contracts_circles_v2_InvitationEscrow_calls.sql`, `contracts_circles_v2_CirclesBackingFactory_calls.sql`, `contracts_circles_v2_ERC20TokenOffer_calls.sql`, `contracts_circles_v2_PaymentGateway_calls.sql`

All five declare `tx_table=source('execution','transactions')`. Because these contracts are invoked via Safe/AA-bundler internal calls, the function selector only appears in `execution.traces`, never in the top-level `to_address` of `execution.transactions`. Row counts vs paired events models:

| Calls model | Calls rows | Events rows |
|---|---|---|
| StandardTreasury_calls | 0 | 11,526 |
| InvitationEscrow_calls | 0 | 6,421 |
| CirclesBackingFactory_calls | 0 | 3,252 |
| ERC20TokenOffer_calls | 0 | 1,106 |
| PaymentGateway_calls | 0 | 0 (expected — see data findings) |

The fix is demonstrated by `contracts_circles_v2_Migration_calls.sql` and `contracts_circles_v2_PaymentGatewayFactory_calls.sql`, which correctly use `execution.traces` and carry 10,596 and 1,468 rows respectively. Historical call data for StandardTreasury (17+ months of group minting activity) and InvitationEscrow (peak 2,167 events/month in January 2026) is permanently missing until a full-refresh is run after the source table is corrected.

### High

**2. Registry contains 4 duplicate address rows — static seed and factory-discovered entries collide**

`models/contracts/Circles/contracts_circles_registry.sql`, `seeds/contracts_circles_registry_static.csv`

Warehouse query confirmed 14,034 total rows vs 14,030 unique addresses. Four addresses appear under two different `contract_type` values each:

| Address | Static type | Factory-discovered type |
|---|---|---|
| 0x76a42... | ERC20TokenOfferCycle | ERC20TokenOfferCycleRuntime |
| 0xb3129... | ERC20TokenOfferCycleV2 | ERC20TokenOfferCycleRuntime |
| 0x12dfe... | ERC20TokenOffer | ERC20TokenOfferRuntime |
| 0x590bb... | PaymentGateway | PaymentGatewayRuntime |

No `unique(address)` test guards against this. Any downstream `LEFT JOIN` on address without a `contract_type` filter will silently produce double rows for these four contracts, inflating event counts in the ~129 `execution/Circles` models that join the registry.

**3. V1 Hub models dev-tagged and 58 days stale — no explicit retirement decision**

`models/contracts/Circles/contracts_circles_v1_Hub_calls.sql`, `contracts_circles_v1_Hub_events.sql`

Both carry `tags=['dev',...]` and have `max_ts = 2026-04-14` (58 days behind 2026-06-11). The entire v1 execution substack (`int_execution_circles_v1_trust_updates`, `int_execution_circles_v1_avatars`) is also dev-tagged. No production metric is affected today, but the large refactor commits (`fe8c9d94`, `0d261e1f`) left the dev tags in place without a rationale comment. The v1 chain is accumulating a silent data gap with no decision to retire or promote.

### Medium

**4. BaseGroup_calls dev-tagged while BaseGroup_events is production — asymmetric pair without rationale**

`models/contracts/Circles/contracts_circles_v2_BaseGroup_calls.sql`, `contracts_circles_v2_BaseGroup_events.sql`

All other calls/events pairs carry consistent tags. No comment in either file explains the asymmetry. If any downstream execution model needs function-level call data from dynamically-discovered BaseGroup runtime contracts, this tag silently blocks it. `BaseGroup_calls` carries 4,214 rows confirming it uses the `execution.transactions` source successfully (BaseGroup contracts receive direct EOA calls, unlike the five zero-row models above).

**5. BaseGroup_calls missing unique_combination_of_columns test — only decode model without grain test**

`models/contracts/Circles/contracts_circles_v2_BaseGroup_calls.sql`, `models/contracts/Circles/schema.yml`

All other 45 models have the `dbt_utils.unique_combination_of_columns` test. `BaseGroup_calls` has only `elementary.schema_changes`. For a dynamic multi-address model covering an unbounded number of group runtime instances, the missing grain test removes early-warning coverage for append-duplicate bugs. `BaseGroup_events` (also dynamic) does carry the test — the omission is specific to the calls model.

### Low

**6. CirclesBackingFactory_events start_blocktime undocumented — differs from seed and calls model without explanation**

`models/contracts/Circles/contracts_circles_v2_CirclesBackingFactory_events.sql`

The events model hardcodes `start_blocktime='2025-04-25'` while the seed and calls model both use `'2025-04-01'`. A direct warehouse query confirmed zero log rows for address `0xeced91232c609a42f6016860e8223b8aecaa7bd0` between 2025-04-01 and 2025-04-24 — the data gap is harmless. However, the discrepancy creates maintenance confusion. A one-line comment explaining that the factory emitted no events before April 25 (confirmed via warehouse query) would eliminate ambiguity.

**7. Mixed-case contract addresses in events models inconsistent with lowercase in calls models**

`models/contracts/Circles/contracts_circles_v2_BaseGroupFactory_events.sql`, `contracts_circles_v2_CMGroupDeployer_events.sql`, `contracts_circles_v2_CirclesBackingFactory_events.sql`, `contracts_circles_v2_ERC20Lift_events.sql`

Several events models pass checksummed addresses (e.g., `'0xD0B5Bd9962197...'`) while calls siblings use lowercase. The `decode_logs` macro normalises addresses internally (`lower + replaceAll '0x'`), so there is no functional impact, but the inconsistency creates review overhead.

**8. unique_key narrower than order_by in multi-address events models — documentation mismatch**

`models/contracts/Circles/contracts_circles_v2_BaseGroup_events.sql`, `contracts_circles_v2_GroupLBPFactory_events.sql`, `contracts_circles_v2_ERC20TokenOfferCycle_events.sql`, `contracts_circles_v2_PaymentGateway_events.sql`

These models declare `unique_key=(contract_address, transaction_hash, log_index)` (3 cols) while `order_by` and `schema.yml` tests both use the 4-col combination including `block_timestamp`. With `incremental_strategy='append'`, `unique_key` is never evaluated at runtime — CH ReplacingMergeTree enforces dedup on the full 4-col `order_by`. No functional bug, but the `unique_key` declaration is misleadingly narrower than the operative dedup key.

---

## Business-logic assessment

### Critical

**Group minting call parameters (StandardTreasury_calls) entirely absent — group collateral analysis is input-blind**

StandardTreasury is the on-chain treasury for group token minting. The 1:1 collateral invariant is tracked via events (CollateralLockedSingle, CollateralLockedBatch, etc.) which are present and healthy. However, the calls table — which would surface `groupMint()` function parameters including caller identity, token amounts, and counterparty — has 0 rows for all 17+ months of historical activity. Any model attempting to reconstruct minting intent, collateral provenance by caller, or group treasury management workflows from the calls layer has no data. This is a permanent historical gap.

### High

**InvitationEscrow call parameters absent — invitation redemption flow reconstruction incomplete**

InvitationEscrow had peak usage of 2,167 events in January 2026 and emitted 6 events in May 2026, confirming it remains part of the active invitation funnel. The escrow release/claim call parameters are entirely absent (0 rows in calls model). The 5-stage invite funnel cadence metric relies on event-level data, but call-level input parameters cannot be used for supplementary analysis or cross-validation of escrow redemption flows.

**ERC20TokenOffer_calls 0 rows — token offer economics unanalysable from calls layer**

58 ERC20TokenOfferRuntime contracts are registered with 1,106 event rows through 2025-12-04. The calls model (which would expose offer amounts, counterparty addresses, and acceptance parameters) has 0 rows because runtime offer contracts are invoked through Safe. All function-call-level data for offer economics is missing.

### Medium

**Registry duplicate addresses create silent fan-out risk for downstream execution models**

Four template addresses appear in both the static seed (as implementation contracts) and factory-discovered children (as runtime instances), producing two registry rows each. Any execution/Circles intermediate model that joins the registry on `address` alone — without a `contract_type` predicate — will double-count events for these four addresses. With ~129 models in the downstream layer, the registry join pattern needs audit to confirm all joins are `contract_type`-safe.

**ERC20TokenOffer_events (production-tagged) shows max_ts 2025-12-04 — over 6 months stale, no monitoring**

`models/contracts/Circles/contracts_circles_v2_ERC20TokenOffer_events.sql`

This model is production-tagged with `start_blocktime='2025-10-01'`. Without a freshness test or a documented inactivity status, production consumers cannot distinguish genuine contract inactivity (no new ERC20TokenOffer runtime instances deployed since December 2025) from a stuck incremental watermark.

### Low

**Three key Circles operational addresses defined only as dbt_project.yml vars — no seed-level auditability**

`circles_v2_cashback_wallet` (0x7abe74b71f2958b624cb2be0596678784c0caf6a), `circles_v2_gcrc_token` (0x548c20e6c24e4876e20dadbeab75362e2f5a4bc1), and `circles_target_group_address` (0x1aca75e38263c79d9d4f10df0635cc6fcfe6f026) govern the WEAU composite metric and quarterly Backer reporting. They exist only in `dbt_project.yml` with no seed-file entry, no schema test, and no cross-reference to on-chain deployment documentation. A misconfiguration would silently corrupt the GA WEAU composite metric without triggering any test failure.

---

## Data findings

Queries run across both inspector shards (14 total warehouse queries):

| Query | Key result |
|---|---|
| CirclesBackingFactory_events row count / freshness | 3,252 rows, min_ts 2025-04-25, zero empty event_name |
| CirclesBackingFactory_events gap window (2025-04-01 to 2025-04-24) | 0 rows on-chain — gap confirmed harmless |
| PaymentGatewayRuntime events via execution.logs (all 72 addresses, since 2025-12-01) | 0 rows — confirmed expected, contracts emit no EVM logs |
| Registry duplicate address check | 14,034 total vs 14,030 unique — 4 collisions confirmed |
| Hub v2 events grain check | 17.7M rows, max 2026-06-08, zero duplicates |
| ERC20TokenOfferCycle_events grain check | 9,044 rows, zero duplicates |
| V1 Hub events/calls freshness | max_ts 2026-04-14, 58 days behind |
| StandardTreasury freshness (calls vs events) | 0 calls rows vs 11,526 events |
| InvitationEscrow freshness (calls vs events) | 0 calls rows vs 6,421 events, last event May 2026 |
| CirclesBackingFactory calls vs events | 0 calls rows vs 3,252 events through 2026-06-08 |
| ERC20TokenOfferRuntime count and event/call rows | 58 runtime instances, 0 calls rows vs 1,106 events |
| Migration_calls (reference / healthy) | 10,596 rows via execution.traces |
| PaymentGatewayFactory_calls (reference / healthy) | 1,468 rows via execution.traces |
| ERC20TokenOffer_events freshness | max_ts 2025-12-04, 6+ months stale |

---

## Pros / Cons

**Pros**

- Registry pattern is architecturally sound — static plus factory-discovered children unified in one view with correct `depends_on` wiring and `resolve_factory_children` macro.
- `Migration_calls` and `PaymentGatewayFactory_calls` correctly use `execution.traces` for Safe/AA-bundler internal calls, establishing a proven fix pattern for the five broken models.
- Incremental watermark via `block_number` is consistent across all 47 models with monthly CH partitioning, avoiding the >100-partition insert block limit.
- All hardcoded addresses in SQL files cross-check correctly against `seeds/contracts_circles_registry_static.csv`.
- Schema.yml covers 45 of 46 production models with both `elementary.schema_changes` and `dbt_utils.unique_combination_of_columns` grain tests — only `BaseGroup_calls` is missing the latter.
- Hub v2 (17.7M rows) passes grain checks with zero duplicates; `ERC20TokenOfferCycle_events` also zero duplicates.
- `PaymentGatewayFactory` dual-path modelling (events for GatewayCreated log, calls for traces) is correctly documented and the factory child-discovery reads the events model.
- CH ReplacingMergeTree dedup operates on the full 4-col `order_by` regardless of the narrower `dbt unique_key` declaration, maintaining on-disk correctness.

**Cons**

- Five calls models are permanently 0 rows due to using `execution.transactions` instead of `execution.traces` — the fix pattern exists but has not been applied.
- Registry contains 4 duplicate address rows with no `unique(address)` test, creating fan-out risk for downstream LEFT JOINs without `contract_type` filter.
- V1 Hub models are 58 days stale and dev-tagged with no explicit retirement decision, leaving the v1 decode chain in limbo.
- `BaseGroup_calls` is dev-tagged while `BaseGroup_events` is production — asymmetric pair with no documented rationale.
- `BaseGroup_calls` is the only decode model missing a `unique_combination_of_columns` test.
- `CirclesBackingFactory_events` start_blocktime differs from seed and calls model without any explanatory comment.
- `ERC20TokenOffer_events` (production-tagged) has max_ts 2025-12-04 with no freshness monitoring to distinguish inactivity from pipeline failure.
- Three key WEAU/Backer operational addresses exist only as dbt_project.yml vars with no seed-level auditability.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| Immediate | Switch `StandardTreasury_calls`, `InvitationEscrow_calls`, `CirclesBackingFactory_calls`, `ERC20TokenOffer_calls`, and `PaymentGateway_calls` from `source('execution','transactions')` to `source('execution','traces')`, following the `Migration_calls` pattern. Run full-refresh on each. StandardTreasury and InvitationEscrow are highest priority given business-metric significance. | `contracts_circles_v2_StandardTreasury_calls.sql`, `contracts_circles_v2_InvitationEscrow_calls.sql`, `contracts_circles_v2_CirclesBackingFactory_calls.sql`, `contracts_circles_v2_ERC20TokenOffer_calls.sql`, `contracts_circles_v2_PaymentGateway_calls.sql` |
| Immediate | Add a `unique(address)` test to `contracts_circles_registry` in `schema.yml`, OR prune the static seed to remove the four addresses also factory-discovered (0x76a42, 0xb3129, 0x12dfe, 0x590bb). Until resolved, audit all ~129 `execution/Circles` models joining the registry to confirm every join includes a `contract_type` predicate. | `contracts_circles_registry.sql`, `seeds/contracts_circles_registry_static.csv` |
| Short-term | Make an explicit V1 decision: (a) tag `contracts_circles_v1_Hub_calls` and `_events` as `tags=['retired',...]` with a deprecation comment citing the migration date and formally retire the v1 execution substack, or (b) re-tag to production, run a catchup full-refresh, and promote the v1 execution intermediates. The current dev-tag-plus-58-day-gap state is operationally confusing. | `contracts_circles_v1_Hub_calls.sql`, `contracts_circles_v1_Hub_events.sql` |
| Short-term | Resolve the `BaseGroup_calls` dev tag — either promote to production with the events model, or add an inline comment explaining why calls are intentionally excluded. Add the missing `dbt_utils.unique_combination_of_columns` test to `schema.yml` regardless of tag decision. | `contracts_circles_v2_BaseGroup_calls.sql`, `schema.yml` |
| Short-term | Add a freshness test (dbt source freshness or `elementary.freshness_anomalies`) to `ERC20TokenOffer_events` and other production-tagged models with no recent rows, or add a schema.yml comment marking the contract as organically inactive since 2025-12-04. | `contracts_circles_v2_ERC20TokenOffer_events.sql` |
| Short-term | Add a one-line comment to `contracts_circles_v2_CirclesBackingFactory_events.sql` explaining that `start_blocktime='2025-04-25'` is used because the factory emitted no events before this date (confirmed via warehouse query 2026-06-11), while the seed deployment date of 2025-04-01 reflects contract deployment, not first event. | `contracts_circles_v2_CirclesBackingFactory_events.sql` |
| Short-term | Add `circles_v2_cashback_wallet`, `circles_v2_gcrc_token`, and `circles_target_group_address` to a Circles-specific constants seed file (or to `contracts_circles_registry_static.csv`) with start dates and descriptive names. These addresses govern the WEAU composite metric and Backer reporting and require the same audit trail as other static registry entries. | `seeds/contracts_circles_registry_static.csv`, `dbt_project.yml` |
| Maintenance | Normalise all hardcoded contract addresses to lowercase in events models (`BaseGroupFactory_events`, `CMGroupDeployer_events`, `CirclesBackingFactory_events`, `ERC20Lift_events`) to match calls model convention. | Four events models |
| Maintenance | Align `unique_key` to match `order_by` in the four multi-address events models — add `block_timestamp` to `unique_key` so the dbt declaration matches the operative CH ReplacingMergeTree dedup key. Documentation fix only; no data change. | `contracts_circles_v2_BaseGroup_events.sql`, `contracts_circles_v2_GroupLBPFactory_events.sql`, `contracts_circles_v2_ERC20TokenOfferCycle_events.sql`, `contracts_circles_v2_PaymentGateway_events.sql` |
| Maintenance | Document the four static-seed-only entries with no decode models (ValueFactory, CirclesBackingOrder, AggregatorsDAI, BaseGroupMintPolicy) with a note in the seed CSV (e.g., `decode_model=none — ABI lookup only`) so future reviewers understand their presence in the registry is intentional. | `seeds/contracts_circles_registry_static.csv` |

---

## Open disagreements

None. Review converged in round 2.

---

## Review log

| Round | Event | Outcome |
|---|---|---|
| R1 | Inspector alpha-1 scanned files 1-24 (registry, v1 Hub, v2 AffiliateGroupRegistry through InvitationEscrow); ran 8 warehouse queries | Identified critical registry duplicate finding, CirclesBackingFactory_events start_blocktime skew (elevated as high data finding), five-model zero-row cluster partially identified (three models in this shard) |
| R1 | Inspector alpha-2 scanned files 24-47 (ERC20TokenOffer through StandardTreasury); ran 8 warehouse queries | Independently confirmed five-model zero-row cluster; identified PaymentGateway_events 0-row anomaly as medium finding; confirmed duplicate address finding |
| R1 | Context gatherer confirmed no semantic models in contracts/Circles layer (correct by design); confirmed downstream execution/Circles semantic layer exists with partial coverage; documented full contract topology and canonical definitions | No challenges issued to context |
| R2 challenge | Inspector alpha-1 challenged: "PaymentGateway_events 0 rows might be a registry filter mismatch rather than expected contract behavior" | Resolved — direct query against `execution.logs` for all 72 PaymentGatewayRuntime addresses since 2025-12-01 returned 0 rows (11.9s full scan); contracts genuinely emit no EVM log events. Finding downgraded from medium to low (informational). |
| R2 challenge | Inspector alpha-2 challenged: "CirclesBackingFactory_events gap window might contain missing data, not confirming start date is correct" | Resolved — direct query against `execution.logs` for address 0xeced91232c609a42f6016860e8223b8aecaa7bd0 between 2025-04-01 and 2025-04-24 returned 0 rows (1.3s); data gap confirmed empty on-chain. Finding downgraded from high to low (documentation inconsistency only). |
| R2 challenge | Context gatherer challenged: "GatewayCreated dual-path (events vs traces) — does registry child-discovery reference the correct model?" | Resolved — `contracts_circles_registry.sql` line 12 correctly depends_on `contracts_circles_v2_PaymentGatewayFactory_events` (log source); the calls model (traces source) is correctly separate. Documented in caveats. |
| R2 challenge | Context gatherer challenged: "unique_key vs order_by asymmetry in multi-address events models — is this a functional bug?" | Resolved — with `incremental_strategy='append'`, `unique_key` is never evaluated at runtime; CH ReplacingMergeTree enforces dedup on the full 4-col `order_by`. Downgraded to informational. |
