# Model review: execution/shared

**Convergence:** converged in 1 round — both inspector and context reports reached identical conclusions on all material issues; no challenges remained unresolved.

---

## Scope and inventory

| Layer | Count | Key models |
|---|---|---|
| Marts (intermediate) | 1 | `int_execution_address_roles_current` — address-to-role pivot |
| Schema / semantic | 2 | `schema.yml`, `semantic/authoring/execution/shared/semantic_models.yml` |
| **Total** | **3** | — |

This is a minimal single-model unit. The model lives in `models/execution/shared/marts/` but carries the `int_` prefix by design — it is a shared join endpoint for mini-apps, not a published API or aggregate KPI mart.

---

## Business context

`int_execution_address_roles_current` answers: "What is this address, and which sectors does it participate in?" for any EVM address on Gnosis Chain. It produces one row per address with UInt8 boolean role flags derived from 10 upstream sources.

**Primary consumers:** Graph Explorer (`ui://cerebro/graph_explorer`) for automatic profile-selection and node-kind role badges; Portfolio mini-app for Overview role-flag display and auto-tab seeding. Also used as a semantic-layer auxiliary join endpoint via two approved relationships (`address_roles_pivot_to_safe`, `address_roles_pivot_to_dune_label`).

**Canonical role definitions:**

| Flag | Definition |
|---|---|
| `is_safe` | Address is registered as a Safe proxy in `contracts_safe_registry` |
| `is_gpay_wallet` | Safe that emitted SafeSetup AND sent a whitelisted ERC-20 to the GPay spender (0x4822521E6135CD2599199c83Ea35179229A172EE), excluding operational seeds |
| `is_ga_user` | EOA currently controlling a GPay Safe via the Zodiac Delay Module (`is_currently_ga_owned=1`) — intentionally NOT via `safes_current_owners.owner`, which returns sentinel 0x...0002 for GPay Safes |
| `controls_gpay_wallet` | The GPay Safe address this GA user currently controls |
| `is_circles_avatar` | Registered Circles v2 avatar (Human, Group, or Org) — covers all ecosystem avatars, not just in-app GA users |
| `circles_avatar_type` | Human / Group / Org from Circles v2 registration |
| `is_circles_wrapper` | ERC20 wrapper token contract deployed via ERC20Lift for a Circles avatar |
| `is_safe_owner` | Appears as a current owner in at least one Safe (point-in-time snapshot, no historical tracking) |
| `is_lp_provider` | Has provided liquidity to a tracked DEX pool (Uniswap V3, Swapr V3, Balancer V2/V3) |
| `is_pool` | Is a DEX pool contract tracked in `int_execution_pools_dex_liquidity_events` |
| `is_lending_user` | Has an active lending position with `balance_usd > 0.01` in Aave V3 or SparkLend — point-in-time, not historical |
| `is_validator_depositor` | 20-byte EVM address from a 0x01/0x02-type withdrawal credential on the Gnosis Beacon Chain; 0x00 BLS validators excluded |
| `has_dune_label` | Appears in `int_crawlers_data_labels` |
| `pool_protocol` | Protocol label for LP addresses (Uniswap V3 / Swapr V3 / Balancer V2 / V3) |
| `dune_project` | Dune-label project classification |

**Contract context:** The GPay spender address 0x4822521E6135CD2599199c83Ea35179229A172EE is hardcoded in `int_execution_gnosis_app_gpay_wallets` schema description only — it is not in any seed or dbt var. Circles v2 Hub and the Zodiac Delay Module are consumed via upstream models, not direct SQL decoding in this unit.

---

## Implementation assessment

### High

**`int_consensus_validators_labels` (direct upstream of `is_validator_depositor`) is tagged `dev`**
(`models/consensus/intermediate/int_consensus_validators_labels.sql`, `models/consensus/intermediate/int_consensus_validators_withdrawal_addresses.sql`)

Confirmed in source: `tags=['dev','consensus','validators']`. `int_consensus_validators_withdrawal_addresses` does `ref('int_consensus_validators_labels')` directly (line 22), and that view is the sole source for `is_validator_depositor`. A CI run or selective build that excludes `dev` models rebuilds labels as empty, silently setting `is_validator_depositor=0` for every address. Warehouse currently shows 873 validator addresses (populated today), so the flag is correct at present — but the `dev` tag is a live latent risk for production builds.

**Upstream `int_execution_gnosis_app_gpay_wallets` uses the banned `delete+insert` strategy**
(`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_gpay_wallets.sql`)

Confirmed: `materialized='incremental'`, `incremental_strategy='delete+insert'`. The project bans this via `scripts/checks/no_delete_insert.py`. This upstream feeds `is_ga_user` and `controls_gpay_wallet`. ALTER DELETE mutations issued by this strategy risk duplicate or stale rows reaching the pivot; the CI guard's purpose is to prevent exactly this. The violation is at the upstream level, not in this model's SQL, but the blast radius lands here.

### Medium

**No `expose_to_mcp` or `privacy_tier` metadata despite MCP reachability**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`, `models/execution/shared/marts/schema.yml`)

The model links on-chain addresses to product roles (GPay wallet, Circles avatar, Safe ownership, GA control) and is registered at `quality_tier: approved` in `semantic_models.yml`, making it reachable by the MCP planner via two approved relationships. Other privacy-sensitive execution models explicitly set `expose_to_mcp: false` and a `privacy_tier`. This model sets neither, so MCP exposure is governed only by the absence of an opt-in rather than an explicit policy decision.

**Not tagged `production`; `check_api_tags.py` would skip it if an `api:` tag is ever added**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`)

Current tags: `['execution', 'shared', 'identity', 'graph_explorer']`. Without `production`, the `check_api_tags.py` CI guard does not evaluate this model, so a future `api:` tag could be added without triggering `granularity:`/`tier:` validation. Low impact today (no `api:` tag exists) but a governance gap.

### Low

**`ReplacingMergeTree` engine on a full-rebuild table with no versioning column**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`)

`materialized='table'` with `engine='ReplacingMergeTree()'` and `order_by='(address)'` but no `ver` column. Full rebuilds drop and recreate the table, so immediate duplicates are unlikely; the unique test passes. However, the engine choice is semantically misleading for a full-rebuild table (no background dedup is meaningful here), and semantic-layer consumers querying between background merges could see duplicate addresses. `MergeTree()` would be clearer, or `FINAL`-on-read semantics should be documented.

**Session-scoped pre/post hooks can leak degraded settings on mid-build failure**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`)

`pre_hook` sets `max_threads=1`, `max_block_size=8192`, `max_bytes_before_external_group_by=2GB`; `post_hook` resets them. On ClickHouse Cloud, session settings are connection-scoped. A failure between hooks leaves the next model on that connection inheriting `max_threads=1`. Using `query_settings={}` in the `config` block (query-scoped) would eliminate the leak risk.

**`UNION ALL` positional column padding is fragile to schema change**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`)

Eight of ten branches in the `all_rows` UNION ALL use positional literal padding (e.g. `SELECT address, 0, 1, 0, '', 0, '', 0, 0, 0, '', 0, 0, 0, 0, ''`). The first and third branches use named columns. Adding a new flag column requires updating every positional branch in lockstep; a missed branch silently shifts column assignments. Named columns throughout would prevent silent misalignment.

---

## Business-logic assessment

### Critical

**`is_lending_user` is 0 for ALL 5.8 million addresses — the lending flag is silently broken**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`, `models/execution/yields/marts/fct_execution_yields_user_lending_positions_latest.sql`, `models/execution/yields/intermediate/int_execution_lending_aave_user_balances_daily.sql`)

Warehouse confirms `sum(is_lending_user) = 0` across all rows. Root cause traced in full:

1. `fct_execution_yields_user_lending_positions_latest` has 0 rows.
2. Its `latest_date` CTE selects `max(date) WHERE date < today()`.
3. In CH Cloud `today()` returns 2026-06-13. The max date in `int_execution_lending_aave_user_balances_daily` is 2026-06-09 (epoch 20613) — a partial incremental load where every row has `balance_usd = 0.00` (balances written, USD prices not yet joined).
4. The prior complete day (2026-06-08, epoch 20612) has 41,086 rows with 19,624 positive-USD positions.
5. Because CH Cloud `today()` is 4 days ahead of the max data date, `date < today()` does NOT skip the broken partial day — it lands on epoch 20613 and the `balance_usd > 0.01` filter eliminates everything.

Any consumer currently sees zero lending users — Graph Explorer role badges, Portfolio overview, and semantic-layer enrichment are all silently wrong. This is a definition-vs-reality failure, not a pipeline lag.

### High

**`is_safe_owner` returns 1 for the GPay sentinel address 0x...0002**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`, `models/execution/safe/intermediate/int_execution_safes_current_owners.sql`)

The schema explicitly warns against using `safes_current_owners` for GA-user identification because Gnosis Pay uses a sentinel `initial_owner` (0x...0002) in `SafeSetup`. However, `is_safe_owner` flags any address appearing in `int_execution_safes_current_owners` without exclusion, meaning the sentinel is tagged as a prolific Safe owner. The `is_ga_user` flag correctly avoids this via the Delay-Module heuristic — `is_safe_owner` just does not apply the same guard.

### Medium

**`is_lending_user` is point-in-time, not 'has ever lent' — undocumented for Graph Explorer consumers**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`)

The flag reflects only currently-open positions with `balance_usd > 0.01`. An address that closed all positions reads 0. Graph Explorer and Portfolio consumers may reasonably expect a historical 'has lent' signal. The semantics are defensible but the schema column description says only 'active position' without qualifying that closure zeroes the flag.

**`pool_protocol` is non-deterministic for multi-protocol LP addresses**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`)

The `dex_roles` CTE uses `anyIf(pool_protocol, role='lp' AND pool_protocol != '')`, which returns ONE arbitrary protocol per address. Warehouse confirms LPs active in both Balancer V2 and V3 exist (e.g. 0x458c... with 39 pool rows mixing both protocols). A consumer reading `pool_protocol` as 'the protocol this LP uses' silently loses secondary protocol memberships. Should use `groupArrayDistinct` to produce a complete set, or the column description must explicitly state the value is a non-deterministic representative sample.

**Semantic model is `quality_tier: approved` but metric descriptions say 'review before relying on it'**
(`models/execution/shared/marts/schema.yml`, `semantic/authoring/execution/shared/semantic_models.yml`)

`semantic_models.yml` registers the model at `quality_tier: approved` and declares three sum-of-flag metrics (`is_safe_value`, `is_gpay_wallet_value`, `is_circles_avatar_value`). The auto-generated metric descriptions read 'candidate; review and promote before relying on it'. These are contradictory signals reaching the MCP planner. Additionally, summing UInt8 flags produces a distinct-address count only because the `unique(address)` constraint holds; a consumer treating the metrics as event counts will be misled. Eleven of 14 flags (including the broken `is_lending_user`) have no corresponding semantic measure, leaving coverage partial.

### Low

**`is_validator_depositor` excludes 0x00 BLS-credential validators — not stated in schema**
(`models/execution/shared/marts/int_execution_address_roles_current.sql`)

Only 0x01/0x02-type withdrawal credentials yield an EVM address. Older un-migrated GBC validators with 0x00 BLS credentials return NULL and are excluded. This is a known architectural limitation but is not documented in the schema column description, so depositor coverage is understated for legacy validators without any warning to consumers.

**GPay spender address is verifiable only via a schema description string**
(`models/execution/gnosis_app/intermediate/int_execution_gpay_wallets.sql`)

The canonical GPay spender 0x4822521E6135CD2599199c83Ea35179229A172EE — which gates the entire `is_gpay_wallet` definition — lives only in a `schema.yml` description. It is not in any seed, `dbt_project.yml` var, or `seeds/contracts_whitelist.csv`. It cannot be audited or updated without editing prose, a governance gap for a hardcoded address that defines a product-level flag.

---

## Data findings

Eight warehouse queries were executed during review:

| Query | Result |
|---|---|
| Grain uniqueness | 5,816,837 rows; all distinct on `address` |
| Role flag sums | `sum(is_lending_user) = 0` confirmed; all other flags non-zero |
| Multi-role addresses | 62,654 addresses carry 2+ role flags |
| Dune-label-only addresses | 4,706,904 of 5,816,837 (81%) have `has_dune_label=1` as their sole role |
| Lending upstream empty check | `fct_execution_yields_user_lending_positions_latest` = 0 rows |
| Aave balances freshness | Max date: 2026-06-09 (epoch 20613); all 4,674 rows have `balance_usd = 0.00`; prior complete day 2026-06-08 has 19,624 positive-USD rows |
| Validator withdrawal addresses | 130,238 rows; 124,293 with non-null `withdrawal_address`; 873 unique addresses flagged in `is_validator_depositor` |
| LP pool_protocol distribution | Multi-protocol LPs confirmed (e.g. 0x458c... spans Balancer V2 and V3 across 39 pool rows) |

The 81% dune-label-only concentration is expected (the Dune labels feed covers the broadest address universe) but consumers counting 'users' across product lines must explicitly filter on the relevant role flag rather than using row count.

---

## Pros / Cons

**Pros**

- Single, well-scoped model with a clear documented purpose: one row per address, boolean role flags powering Graph Explorer and Portfolio mini-apps. Not a KPI/dashboard surface, so blast radius for defects is analyst tooling rather than external reporting.
- Grain integrity is solid: 5.8 million rows all distinct on `address`, enforced by `GROUP BY` and `not_null + unique` dbt tests.
- Aggregation design is genuinely efficient: per-source CTEs pre-collapse billion-row upstreams to unique-address cardinality; single UNION ALL + GROUP BY; one `arrayJoin` pass for `is_lp_provider` and `is_pool` avoids double-scanning the DEX events table.
- Memory tuning (`grace_hash`, external spill at 2 GB, `max_threads=1`) is deliberate and appropriate for the upstream scale.
- Canonical role definitions are well-documented and cross-referenced to `docs/economic_concepts.md`. The `is_ga_user` flag correctly uses the Delay-Module heuristic and explicitly avoids the `safes_current_owners` sentinel trap.
- Both reviewer agents converged on the same model boundary, grain, and top defects — strong shared ground truth from a single round.

**Cons**

- `is_lending_user` is currently 0 for every address. A product flag is silently and entirely wrong, traceable to a partial-day load that the `date < today()` guard fails to skip because CH Cloud `today()` is 4 days ahead of the max data date.
- The direct upstream of `is_validator_depositor` (`int_consensus_validators_labels`) is tagged `dev`; a CI or selective run excluding `dev` models zeros the flag with no error.
- `is_safe_owner` does not exclude the GPay sentinel 0x...0002, even though the schema warns against trusting `safes_current_owners` for ownership identification.
- Upstream `int_execution_gnosis_app_gpay_wallets` violates the project's `no_delete_insert` ban, feeding `is_ga_user` and `controls_gpay_wallet` through a strategy the CI guard exists to prevent.
- Registered `quality_tier: approved` in the semantic layer and reachable via MCP, yet carries no `expose_to_mcp` or `privacy_tier` metadata despite linking addresses to product roles.
- `pool_protocol` is non-deterministic for multi-protocol LPs and undocumented as such.
- Semantic coverage is partial and self-contradictory: `approved` tier vs 'review before relying on it' metric descriptions; 11 of 14 flags have no semantic measure.
- Not tagged `production`, so `check_api_tags.py` would silently skip it if an `api:` tag is later added.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 — Fix now | Fix `fct_execution_yields_user_lending_positions_latest` to skip partial days: select the latest date that has a minimum positive-USD row count (or `balance_usd > 0` row count), not blindly `max(date) < today()`. Verify `sum(is_lending_user) > 0` after rebuild. Also add a `not_null` / `row_count > 0` dbt test on this mart so a future empty-mart regression fails CI instead of silently zeroing the flag. | `models/execution/yields/marts/fct_execution_yields_user_lending_positions_latest.sql`, `models/execution/yields/intermediate/int_execution_lending_aave_user_balances_daily.sql` |
| P1 — Fix soon | Re-tag `int_consensus_validators_labels` from `dev` to `production` (or point `int_consensus_validators_withdrawal_addresses` at a production-tagged source). Confirm `is_validator_depositor` survives a production-only selective run. | `models/consensus/intermediate/int_consensus_validators_labels.sql` |
| P1 — Fix soon | Exclude the GPay sentinel 0x...0002 (and entries in `seeds/gpay_operational_wallets.csv`) from `is_safe_owner`, mirroring the guard the schema already documents for GA-user identification. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P1 — Fix soon | Resolve the `delete+insert` violation on `int_execution_gnosis_app_gpay_wallets`: migrate to an allowed incremental strategy, or formally grandfather it with a documented CI exception so the guard's intent is explicit. | `models/execution/gnosis_app/intermediate/int_execution_gnosis_app_gpay_wallets.sql` |
| P2 — Address before next audit | Add `expose_to_mcp` and `privacy_tier` metadata to this model in `dbt_project.yml` or `schema.yml`, consistent with other address-bearing execution models. | `models/execution/shared/marts/schema.yml` |
| P2 — Address before next audit | Reconcile the semantic layer: either demote `quality_tier` from `approved` to `candidate`, or update the metric descriptions and document that sum-of-flag measures are distinct-address counts valid only under the `unique(address)` constraint. | `semantic/authoring/execution/shared/semantic_models.yml` |
| P2 — Address before next audit | Make `pool_protocol` deterministic for multi-protocol LPs using `groupArrayDistinct` to produce a complete protocol set, or document the column as a non-deterministic representative value in the schema. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P3 — Housekeeping | Tighten schema column descriptions: state that `is_lending_user` is point-in-time (not 'has ever lent') and that `is_validator_depositor` excludes 0x00 BLS validators. | `models/execution/shared/marts/schema.yml` |
| P3 — Housekeeping | Add the `production` tag and document the intentional `int_`-in-marts naming break so `check_api_tags.py` evaluates the model if an `api:` tag is ever added. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P3 — Housekeeping | Promote the GPay spender address 0x4822521E6135CD2599199c83Ea35179229A172EE to a `dbt_project.yml` var or seeds entry so it can be audited and updated without editing schema prose. | `models/execution/gnosis_app/intermediate/int_execution_gpay_wallets.sql` |
| P3 — Housekeeping | Replace session-scoped pre/post hooks with `query_settings={}` in the `config` block to eliminate the risk of leaking `max_threads=1` to subsequent models on the same connection after a mid-build failure. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P4 — Low priority | Convert `UNION ALL` positional literal padding to named columns throughout `all_rows` to prevent silent column-shift on future flag additions. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P4 — Low priority | Replace `ReplacingMergeTree` with `MergeTree` (no versioning column is used) or document `FINAL`-on-read semantics for semantic-layer consumers. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |

---

## Open disagreements

None. Review converged in round 1.

---

## Review log

| Round | Agent | Action | Outcome |
|---|---|---|---|
| 1 | Inspector | Ran 8 warehouse queries; confirmed is_lending_user=0 root cause, dev tag on validators_labels, delete+insert violation, non-deterministic anyIf, ReplacingMergeTree/hook issues | All findings confirmed in source |
| 1 | Context | Supplied canonical definitions, confirmed is_ga_user Delay-Module heuristic, sentinel caveat for is_safe_owner, SparkLend coverage via fct_execution_yields, and semantic-layer approved/candidate contradiction | No challenges issued; supplemented inspector with sentinel finding |
| 1 | Verdict | Merged both reports; verified four load-bearing claims in source; issued final severity assignments | Converged; no disagreements |
