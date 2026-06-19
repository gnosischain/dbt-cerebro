# Model review: p2p

**Convergence:** converged in 2 rounds — all three round-2 challenges resolved by direct warehouse evidence; no factual disagreements remain between inspectors.

---

## Scope and inventory

The `p2p` sector is Gnosis Chain's node-discovery analytics layer built on data from the Nebula crawler (operated by ProbeLab/Protocol Labs). It tracks peer topology, client distribution, geographic spread, ISP concentration, and consensus-fork adoption across two discovery protocols: Discv4 (legacy execution-layer DHT) and Discv5 (modern consensus/beacon-oriented).

| Layer | Count | Notes |
|---|---|---|
| Staging | 10 views | Pass-through SELECT from `nebula` and `nebula_discv4` schemas |
| Intermediate | 9 models | incremental insert_overwrite on monthly partitions; 2 materalized=table topology snapshots |
| Marts (api_* / fct_*) | 11 models | 9 api_ endpoints + 2 fct_ fact tables |
| Semantic models | 20 registered | All at `quality_tier: candidate`; includes both mart and intermediate grain exposures |
| Sources | 2 crawl schemas + 1 IP enrichment | `nebula`, `nebula_discv4`, `crawlers_data.ipinfo` |

All 27 SQL files and 3 schema.yml files were fully reviewed. Eight targeted warehouse queries were run in the staging/intermediate shard and six in the marts shard; three further verification queries were run in the revision round.

---

## Business context

**Intended purpose.** The unit answers five classes of questions for the public analytics dashboard, the Cerebro REST API (routes built live from dbt tags), the MetricFlow MCP semantic layer, and internal engineering teams monitoring node diversity and fork upgrade adoption:

1. How many Discv4 (execution-layer) and Discv5 (consensus-layer) nodes are live today, and how has that count changed week-over-week? (`api_p2p_clients_latest`)
2. Which consensus clients dominate the network, broken down by day, platform, ISP, and country? (`api_p2p_discv4/5_clients_daily`, `api_p2p_discv4/5_clients_latest`)
3. What is the crawl success rate and number of distinct crawl sessions per day per protocol? (`api_p2p_visits_latest`)
4. Which consensus fork are peers running, and which fork are they signalling readiness for? (`api_p2p_discv5_current_fork_daily`, `api_p2p_discv5_next_fork_daily`)
5. What is the geographic and organizational peer-to-peer topology of the network? (`api_p2p_topology_latest`)

**Canonical definitions.**

- **Peer (Discv4):** Network node reachable via the Kademlia DHT, filtered to `network_id = 100` (Gnosis Chain). Source: `nebula_discv4.visits`.
- **Peer (Discv5):** Node identified by ENR, filtered to `fork_digest IN (known Gnosis set) OR next_fork_version LIKE '%064'` (Gnosis chain suffix 0x64=100). Source: `nebula.visits`.
- **Successful visit (visits layer, OR):** `empty(dial_errors) = 1 OR crawl_error IS NULL`. Used exclusively in `int_p2p_discv4/5_visits_daily` for the crawl-health ratio. This is an intentional permissive definition: any visit that did not result in a complete dial failure.
- **Reachable peer (peer/client/fork layer, AND):** `empty(dial_errors) = 1 AND crawl_error IS NULL`. The stricter filter used in `int_p2p_discv4/5_clients_daily` and `int_p2p_discv5_forks_daily` to determine which peers contribute to client/fork diversity counts.
- **Note:** The OR and AND definitions are semantically coherent across their respective contexts but are undocumented, creating apparent inconsistency for consumers comparing `pct_successful` against client counts on the same day.
- **Client:** Leading slash-delimited token of `agent_version`; empty string coerced to `'Unknown'`.
- **generic_provider:** ISP bucket derived by keyword-matching the `ip_org` field from `crawlers_data.ipinfo` into 24 named buckets (AWS, Google, Azure, Hetzner, etc.).
- **cl_fork_name:** Human-readable consensus fork name mapped from `fork_digest` in peer ENR properties. Hardcoded through Fulu (0x3237dab6) in two SQL files.
- **Topology edge:** COUNT of neighbor-response edges between a peer's and neighbor's `discovery_id_prefix` observed in a single crawl day, restricted to peers appearing in yesterday's Gnosis peer set.

**Contract context.** No smart contracts or on-chain addresses are referenced. All data is off-chain crawler telemetry. Sources are declared `meta.authoritative: false`. Freshness SLA: nebula schemas warn at 36 h / error at 72 h; ipinfo warns at 18 h / error at 30 h.

---

## Implementation assessment

### Critical

**`int_p2p_discv4_visits_daily`: `successful_visits` is always equal to `total_visits` — metric is vacuous**
`models/p2p/intermediate/int_p2p_discv4_visits_daily.sql`, `models/p2p/marts/api_p2p_visits_latest.sql`

The `successful_visits` aggregate uses `SUM(IF(empty(dial_errors) = 1 OR crawl_error IS NULL, 1, 0))`. Because `crawl_error` defaults to NULL for all rows in the discv4 source — including failed visits — the OR condition matches every row. Warehouse confirmation: `sum(successful_visits) = sum(total_visits) = 9,786` for the last 14 days, giving `pct_successful = 100%` at all times. The discv5 equivalent produces ~19% which is plausible; the discv4 result carries zero information and is actively misleading for any consumer of `api_p2p_visits_latest`. While the OR/AND split is intentional across semantic contexts, the discv4 OR result is structurally indistinguishable from a broken metric. Either `crawl_error` must be populated for discv4 failures at source, the metric must switch to AND, or `discv4_pct_successful` must be suppressed from the mart output until the source is fixed.

---

### High

**`int_p2p_discv4_peers`: missing `join_use_nulls` pre_hook — empty-string geo fields pollute client distributions**
`models/p2p/intermediate/int_p2p_discv4_peers.sql`, `models/p2p/intermediate/int_p2p_discv4_clients_daily.sql`

The pre_hook sets only `allow_experimental_json_type`; `join_use_nulls = 1` is absent. Under ClickHouse defaults, unmatched LEFT JOIN rows return the column type's default value (`''` for String) rather than NULL. Warehouse confirmation: 2,293 of 4,303 recent rows (53%) have `peer_country = ''` and `generic_provider = ''`. These empty strings propagate into `api_p2p_discv4_clients_daily` as an explicit empty-label bucket in the Country and Provider metrics, distorting the geographic and ISP distribution of discv4 peers. The discv5 equivalent model (`int_p2p_discv5_peers`) correctly sets `join_use_nulls`. Fix: add `SET join_use_nulls = 1` to the pre_hook and `SET join_use_nulls = 0` to the post_hook, matching the discv5 pattern.

**Topology intermediates missing `join_use_nulls` — 65% of discv4 edges and 54% of discv5 edges silently dropped**
`models/p2p/intermediate/int_p2p_discv4_topology_latest.sql`, `models/p2p/intermediate/int_p2p_discv5_topology_latest.sql`, `models/p2p/marts/api_p2p_topology_latest.sql`

Both topology intermediate tables LEFT JOIN `ipinfo` twice (peer and neighbor) without `join_use_nulls`. Warehouse confirmation: 100% of unmatched rows in both tables carry empty-string `loc` (not NULL) — 43 empty-string rows in discv4 (69 total), 1,143 in discv5 (2,875 total). `fct_p2p_topology_latest` parses lat/lon via `toFloat64OrNull('')` → NULL; `api_p2p_topology_latest` then filters `WHERE peer_lat IS NOT NULL AND neighbor_lat IS NOT NULL`, silently removing 45/69 discv4 rows (65%) and 1,559/2,875 discv5 rows (54%). The topology map served to consumers represents a minority of the known peer graph with no caveat. Root cause is in the topology intermediates' own LEFT JOINs; upstream peers models are not the source (confirmed by round-2 verification queries). Fix: add `join_use_nulls = 1` pre_hook to both topology intermediates.

**`api_p2p_clients_latest` and `api_p2p_visits_latest`: schema.yml documents columns absent from the final SELECT**
`models/p2p/marts/api_p2p_clients_latest.sql`, `models/p2p/marts/api_p2p_visits_latest.sql`, `models/p2p/marts/schema.yml`

`api_p2p_clients_latest` schema.yml declares `date` with a `not_null` test; the final SELECT (CROSS JOIN of four scalar CTEs) outputs only `discv4_count`, `change_discv4_pct`, `discv5_count`, `change_discv5_pct` — no `date` column. `api_p2p_visits_latest` schema.yml documents `date` and `crawls` columns that likewise do not appear in the final SELECT. dbt tests on these phantom columns fire false results and MCP metadata exposes wrong column schemas to API consumers.

**`int_p2p_discv5_forks_daily` schema.yml documents phantom `peer_id` and `next_fork` columns**
`models/p2p/intermediate/schema.yml`

The intermediate schema.yml for `int_p2p_discv5_forks_daily` describes columns `peer_id`, `fork`, `next_fork` with `unique` and `not_null` tests on `peer_id`. The actual warehouse output is `(date, label, fork, cnt)` — no `peer_id` or `next_fork`. Tests on `peer_id` exercise a non-existent column.

**`int_p2p_discv4/5_clients_daily` schema.yml documents CTE columns, not final SELECT output**
`models/p2p/intermediate/schema.yml`

Both clients_daily schema entries list `peer_id`, `client`, `platform`, `generic_provider`, `peer_country` with `unique`/`not_null` tests. The actual final output is `(date, metric, label, value)` after the UNION ALL pivoting step. The schema documents the intermediate `peers` CTE, not the model output. All per-column tests on documented columns are invalid.

**`stg_nebula_discv4/5__visits` and neighbors schema.yml: `unique` tests on `crawl_id` and `peer_id` are wrong**
`models/p2p/staging/schema.yml`

A single crawl produces thousands of visit rows and neighbor edges, so neither `crawl_id` nor `peer_id` is unique in these views. These tests always fail on current data, generating noise that masks real test failures. Correct candidate keys: `(crawl_id, peer_id)` for visits; `(crawl_id, peer_discovery_id_prefix, neighbor_discovery_id_prefix)` for neighbors.

---

### Medium

**CROSS JOIN in `api_p2p_clients_latest` and `api_p2p_visits_latest` returns 0 rows on any crawl gap**
Both views join four scalar CTEs. If any subquery returns empty — for example, no crawl data on the -7d anchor date — the view silently returns zero rows with no warning. A LEFT JOIN with `COALESCE(prev_count, 0)` fallback would be resilient.

**`int_p2p_discv4_peers` schema.yml: column-level `unique` test on `peer_id` contradicts composite key**
`models/p2p/intermediate/schema.yml`
The column-level unique test on `peer_id` alone contradicts the table-level `unique_combination_of_columns: [visit_ended_at, peer_id]` test, which correctly reflects the actual grain. The column-level test will always fail for any peer seen across multiple days.

**`fct_p2p_discv5_forks_daily` has no `config()` block — materializes as project default with no tags**
`models/p2p/marts/fct_p2p_discv5_forks_daily.sql`
No `config()` block, no engine, no partition settings, no `api:`/`tier:` tags. This model is the source for `api_p2p_discv5_current_fork_daily` and `api_p2p_discv5_next_fork_daily`; view-level materialization may be inadequate for production query performance and the model bypasses the CI tag guard.

**`any()` instead of `argMax()` for fork name selection in `int_p2p_discv5_forks_daily`**
`models/p2p/intermediate/int_p2p_discv5_forks_daily.sql`
`toString(any(cl_fork_name)) AS fork` per `(date, peer_id)` is non-deterministic in ClickHouse. For a peer with multiple visits on the same day (e.g., a fork transition day), the selected fork name is arbitrary. `argMax(cl_fork_name, visit_ended_at)` would be deterministic and select the latest observed value.

**Asymmetric incremental lookback: discv4 uses 1 day, discv5 uses 3 days without documentation**
`models/p2p/intermediate/int_p2p_discv4_clients_daily.sql`, `models/p2p/intermediate/int_p2p_discv4_visits_daily.sql`
discv4 intermediates use the default `lookback_days=1`; discv5 equivalents explicitly pass `lookback_days=3`. Under current `insert_overwrite` strategy the whole-month partition is re-read regardless, but the inconsistency is undocumented and would leave discv4 with a gap for late-arriving records if the strategy were ever changed.

**Topology intermediates: `materialized=table` with hardcoded `today()-1` — no resilience to missed runs**
`models/p2p/intermediate/int_p2p_discv5_topology_latest.sql`, `models/p2p/intermediate/int_p2p_discv4_topology_latest.sql`
Both topology tables are rebuilt as full tables filtered to exactly `today() - INTERVAL 1 DAY`. A single missed dbt run leaves the network graph map showing stale data with no fallback window and no indication of staleness.

---

### Low

**Staging visits models omit `materialized` key — inconsistent with siblings**
`models/p2p/staging/stg_nebula_discv5__visits.sql`, `models/p2p/staging/stg_nebula_discv4__visits.sql`
The visits staging models omit the `materialized` key; neighbors and discovery_id models explicitly set `materialized='view'`. The visits models inherit the project default and would be silently affected by any project-level change.

**Fork digest lookup table duplicated verbatim in two SQL files**
`models/p2p/intermediate/int_p2p_discv5_peers.sql`, `models/p2p/intermediate/int_p2p_discv5_visits_daily.sql`
The hardcoded Phase0-through-Fulu fork digest CTE is copy-pasted identically in both files. Any post-Fulu fork requires a synchronized two-file update with no safeguard against divergence.

**All `api_` endpoints missing `window:` tag required by four-tag convention**
All nine `api_p2p_*` models omit the `window:` tag. CI does not currently enforce `window:` (only `api:`, `granularity:`, `tier:`), so this is a convention gap rather than a hard failure, but it blocks semantic model promotion.

---

## Business-logic assessment

### Critical

**discv4 `pct_successful` in `api_p2p_visits_latest` is always 100% — metric actively misleads consumers**
`models/p2p/intermediate/int_p2p_discv4_visits_daily.sql`, `models/p2p/marts/api_p2p_visits_latest.sql`
Permanently 100% success rate misrepresents network health and will undermine consumer trust if investigated. This is the most damaging business logic issue in the unit because it does not merely omit data — it replaces a meaningful signal with a constant. Suppression or a source-level fix is required before the next external publish.

---

### High

**discv4 country and provider distributions include a spurious empty-string majority bucket**
`models/p2p/intermediate/int_p2p_discv4_peers.sql`, `models/p2p/marts/api_p2p_discv4_clients_daily.sql`
The 53% empty-string geo rate (caused by missing `join_use_nulls`) flows into `api_p2p_discv4_clients_daily` as an explicit unlabelled bucket, making the apparent geographic and ISP distribution of discv4 peers misleading. This affects public dashboards and quarterly geographic reporting.

**Topology map silently represents only 35% of discv4 and 46% of discv5 known edges**
`models/p2p/marts/api_p2p_topology_latest.sql`
The network topology visualization served to the public analytics dashboard represents a minority of the known peer graph, biased toward geo-enriched peers (likely large cloud providers). No caveat documents this loss. The fix (`join_use_nulls` in topology intermediates) would recover the dropped edges if ipinfo matches exist; ungeolocated edges would become NULL-coordinate rows that could be filtered or rendered differently.

**discv5 geo enrichment is structurally limited to 6.9% of visits — geographic data is not representative**
`models/p2p/staging/stg_nebula_discv5__visits.sql`, `models/p2p/intermediate/int_p2p_discv5_peers.sql`, `models/p2p/marts/api_p2p_discv5_clients_daily.sql`
Round-2 verification: 52,419,739 of 56,316,889 rows (93%) in `stg_nebula_discv5__visits` over the last 7 days have `connect_maddr` NULL or empty. Only 3,897,148 rows (6.9%) carry a `/ip4/` address; zero `/dns4/` and two `/ip6/` rows. The staging inspector's hypothesis that the regex fails on non-IPv4 multiaddrs was rebutted — there are no such rows to recover. The 91% geo-null rate at the `int_p2p_discv5_peers` level is a source-level limitation of the Nebula discv5 crawler, not a code defect. However, this limitation is undocumented in schema.yml or the docs site, meaning consumers drawing conclusions about discv5 node geography are working with an unrepresentative 7% sample.

**OR vs AND split is intentional but undocumented — `pct_successful` and client counts are incomparable**
`models/p2p/intermediate/int_p2p_discv4_visits_daily.sql`, `models/p2p/intermediate/int_p2p_discv5_visits_daily.sql`, `models/p2p/marts/api_p2p_visits_latest.sql`
The visits layer uses the permissive OR definition; the client/fork layer uses the stricter AND. Both are correct for their context, but the split is not documented anywhere. A consumer comparing `pct_successful` in `api_p2p_visits_latest` against the client count trend in `api_p2p_discv4/5_clients_daily` will observe apparent inconsistency that is actually a definitional gap. This must be documented in schema.yml and the public docs for both protocol variants.

**cerebro-docs Key Models Reference documents four models that never existed**
`models/p2p/marts/`
`cerebro-docs/docs/models/p2p.md` Key Models Reference table and all associated query examples reference `api_p2p_network_size_daily`, `api_p2p_nodes_by_client_daily`, `api_p2p_nodes_by_country_daily`, and `api_p2p_discovered_nodes_daily`. Confirmed via `git log -S` across all commits: none of these models have ever existed in dbt-cerebro. They were introduced as placeholder documentation at docs creation time (2026-03-11) without reference to the actual model names. Any consumer following the docs site query examples receives a table-not-found error.

---

### Medium

**Duplicate semantic model question_synonyms create ambiguous MCP routing**
`semantic/authoring/p2p/semantic_models.yml`
Both `p2p_discv4_clients_daily` (mart, dimensions: `date/metric/label`) and `int_p2p_discv4_clients_daily` (intermediate, additional dimensions: `peer_id/client/platform/generic_provider/peer_country`) share the synonym `'p2p discv4 clients daily'`. Same collision for discv5 and for `forks_daily`. The dual registration is structurally intentional (mart = post-aggregated; intermediate = pre-aggregated peer grain for finer queries), but the synonym collision is real and unresolved. The MCP router has no deterministic way to select the correct model.

**discv4 topology contains only 24 edges after geo filter — map is too thin to be representative**
`models/p2p/marts/api_p2p_topology_latest.sql`, `models/p2p/intermediate/int_p2p_discv4_topology_latest.sql`
Even after fixing `join_use_nulls`, `int_p2p_discv4_topology_latest` has only 69 total rows for a day's snapshot. With discv4 peer counts in the hundreds, 24 geo-matched edges is not a representative topology sample for the discv4 network. Whether this reflects a crawler coverage gap for discv4 topology specifically should be investigated.

---

### Low

**Gnosis discv5 filter via `next_fork_version LIKE '%064'` may over-include non-Gnosis nodes**
The suffix `%064` is intended to match Gnosis Chain (0x64 = 100). If any other Ethereum chain uses a fork version ending in `064`, those peers would be incorrectly included. Low-probability risk but should be validated against fork version registries.

**Post-Fulu fork digests will be silently excluded from discv5 metrics**
After Fulu activates, peers reporting an unrecognized fork_digest will not match any known digest. Their `cl_fork_name` will be NULL and they may be silently excluded from fork-based counts. No alerting exists for unrecognized fork digests.

---

## Data findings

All queries were run by the inspector agents. Key confirmed numbers:

| Finding | Value | Source |
|---|---|---|
| discv4 visits_daily — successful = total (OR vacuous) | 9,786 / 9,786 (100%) | `int_p2p_discv4_visits_daily`, last 14 days |
| discv4 peers — empty-string geo rate | 2,293 / 4,303 (53%) | `int_p2p_discv4_peers`, last 7 days |
| discv5 peers — NULL geo rate | 471,279 / 515,579 (91%) | `int_p2p_discv5_peers`, last 7 days |
| discv5 source connect_maddr null/empty | 52.4M / 56.3M (93%) | `stg_nebula_discv5__visits`, last 7 days |
| discv4 topology — empty-string loc rows | 43 / 69 (62%) | `int_p2p_discv4_topology_latest` |
| discv5 topology — empty-string loc rows | 1,143 / 2,875 (40%) | `int_p2p_discv5_topology_latest` |
| discv4 topology edges surviving geo filter | 24 / 69 (35%) | `fct_p2p_topology_latest` |
| discv5 topology edges surviving geo filter | 1,316 / 2,875 (46%) | `fct_p2p_topology_latest` |
| discv5 visits_daily success rate (OR) | ~19% over 30 days | `int_p2p_discv5_visits_daily` |
| discv4 clients_daily max date | 2026-06-10 | `int_p2p_discv4_clients_daily` |
| discv5 clients_daily max date | 2026-06-10 | `int_p2p_discv5_clients_daily` |
| discv4 history start | 2025-07-06 | `int_p2p_discv4_clients_daily` |
| discv5 history start | 2025-05-03 | `int_p2p_discv5_clients_daily` |

Round-1 staging shard reported max_date 2026-06-08 (3 days stale). Round-2 verification confirmed both `int_p2p_discv4_visits_daily` and `int_p2p_discv4_clients_daily` share max_date 2026-06-10 — the earlier staleness was a timing artifact from when the shard ran, not a persistent pipeline gap.

---

## Pros / Cons

**Pros**

- Full end-to-end coverage of both Discv4 and Discv5 with consistent schema patterns and shared staging conventions.
- `insert_overwrite` monthly partitions with the `apply_monthly_incremental_filter` macro provide correct partition-replacement semantics for ClickHouse, avoiding double-counting on reruns.
- `join_use_nulls` is correctly applied in `int_p2p_discv5_peers` and the discv5 topology model, establishing the correct pattern; discv4 is the gap, not the default.
- Fork digest-to-name mapping covers all known Gnosis CL forks through Fulu with the correct Gnosis-specific digests (chain suffix 0x64).
- The OR/AND dual-definition for successful visits vs reachable peer is semantically coherent once the two contexts are understood.
- The metric/label unpivot pattern in `clients_daily` produces a normalized long-format output suitable for multi-dimensional slicing across client, platform, provider, and country in a single mart.
- The semantic layer correctly registers both post-aggregated mart grain and pre-aggregated intermediate grain, enabling summary and peer-level queries through MCP without raw SQL exposure.
- Source freshness SLAs (warn 36 h / error 72 h for nebula; warn 18 h / error 30 h for ipinfo) are declared and enforceable.

**Cons**

- discv4 `pct_successful` is always 100% by construction — the OR condition is vacuous because `crawl_error` defaults to NULL for all discv4 rows. The metric is the most visible KPI in `api_p2p_visits_latest` and actively misleads consumers.
- Missing `join_use_nulls` in `int_p2p_discv4_peers` causes 53% of rows to carry empty-string geo fields, polluting the client distribution with an unlabelled majority bucket.
- Missing `join_use_nulls` in both topology intermediates silently drops 65% of discv4 edges and 54% of discv5 edges from the topology map, biasing the visualization toward a non-representative subset.
- schema.yml phantom columns across multiple models mean dbt tests are validating non-existent columns and MCP metadata is wrong for at least four mart/intermediate models.
- Duplicate question_synonyms create genuine MCP routing ambiguity across three model pairs.
- Fork digest lookup is hardcoded identically in two SQL files with no centralization — post-Fulu requires a synchronized two-file update with no safeguard.
- Topology intermediates use `materialized=table` with hardcoded `today()-1` with no resilience to missed runs and no history.
- discv5 geo enrichment is structurally limited to 6.9% of visits due to null `connect_maddr` at source — geographic distributions are severely incomplete but this is undocumented.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| IMMEDIATE | Suppress or fix `discv4_pct_successful` in `api_p2p_visits_latest`: either populate `crawl_error` at source for discv4 failures, switch the OR condition to AND, or mark the column NULL/absent until resolved. A 100% health metric is more harmful than a missing one. | `int_p2p_discv4_visits_daily.sql`, `api_p2p_visits_latest.sql` |
| IMMEDIATE | Add `SET join_use_nulls = 1` pre_hook (and reset post_hook) to `int_p2p_discv4_peers`, `int_p2p_discv4_topology_latest`, and `int_p2p_discv5_topology_latest`, matching the pattern in `int_p2p_discv5_peers`. This single class of fix resolves the empty-string geo bucket in discv4 distributions and restores the dropped topology edges. | `int_p2p_discv4_peers.sql`, `int_p2p_discv4_topology_latest.sql`, `int_p2p_discv5_topology_latest.sql` |
| HIGH | Fix schema.yml for `api_p2p_clients_latest`, `api_p2p_visits_latest`, `int_p2p_discv5_forks_daily`, and both `clients_daily` models to document actual output columns. Remove invalid unique/not_null tests on phantom columns. Add correct composite key tests where warranted. Also fix the staging visits/neighbors unique tests to use the correct composite candidate keys. | `models/p2p/marts/schema.yml`, `models/p2p/intermediate/schema.yml`, `models/p2p/staging/schema.yml` |
| HIGH | Update `cerebro-docs/docs/models/p2p.md` Key Models Reference section and all query examples to remove the four non-existent model names and replace with the actual models (`api_p2p_discv4/5_clients_daily`, `api_p2p_visits_latest`) with their real column schemas (`date, metric, label, value`). | cerebro-docs/docs/models/p2p.md |
| HIGH | Differentiate semantic model question_synonyms: add a qualifier to intermediate model synonyms (e.g., `'p2p discv4 clients daily peer grain'`) without removing them. Designate the mart models as canonical entry points for standard daily queries. | `semantic/authoring/p2p/semantic_models.yml` |
| MEDIUM | Add a prominent data quality caveat to schema.yml and the docs site for discv5 geo coverage: geographic and ISP fields are populated for approximately 7% of discv5 visits due to null/empty `connect_maddr` in the nebula source. Coordinate with the Nebula crawler team on whether TCP multiaddr recording can be enabled for discv5. | `int_p2p_discv5_peers.sql`, `api_p2p_discv5_clients_daily.sql`, docs |
| MEDIUM | Document the OR/AND definitional split in schema.yml and the public docs: `successful_visits` uses OR (crawl health ratio — any non-total-failure), while client/fork peer counts use AND (fully reachable peer). The two metrics are not directly comparable on the same day. | `int_p2p_discv4/5_visits_daily.sql` schema.yml descriptions |
| MEDIUM | Add a `config()` block to `fct_p2p_discv5_forks_daily` with explicit materialization, engine, partition key, and `api:/granularity:/window:/tier` tags. | `fct_p2p_discv5_forks_daily.sql` |
| MEDIUM | Replace `any(cl_fork_name)` with `argMax(cl_fork_name, visit_ended_at)` in `int_p2p_discv5_forks_daily` to make fork name selection deterministic. | `int_p2p_discv5_forks_daily.sql` |
| LOW | Extract the fork digest-to-name and next_fork_version-to-name mappings to a dbt seed file or vars block and reference it via a macro from both `int_p2p_discv5_peers.sql` and `int_p2p_discv5_visits_daily.sql`. Add a dbt test or CI check that warns when an unrecognized fork_digest appears in source data. | `int_p2p_discv5_peers.sql`, `int_p2p_discv5_visits_daily.sql` |
| LOW | Add `window:` tags to all nine `api_p2p_*` models (`window:latest` for snapshot endpoints, `window:all_time` for full daily series) to complete the four-tag convention. Update `check_api_tags.py` to enforce `window:`. | All `api_p2p_*` mart models |
| LOW | Explicitly declare `materialized='view'` in the config block of `stg_nebula_discv4/5__visits.sql` to match the convention used in the neighbors and discovery_id staging models. | `stg_nebula_discv5__visits.sql`, `stg_nebula_discv4__visits.sql` |

---

## Open disagreements

None. All three round-2 challenges converged on direct warehouse evidence:

- The staging inspector's freshness divergence finding (visits_daily max 2026-06-08 vs clients_daily max 2026-06-10) was a timing artifact — both tables share max_date 2026-06-10 per a simultaneous query.
- The staging inspector's hypothesis that 91% discv5 geo nulls stem from regex failures on `/dns4/` and `/ip6/` multiaddrs was rebutted: there are zero `/dns4/` rows and two `/ip6/` rows in the source; the root cause is null/empty `connect_maddr` in 93% of source visits.
- The marts inspector's topology NULL attribution was confirmed and more precisely located: empty-string `loc` fields (not NULLs) originate in the topology intermediates' own LEFT JOINs, not upstream from the peers models.

---

## Review log

| Round | Agent | Challenge | Outcome |
|---|---|---|---|
| 2 | Revision | Staging shard reported freshness divergence between `int_p2p_discv4_visits_daily` (2026-06-08) and `int_p2p_discv4_clients_daily` (2026-06-10) | Rebutted: simultaneous query showed both tables at max_date 2026-06-10; timing artifact |
| 2 | Revision | Staging shard attributed topology NULL root cause to upstream peers model NULL propagation | Confirmed and refined: topology intermediates independently re-join ipinfo and produce empty strings regardless of upstream; 100% of unmatched topology rows carry `''` not NULL |
| 2 | Revision | Staging shard hypothesized discv5 91% geo-null rate caused by regex failing on `/dns4/` and `/ip6/` multiaddrs | Rebutted: source query showed 0 `/dns4/` rows, 2 `/ip6/` rows, 52.4M null/empty out of 56.3M total; null/empty `connect_maddr` at source is the sole cause |
