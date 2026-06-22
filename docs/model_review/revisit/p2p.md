# Model review (revisit 2026-06-21): p2p

Baseline `docs/model_review/p2p.md` (dated `2026-06-11`); `26` cases re-verified over `3` rounds. Headline: `0` resolved by code fix, `1` resolved as a false finding (`P2P-C16`), `1` changed (`P2P-C21`, severity held at high after measuring both protocols), and `24` still confirmed — every standing design/doc defect reproduces against live dbt today, with `0` incident attributions (these are standing defects, not June-2026-incident regressions).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| P2P-C01 | — | discv4 `successful_visits` always == `total_visits` (vacuous OR), `pct_successful` permanently 100% | critical | CONFIRMED | critical | high | none | 3 |
| P2P-C02 | — | `int_p2p_discv4_peers` missing `join_use_nulls`; unmatched geo returns `''` not NULL | high | CONFIRMED | high | high | none | 3 |
| P2P-C03 | — | Both topology intermediates LEFT JOIN ipinfo twice w/o `join_use_nulls`; ~half of edges silently dropped | high | CONFIRMED | high | high | none | 3 |
| P2P-C04 | — | `api_p2p_clients_latest`/`visits_latest` schema.yml documents/tests absent `date`/`crawls` columns | high | CONFIRMED | high | high | none | 3 |
| P2P-C05 | — | `int_p2p_discv5_forks_daily` schema.yml tests phantom `peer_id`/`next_fork` | high | CONFIRMED | high | high | none | 3 |
| P2P-C06 | — | `int_p2p_discv4/5_clients_daily` schema.yml tests pre-pivot CTE columns (`peer_id` etc.) | high | CONFIRMED | high | high | none | 3 |
| P2P-C07 | — | staging `visits`/`neighbors` single-column unique on `crawl_id`/`peer_id` (wrong grain) | high | CONFIRMED | high | high | none | 3 |
| P2P-C08 | — | mart 4-way CROSS JOIN of scalar CTEs returns 0 rows on any crawl gap | medium | CONFIRMED | medium | medium | none | 3 |
| P2P-C09 | — | `int_p2p_discv4_peers` column-level unique on `peer_id` contradicts composite unique | medium | CONFIRMED | medium | high | none | 3 |
| P2P-C10 | — | `fct_p2p_discv5_forks_daily` has no `config()` block; view, no tags, bypasses CI guard | medium | CONFIRMED | medium | high | none | 3 |
| P2P-C11 | — | `int_p2p_discv5_forks_daily` uses `any(cl_fork_name)` (non-deterministic) not `argMax` | medium | CONFIRMED | medium | high | none | 3 |
| P2P-C12 | — | Asymmetric incremental lookback: discv4 default 1 vs discv5 explicit 3, undocumented | medium | CONFIRMED | medium | high | none | 3 |
| P2P-C13 | — | Topology intermediates `table` filtered to `today()-1` with no fallback window | medium | CONFIRMED | medium | medium | none | 3 |
| P2P-C14 | — | staging `visits` models omit `materialized` key (siblings set `view`) | low | CONFIRMED | low | high | none | 3 |
| P2P-C15 | — | Fork-digest CTE copy-pasted in two discv5 files, no shared seed/macro | low | CONFIRMED | low | high | none | 3 |
| P2P-C16 | — | All nine `api_p2p_*` marts omit `window:` tag (alleged convention gap) | low | RESOLVED | resolved | high | none | 3 |
| P2P-C17 | — | discv4 `pct_successful` in `api_p2p_visits_latest` always 100% (business-logic) | critical | CONFIRMED | critical | high | none | 3 |
| P2P-C18 | — | discv4 country/provider has spurious `''` majority bucket (53% empty geo) | high | CONFIRMED | high | high | none | 3 |
| P2P-C19 | — | Public topology map shows only ~half of known edges, biased to cloud peers, no caveat | high | CONFIRMED | high | high | none | 3 |
| P2P-C20 | — | discv5 geo enrichment capped at ~7% of visits (source limitation), undocumented | high | CONFIRMED | high | high | none | 3 |
| P2P-C21 | — | OR (visits) vs AND (reachable) definitional split intentional but undocumented | high | CHANGED | high | high | none | 3 |
| P2P-C22 | — | docs Key Models Reference documents four models that never existed | high | CONFIRMED | high | high | none | 3 |
| P2P-C23 | — | Duplicate `question_synonyms` across mart/intermediate pairs → ambiguous MCP routing | medium | CONFIRMED | medium | medium | none | 3 |
| P2P-C24 | — | discv4 topology only ~24-49 edges/day — too thin to be representative | medium | CONFIRMED | medium | high | none | 3 |
| P2P-C25 | — | Gnosis discv5 filter `next_fork_version LIKE '%064'` may over-include non-Gnosis nodes | low | CONFIRMED | low | high | none | 3 |
| P2P-C26 | — | Post-Fulu fork digests silently excluded; no unrecognized-digest alert | low | CONFIRMED | low | high | none | 3 |

Final tally: `24` CONFIRMED / `1` RESOLVED / `1` CHANGED / `0` NEW / `0` unverifiable.

## Delta vs baseline

### RESOLVED (1)
- `P2P-C16` — RESOLVED as a **false finding**, not a code fix. The premise (`window:` is part of a required four-tag CI convention) is wrong. `scripts/checks/check_api_tags.py` enforces only: `api:` resource grain/window-free, exactly one `granularity:` tag, a `tier{0|1|2}` tag, typed column schema, and a granularity-aware freshness column. `window:` is not in the enforced set, and `latest` is in `POINT_GRANS` (line 27) so latest-snapshot endpoints are legitimately point-in-time. Running `python3 scripts/checks/check_api_tags.py` prints `API tag/schema convention OK` and **passes for all nine `api_p2p_*` models** despite none carrying a `window:` tag. Incident: none.

### CHANGED (1)
- `P2P-C21` — status CHANGED (citation/scope refined), **severity held at `high`** (not downgraded to medium). The OR-vs-AND definitional split is real and undocumented in both protocols. Round 2 measured discv5 (`OR=16.9%` vs `AND=11.9%`, ~`5pp` — moderate) and proposed a downgrade; round 3 measured discv4 and found the gap is **sharp**: `OR=100.0%` vs `AND=76.0%` on the latest day (`2026-06-20`), a `24pp` gap, because the OR pct is pinned at 100%. The consumer-visible inconsistency where it matters (discv4 `pct_successful`) is large, so the blanket downgrade was rejected. Incident: none.

### STILL CONFIRMED (24)
join_use_nulls geo-pollution cluster:
- `P2P-C02` — `int_p2p_discv4_peers` last 7d: `3,416 / 7,059` (`48.4%`) empty `peer_country`/`generic_provider`, `0` NULL; all empties are ipinfo non-matches. Sibling `int_p2p_discv5_peers` sets `join_use_nulls=1` and yields NULL not `''`. Incident: none.
- `P2P-C03` — `fct_p2p_topology_latest`: discv5 survivors `889 / 2,107` (`42%`, `1,218` dropped), discv4 `49 / 90` (`54%`, `41` dropped). Drop split (discv5): `both_null=205`, `peer_only_null=725`, `neighbor_only_null=288` — both join legs contribute. Incident: none.
- `P2P-C18` — `api_p2p_discv4_clients_daily` Country metric: `''` is the **top label every one of 14 days**, value-weighted share `35.6%`–`41.0%` (latest day `18/45 = 40.0%`, above `US 22.2%`). Incident: none.
- `P2P-C19` — Served topology represents only `54%` (discv4) / `42%` (discv5) of known edges; survivors skew to cloud orgs (Hetzner AS24940=`272`, OVH AS16276=`159`, IONOS, netcup); `models/p2p/marts/schema.yml` (lines 446-447) carries no coverage/geo caveat. Incident: none.

vacuous-success / definitional split cluster:
- `P2P-C01` — `int_p2p_discv4_visits_daily` over a **30/30-day** contiguous window: `gap = successful_visits - total_visits = 0` every day. Source line `int_p2p_discv4_visits_daily.sql:18` `SUM(IF(empty(dial_errors)=1 OR crawl_error IS NULL,1,0))` unchanged. Root cause refined: the OR is vacuous because `empty(dial_errors)=1` is always true for discv4 (`3654/3654`), not because `crawl_error` is always NULL (`965/3654` are NOT NULL). Incident: none.
- `P2P-C17` — `api_p2p_visits_latest` serves `discv4_pct_successful=100.0` and `discv5_pct_successful=16.9` on the same row; mart is api-tagged (`api:visits_per_protocol`, `tier0`) and registered as semantic model `p2p_visits_latest` with measure `discv4_pct_successful_value`. Incident: none.

semantic-column-drift (phantom tests) cluster — now artifact-proven from `target/manifest.json`:
- `P2P-C04` — `not_null` test nodes on `api_p2p_clients_latest.date` (uid `79522e2cdc`) and `api_p2p_visits_latest.date` (uid `05d6acdde5`) compiled; neither model emits `date` (or `crawls`). Incident: none.
- `P2P-C05` — `unique` (uid `31fc691e37`) + `not_null` (uid `7e2ec92420`) test nodes on `int_p2p_discv5_forks_daily.peer_id`; output is `(date,label,fork,cnt)`. Incident: none.
- `P2P-C06` — `peer_id` unique+not_null compiled on BOTH `int_p2p_discv4_clients_daily` (`cadec56725`/`373111a0c3`) and `int_p2p_discv5_clients_daily` (`2414407777`/`dbdc25d7e1`); output is `(date,metric,label,value)`. Incident: none.
- `P2P-C09` — `int_p2p_discv4_peers` 7d: `7,059` rows / `79` distinct `peer_id` (`6,980` excess), composite `(visit_ended_at,peer_id)` exactly unique; column-level unique on `peer_id` (schema.yml lines 17-18) coexists with composite (lines 100-105). Incident: none.

wrong-grain unique keys:
- `P2P-C07` — `int_p2p_discv5_peers` latest day: `40` crawls / `97,521` rows = `2,438` rows/crawl. Substitute composite keys: `(crawl_id, peer_id)` for visits, `(crawl_id, peer_discovery_id_prefix, neighbor_discovery_id_prefix)` for neighbors. Incident: none.

config / convention gaps:
- `P2P-C10` — `fct_p2p_discv5_forks_daily` has no `config()` block; `manifest.json` resolves `config.materialized=view`, `tags=[]`. (Baseline lineage claim inverted: it is an orphan mart consumed only by the semantic layer; the two `api_` fork endpoints ref `int_p2p_discv5_forks_daily` directly.) Incident: none.
- `P2P-C14` — `stg_nebula_discv4__visits` and `stg_nebula_discv5__visits` resolve to `materialized=view` (inherited, not explicit); sibling staging models set `view` explicitly. Latent divergence, not current breakage. Incident: none.

other standing defects:
- `P2P-C08` — both marts use 4-way CROSS JOIN of scalar CTEs anchored at `subtractDays(MAX(date),7)`, no COALESCE/LEFT-JOIN fallback. Source currently dense (`8/8` days) so latent-but-plausible. Incident: none.
- `P2P-C11` — `int_p2p_discv5_forks_daily.sql:21` `toString(any(cl_fork_name))`; `48` peer-days (3d) have `>1` distinct `cl_fork_name`, sets are real forks `['Electra','Fulu']` (40-41 visits each) — harmful non-determinism, not NULL-noise. Incident: none.
- `P2P-C12` — discv4 `clients_daily:30`/`visits_daily:25` use default `lookback_days=1`; discv5 explicitly `3`. All four `insert_overwrite` + `toStartOfMonth` so latent; no rationale comment anywhere. Incident: none.
- `P2P-C13` — both topology intermediates `materialized='table'` filtered strictly to `today() - INTERVAL 1 DAY`, no OR fallback / staleness flag; `fct`/`api` are pass-through. Incident: none.
- `P2P-C15` — 7-entry `fork_digests` CTE byte-identical in `int_p2p_discv5_peers.sql` and `int_p2p_discv5_visits_daily.sql`; grep of `macros/`+`seeds/` for `fork_digest`/`Fulu` returns zero — genuine missing abstraction. Incident: none.
- `P2P-C20` — `int_p2p_discv5_peers` 7d: `686,167 / 747,254` (`91.8%`) NULL `peer_country`; `631,336` of `686,167` null-geo rows (`92%`) have no extracted IP, pinning the limitation to the Nebula source feed (no ip-bearing `connect_maddr`) not ipinfo misses. No schema.yml/docs caveat. Incident: none.
- `P2P-C22` — Live cerebro-docs (`/Users/hugser/Documents/Gnosis/repos/cerebro-docs`) `docs/models/p2p.md` ships all four phantom names (`api_p2p_network_size_daily`, `api_p2p_nodes_by_client_daily`, `api_p2p_nodes_by_country_daily`, `api_p2p_discovered_nodes_daily`) — 7 hits (table lines 116-119 + examples 127/136/146) plus `reports.md:262`; none exist in `models/p2p/marts/`. Baseline citation path/date were wrong (real introduction commit `662650a4` on `2026-06-19`, not 2026-03-11) but the live-site harm is real. Incident: none.
- `P2P-C23` — `semantic/authoring/p2p/semantic_models.yml`: identical `question_synonyms` on each mart/intermediate pair (`p2p discv4 clients daily`, `p2p discv5 clients daily`, `p2p discv5 forks daily`); all share `quality_tier=candidate`, both members registered, no tie-break. Incident: none.
- `P2P-C24` — `int_p2p_discv4_topology_latest`: `90` rows / `33` distinct `peer_discovery_id_prefix`; reachable discv4 peers yesterday = `45` distinct `peer_id`. Thinness is genuine coverage (few reachable peers), the further `90→49` drop is the geo-filter artifact of C03. Incident: none.
- `P2P-C25` — `int_p2p_discv5_peers.sql` filters on `(fork_digest IN known) OR next_fork_version LIKE '%064'` with no network_id guard; `matched_only_via_064 = 0` currently (all six `%064` values are Gnosis `0x01000064`..`0x06000064`). Unguarded residual theoretical risk. Incident: none.
- `P2P-C26` — Fork-digest map latest entry `('0x3237dab6','Fulu')`; `null_fork=0`/`null_fork_reachable=0` today (latent). `int_p2p_discv5_forks_daily` has no WHERE/HAVING dropping NULL fork, so a post-Fulu digest surfaces as an unlabelled `''`/NULL bucket (via `toString(any(NULL))`), not silently dropped; no unrecognized-digest alert. Incident: none.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None. The round-2 site-docs unverifiability for `P2P-C22` was resolved in round 3 by locating the live cerebro-docs checkout.

## Evidence appendix

Per case (grouped where queries are shared). Numbers are the round-3 (final) measurements unless noted.

**P2P-C01 / P2P-C17 / P2P-C21 (vacuous discv4 success + OR-vs-AND split)**
- `SELECT date,total_visits,successful_visits,(successful_visits-total_visits) AS gap FROM dbt.int_p2p_discv4_visits_daily WHERE date>=today()-30 ORDER BY date` → `gap=0` on `30/30` days, contiguous through `2026-06-20`.
- Source line `int_p2p_discv4_visits_daily.sql:18`: `SUM(IF(empty(dial_errors)=1 OR crawl_error IS NULL,1,0))` (unchanged). Source proxy `int_p2p_discv4_peers` 3d: `empty(dial_errors)=1` for all `3654`; `crawl_error IS NULL` only `2689/3654`; `genuinely_failed (dial nonempty AND crawl_error nonnull) = 0`.
- `SELECT discv4_pct_successful, discv5_pct_successful FROM dbt.api_p2p_visits_latest` → `100.0` / `16.9` (same row; discv4 `total_visits=967`, discv5 `total_visits=97521`).
- discv4 latest day OR vs AND: `total=967`, OR-success `967` (`100.0%`), AND-success `735` (`76.0%`) → `24pp` gap. discv5: OR `~16.9%` vs AND `~11.9%` → `~5pp`. `api_p2p_visits_latest` is api-tagged (`api:visits_per_protocol`, `tier0`) and registered as semantic model `p2p_visits_latest`.

**P2P-C02 / P2P-C18 (discv4 geo `''` pollution)**
- `SELECT count(),sum(peer_country=''),sum(peer_country IS NULL),sum(generic_provider='') FROM dbt.int_p2p_discv4_peers WHERE toDate(visit_ended_at)>=today()-7` → `7059` rows, `3416` empty `peer_country` (`48.4%`), `0` NULL, `3416` empty `generic_provider`. Pre_hook `int_p2p_discv4_peers.sql:9` only `SET allow_experimental_json_type=1` (no `join_use_nulls`); `int_p2p_discv5_peers.sql:12` sets `join_use_nulls=1` and its `peer_country` is Nullable (`686,167` NULL / `0` empty over 7d).
- `SELECT date, sumIf(value,label='')/sum(value)*100 AS empty_share, argMax(label,value) AS top_label FROM dbt.int_p2p_discv4_clients_daily WHERE metric='Country' AND date>=today()-14 GROUP BY date` → empty-share min `35.6%`, max `41.0%`, avg `~39%`; `top_label=''` all 14 days.

**P2P-C03 / P2P-C19 / P2P-C24 (topology geo-filter drop + bias + thinness)**
- `SELECT protocol, countIf(peer_lat IS NULL AND neighbor_lat IS NULL), countIf(peer_lat IS NULL AND neighbor_lat IS NOT NULL), countIf(peer_lat IS NOT NULL AND neighbor_lat IS NULL), countIf(peer_lat IS NOT NULL AND neighbor_lat IS NOT NULL), count() FROM dbt.fct_p2p_topology_latest GROUP BY protocol` → DiscV5 `both_null=205`, `peer_only_null=725`, `neighbor_only_null=288`, survivors `889/2107` (`42%`); DiscV4 `both_null=22`, `peer_only_null=18`, `neighbor_only_null=1`, survivors `49/90` (`54%`).
- Survivor org skew (DiscV5): Hetzner AS24940=`272`, OVH AS16276=`159`, IONOS, netcup, Allnodes, Alibaba; dropped rows carry `peer_org=''` by construction.
- `models/p2p/marts/schema.yml` lines 446-447 describe the mart generically with no coverage caveat; repo-wide grep for `caveat`/`incomplete`/`coverage`/`limitation` in p2p schema.yml finds nothing.
- `int_p2p_discv4_topology_latest`: `90` rows / `33` distinct `peer_discovery_id_prefix`; reachable discv4 peers yesterday = `45` distinct `peer_id`.

**P2P-C04 / P2P-C05 / P2P-C06 / P2P-C09 (phantom/contradictory tests, manifest-proven)**
- `target/manifest.json`: `not_null` on `api_p2p_clients_latest.date` (`79522e2cdc`) + `api_p2p_visits_latest.date` (`05d6acdde5`); `unique`+`not_null` on `int_p2p_discv5_forks_daily.peer_id` (`31fc691e37`/`7e2ec92420`); `peer_id` unique+not_null on `int_p2p_discv4_clients_daily` (`cadec56725`/`373111a0c3`) and `int_p2p_discv5_clients_daily` (`2414407777`/`dbdc25d7e1`).
- Model outputs (verified by successful aggregate queries against the materialized relations): `api_p2p_clients_latest` = `(discv4_count,change_discv4_pct,discv5_count,change_discv5_pct)`; `api_p2p_visits_latest` has no `date`/`crawls`; `int_p2p_discv5_forks_daily` = `(date,label,fork,cnt)`; both `clients_daily` = `(date,metric,label,value)`.
- `SELECT count(),uniqExact(peer_id) FROM dbt.int_p2p_discv4_peers WHERE toDate(visit_ended_at)>=today()-7` → `7059` rows, `79` distinct `peer_id` (`6980` excess); composite `(visit_ended_at,peer_id)` = `7059` (unique). schema.yml column-level unique at lines 17-18, composite at lines 100-105.

**P2P-C07 (wrong-grain staging unique)**
- `SELECT uniqExact(crawl_id),count(),count()/uniqExact(crawl_id) FROM dbt.int_p2p_discv5_peers WHERE toDate(visit_ended_at)=today()-1` → `40` crawls, `97,521` rows, `2,438` rows/crawl. staging/schema.yml unique tests: `crawl_id`+`peer_id` for visits (lines 13/23, 89/99), `crawl_id` for neighbors (lines 248, 296).

**P2P-C08**
- `SELECT count(DISTINCT date) FROM dbt.int_p2p_discv4_clients_daily WHERE date>=today()-8 AND metric='Clients'` → `8/8` (discv5 same). Both marts: 4-way CROSS JOIN of scalar CTEs, no COALESCE/LEFT JOIN fallback.

**P2P-C10 / P2P-C14 (manifest config)**
- `manifest.json`: `fct_p2p_discv5_forks_daily` `config.materialized=view`, `tags=[]`; `stg_nebula_discv4__visits` and `stg_nebula_discv5__visits` both `materialized=view` (inherited; sibling staging set `view` explicitly).

**P2P-C11**
- `SELECT d,peer_id,arraySort(groupUniqArray(cl_fork_name)) AS fork_set,count() FROM dbt.int_p2p_discv5_peers WHERE toDate(visit_ended_at)>=today()-3 GROUP BY d,peer_id HAVING uniqExact(cl_fork_name)>1` → `48` peer-days; top groups `fork_set=['Electra','Fulu']` with 40-41 visits each. `int_p2p_discv5_forks_daily.sql:21` `toString(any(cl_fork_name))`.

**P2P-C12**
- Code read: discv4 `clients_daily:30`/`visits_daily:25` call `apply_monthly_incremental_filter` with no `lookback_days` (default 1); discv5 `clients_daily:30`/`visits_daily:45` pass `lookback_days=3`. All four `insert_overwrite` + `partition_by=toStartOfMonth`. grep of the four files + schema.yml found no rationale comment.

**P2P-C13**
- Both topology intermediates `materialized='table'` (line 3); filter `WHERE toStartOfDay(...) = today() - INTERVAL 1 DAY` (discv4 lines 19,93; discv5 lines 21,107), no OR fallback. `fct_p2p_topology_latest` max(date)=`2026-06-20`; `api_p2p_topology_latest` is a plain SELECT with no date filter / COALESCE.

**P2P-C15**
- diff of `fork_digests` CTE: byte-identical between `int_p2p_discv5_peers.sql` (lines 24-39) and `int_p2p_discv5_visits_daily.sql` (lines 14-29). grep `macros/`+`seeds/` for `fork_digest`/`Fulu` → 0 hits.

**P2P-C16 (RESOLVED)**
- `python3 scripts/checks/check_api_tags.py` → `API tag/schema convention OK: all production api: endpoints are grain/window-free, have granularity + tier, and complete typed column schemas.` `window:` not in enforced set; `latest` in `POINT_GRANS` (line 27).

**P2P-C20**
- `SELECT count(),sum(peer_country IS NULL),sum(ip IS NULL OR ip=''),sum((ip IS NULL OR ip='') AND peer_country IS NULL),sum(ip IS NOT NULL AND ip!='' AND peer_country IS NULL) FROM dbt.int_p2p_discv5_peers WHERE toDate(visit_ended_at)>=today()-7` → `747,254` rows; `686,167` NULL `peer_country` (`91.8%`); `631,336` no-IP; `631,336` of null-geo lack IP (`92%`); `54,831` have IP but null geo (ipinfo miss). No coverage caveat in any discv5 schema.yml/docs. (Raw `nebula_discv5.visits` privilege-blocked; round-1 measured `93.5%` empty `connect_maddr` before that restriction.)

**P2P-C22**
- grep `cerebro-docs/docs/models/p2p.md` for the four phantom names → 7 hits (table lines 116-119, SQL examples 127/136/146); `reports.md:262` also references `api_p2p_nodes_by_client_daily`. `ls models/p2p/marts/` confirms none exist. `git log -S` shows introduction in commit `662650a4` 'add docs' (`2026-06-19`); `git log --diff-filter=D` shows no deleted model files of these names.

**P2P-C23**
- `semantic/authoring/p2p/semantic_models.yml`: `p2p discv4 clients daily` at lines 59 & 102; `p2p discv5 clients daily` at 325 & 368; `p2p discv5 forks daily` at 441 & 475. Each pair shares the synonym; all `quality_tier=candidate`, no `default_metric`/authoritative discriminator; both members registered.

**P2P-C25**
- `SELECT uniqExact(next_fork_version), groupUniqArrayIf(next_fork_version, next_fork_version LIKE '%064'), countIf(fork_digest NOT IN (known) AND next_fork_version LIKE '%064') FROM dbt.int_p2p_discv5_peers WHERE toDate(visit_ended_at)>=today()-3` → `8` distinct next_fork_version, `6` end in `064` (all Gnosis `0x01000064`..`0x06000064`), `matched_only_via_064=0`. No network_id guard precedes the LIKE.

**P2P-C26**
- `SELECT countIf(cl_fork_name IS NULL), countIf(cl_fork_name IS NULL AND empty(dial_errors)=1 AND crawl_error IS NULL) FROM dbt.int_p2p_discv5_peers WHERE toDate(visit_ended_at)>=today()-3` → `0`/`0`. `int_p2p_discv5_forks_daily` peers CTE has no WHERE/HAVING dropping NULL fork; computes `toString(any(cl_fork_name)) AS fork` then GROUPs — a post-Fulu NULL would surface as an unlabelled bucket. No unrecognized-digest alert/test.

## Review log (>=3 rounds per case)

- **P2P-C01**: R1 CONFIRMED (14d, `gap=0`, sum 12,809) → challenge: prove OR vacuous at source → R2 CONFIRMED (source proxy: `empty(dial_errors)=1` for all 3654, `genuinely_failed=0`; root cause refined) → challenge: 30-day per-day breakdown → R3 CONFIRMED (`30/30` days `gap=0`). Held critical.
- **P2P-C02**: R1 CONFIRMED (`48.4%` empty, no `join_use_nulls`) → challenge: prove ipinfo-non-match mechanism → R2 CONFIRMED (all 3416 empties are ipinfo non-matches) → challenge: prove fix safe via discv5 sibling → R3 CONFIRMED (discv5 yields NULL not `''`). Held high.
- **P2P-C03**: R1 CONFIRMED (discv4 49/90, discv5 889/2107) → challenge: show survivor bias to cloud peers → R2 CONFIRMED (Hetzner/OVH dominate survivors) → challenge: show both join legs drop → R3 CONFIRMED (split 205/725/288). Held high.
- **P2P-C04**: R1 CONFIRMED (schema vs SELECT) → challenge: prove test runs not no-ops → R2 CONFIRMED (SQL-generation reasoning) → challenge: settle from manifest/run_results → R3 CONFIRMED (compiled test nodes in manifest). Held high.
- **P2P-C05**: R1 CONFIRMED (phantom `peer_id`/`next_fork`) → challenge: locate in manifest → R2 CONFIRMED (reasoned) → challenge: confirm compiled test nodes → R3 CONFIRMED (uids `31fc691e37`/`7e2ec92420`). Held high.
- **P2P-C06**: R1 CONFIRMED (both clients_daily) → challenge: compiled artifacts for both → R2 CONFIRMED (reasoned) → challenge: manifest + materialized schema → R3 CONFIRMED (4 uids, output `(date,metric,label,value)`). Held high.
- **P2P-C07**: R1 CONFIRMED (single-column unique in schema) → challenge: prove violation with data → R2 CONFIRMED (`~2,500` rows/crawl) → challenge: corroborate neighbors half → R3 CONFIRMED (`2,438` rows/crawl; neighbors row-per-edge). Held high.
- **P2P-C08**: R1 CONFIRMED (CROSS JOIN, no fallback) → challenge: is gap reachable → R2 CONFIRMED (structural) → challenge: check source density → R3 CONFIRMED (`8/8` dense, latent). Held medium.
- **P2P-C09**: R1 CONFIRMED (column vs composite unique) → challenge: prove with data → R2 CONFIRMED (66 dup peer_ids) → challenge: confirm unchanged at HEAD + no allowlist → R3 CONFIRMED (`79/7059`, no allowlist). Held medium.
- **P2P-C10**: R1 CONFIRMED (no config block) → challenge: confirm view + lineage → R2 CONFIRMED (orphan mart, baseline lineage inverted) → challenge: confirm via manifest (SYSTEM blocked) → R3 CONFIRMED (`materialized=view`, `tags=[]`). Held medium.
- **P2P-C11**: R1 CONFIRMED (`any()`) → challenge: is non-determinism reachable → R2 CONFIRMED (48 peer-days) → challenge: prove conflicting real forks → R3 CONFIRMED (`['Electra','Fulu']`). Held medium.
- **P2P-C12**: R1 CONFIRMED (asymmetry 1 vs 3) → challenge: confirm latent under insert_overwrite → R2 CONFIRMED (all four insert_overwrite+month) → challenge: confirm purely undocumented → R3 CONFIRMED (no rationale comment). Held medium.
- **P2P-C13**: R1 CONFIRMED (`today()-1`, no fallback) → challenge: confirm mart pass-through → R2 CONFIRMED (no max(date) fallback) → challenge: check run cadence/strict filter → R3 CONFIRMED (strict `=today()-1`, cadence config not located). Held medium.
- **P2P-C14**: R1 CONFIRMED (visits omit `materialized`) → challenge: confirm inherited default is view → R2 CONFIRMED (project sets no override) → challenge: confirm via manifest → R3 CONFIRMED (both `materialized=view`). Held low.
- **P2P-C15**: R1 CONFIRMED (duplicated CTE) → challenge: confirm not already diverged + scope → R2 CONFIRMED (byte-identical, partial dup) → challenge: confirm no seed/macro exists → R3 CONFIRMED (0 grep hits). Held low.
- **P2P-C16**: R1 CONFIRMED (no `window:` tag) → challenge: confirm `window:` is actually required → R2 RESOLVED (guard does not require it; `latest` in POINT_GRANS) → challenge: run guard → R3 RESOLVED (`check_api_tags.py` passes). Resolved.
- **P2P-C17**: R1 CONFIRMED (`pct=100%` 14d) → challenge: prove at public surface → R2 CONFIRMED (`api_p2p_visits_latest` discv4=100.0/discv5=16.9) → challenge: confirm external exposure → R3 CONFIRMED (api tag + semantic model). Held critical.
- **P2P-C18**: R1 CONFIRMED (`''` bucket present) → challenge: show distortion magnitude → R2 CONFIRMED (`''`=40% top Country bucket) → challenge: prove not one-day fluke → R3 CONFIRMED (top label all 14 days, `35.6-41.0%`). Held high.
- **P2P-C19**: R1 CONFIRMED (54%/42% survive) → challenge: prove cloud-provider bias → R2 CONFIRMED (Hetzner/OVH survivors) → challenge: settle no-caveat clause → R3 CONFIRMED (schema.yml generic, no caveat). Held high.
- **P2P-C20**: R1 CONFIRMED (93.5% empty maddr, 91.9% geo-null) → challenge: prove source-level + no caveat → R2 CONFIRMED (91.8% geo-null; raw blocked, confidence medium) → challenge: split null-geo by IP presence → R3 CONFIRMED (`92%` of null-geo lack IP; confidence lifted to high). Held high.
- **P2P-C21**: R1 CONFIRMED (OR vs AND, undocumented) → challenge: measure consumer gap → R2 CHANGED→medium (discv5 ~5pp) → challenge: measure discv4 gap too → R3 CHANGED, severity held high (discv4 `24pp` sharp). Changed, high.
- **P2P-C22**: R1 CONFIRMED (4 phantom names in docs) → challenge: citation path/date wrong, correct it → R2 CHANGED→medium (strings in review doc; cerebro-docs absent in checkout, site harm unverifiable) → challenge: locate live cerebro-docs or mark unverifiable → R3 CONFIRMED→high (live cerebro-docs ships all four; 8 occurrences). Confirmed, high.
- **P2P-C23**: R1 CONFIRMED (shared synonyms) → challenge: confirm no other discriminator → R2 CONFIRMED (same tier, no default_metric) → challenge: confirm both members exposed → R3 CONFIRMED (both registered, live collision). Held medium.
- **P2P-C24**: R1 CONFIRMED (90 rows/49 geo) → challenge: crawler-gap vs join-loss → R2 CONFIRMED (90→49 is geo artifact; raw blocked, medium) → challenge: separate using queryable models → R3 CONFIRMED (33 topology peers ~ 45 reachable; genuine coverage). Held medium.
- **P2P-C25**: R1 CONFIRMED (`LIKE '%064'`) → challenge: is over-inclusion non-zero → R2 CONFIRMED (`matched_only_via_064=0`) → challenge: check for network_id guard → R3 CONFIRMED (no guard; residual risk). Held low.
- **P2P-C26**: R1 CONFIRMED (post-Fulu → NULL, no alert) → challenge: is silent-exclusion active → R2 CONFIRMED (`null_fork=0`, latent) → challenge: pin failure mode → R3 CONFIRMED (unlabelled bucket, not drop). Held low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE) | Fix discv4 `pct_successful` — the OR is vacuous because `empty(dial_errors)=1` is always true for discv4. Either suppress `discv4_pct_successful` from the public mart or redefine success on a meaningful predicate; it is api-tagged + semantic-exposed (constant 100% misleads consumers). | `models/p2p/intermediate/int_p2p_discv4_visits_daily.sql`, `models/p2p/marts/api_p2p_visits_latest.sql` |
| P0 (ESCALATE) | Remove the four phantom models from the LIVE cerebro-docs (`docs/models/p2p.md` lines 116-119/127/136/146, `docs/mcp/reports.md:262`); their query examples return table-not-found. | `cerebro-docs/docs/models/p2p.md`, `cerebro-docs/docs/mcp/reports.md` |
| P1 (KEEP) | Add `join_use_nulls=1` pre_hook to `int_p2p_discv4_peers` and both topology intermediates (mirror `int_p2p_discv5_peers`); converts `''` geo to NULL, removing the `40%` `''` Country bucket and the empty-bucket distortion. | `int_p2p_discv4_peers.sql`, `int_p2p_discv4_topology_latest.sql`, `int_p2p_discv5_topology_latest.sql`, `api_p2p_discv4_clients_daily.sql` |
| P1 (KEEP) | Remove phantom/contradictory schema.yml tests (compiled in manifest, error at runtime): `date`/`crawls` on the two `*_latest` marts; `peer_id`/`next_fork` on `forks_daily`; pre-pivot CTE columns + `peer_id` unique/not_null on both `clients_daily`; drop column-level unique on `peer_id` for `int_p2p_discv4_peers`. | `models/p2p/marts/schema.yml`, `models/p2p/intermediate/schema.yml` |
| P1 (KEEP) | Replace single-column unique tests on `crawl_id`/`peer_id` in staging with composite keys: `(crawl_id, peer_id)` for visits, `(crawl_id, peer_discovery_id_prefix, neighbor_discovery_id_prefix)` for neighbors. | `models/p2p/staging/schema.yml` |
| P1 (KEEP) | Document the served-topology coverage loss (`~46%` discv4 / `~58%` discv5 edges dropped, biased to cloud peers) and the discv5 geo source limitation (`~92%` no IP) in the mart/staging schema.yml descriptions. | `api_p2p_topology_latest`, `int_p2p_discv5_peers`, `models/p2p/marts/schema.yml` |
| P2 (KEEP) | Document the OR-vs-AND definitional split (visits=OR success, clients/forks=AND reachable); sharp for discv4 (`24pp`). | `int_p2p_discv4/5_visits_daily.sql`, `int_p2p_discv4/5_clients_daily.sql`, schema.yml/docs |
| P2 (KEEP) | Replace `any(cl_fork_name)` with `argMax(cl_fork_name, visit_ended_at)` to make per-peer-day fork assignment deterministic (`48` conflicting peer-days, `Electra` vs `Fulu`). | `int_p2p_discv5_forks_daily.sql` |
| P2 (KEEP) | Add a `config()` block to `fct_p2p_discv5_forks_daily` (materialization/engine/partition + `api:`/`tier:` tags) — currently a view with empty tags. | `fct_p2p_discv5_forks_daily.sql` |
| P2 (KEEP) | Disambiguate duplicate `question_synonyms` across mart/intermediate pairs (or exclude intermediates from the semantic registry). | `semantic/authoring/p2p/semantic_models.yml` |
| P2 (KEEP) | Add a CROSS-JOIN/anchor-gap guard (LEFT JOIN + `COALESCE(prev_count,0)`) to the `*_latest` marts; and a fallback window / staleness flag on the topology intermediates. | `api_p2p_clients_latest.sql`, `api_p2p_visits_latest.sql`, `int_p2p_discv4/5_topology_latest.sql` |
| P3 (KEEP) | Tidy latent/config items: document/align discv4 vs discv5 `lookback_days`; set explicit `materialized='view'` on visits staging; extract the duplicated `fork_digests` CTE to a seed/macro; add a network_id guard before `LIKE '%064'`; add an unrecognized-fork-digest test/alert. | discv4/discv5 `clients_daily`/`visits_daily`, `stg_nebula_discv4/5__visits`, `int_p2p_discv5_peers`, `int_p2p_discv5_visits_daily` |
| — (DROP) | `P2P-C16` — no action needed; `window:` is not part of the enforced four-tag CI convention and `latest` endpoints are point-in-time. Guard already passes for all nine `api_p2p_*` models. | (none) |
