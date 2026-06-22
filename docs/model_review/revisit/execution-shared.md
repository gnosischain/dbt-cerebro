# Model review (revisit 2026-06-21): execution/shared

Baseline `docs/model_review/execution-shared.md` (2026-06-11); 18 cases re-verified across 4 rounds (17 baseline + 1 new). Headline: `0` resolved, `4` changed (all downgrades/magnitude corrections), `11` still confirmed, `1` new high-severity upstream double-load — the lending flag (`is_lending_user`) is still `100%` broken live (critical) and a fresh duplicate-partition load now threatens `~2x` double-counting on the next rebuild.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONSHARED-C01 | P0-04 | `int_consensus_validators_labels` still `dev`-tagged; selective build could empty `is_validator_depositor` | high | CHANGED | medium | high | none | 4 |
| EXECUTIONSHARED-C02 | | Upstream `int_execution_gnosis_app_gpay_wallets` uses banned `delete+insert` | high | CHANGED | medium | high | none | 4 |
| EXECUTIONSHARED-C03 | | No `expose_to_mcp`/`privacy_tier` on an MCP-reachable address-role model | medium | CONFIRMED | medium | high | none | 4 |
| EXECUTIONSHARED-C04 | | Not `production`-tagged, so `check_api_tags.py` never validates a future `api:` tag | medium | CONFIRMED | low | high | none | 4 |
| EXECUTIONSHARED-C05 | | `ReplacingMergeTree()` on full-rebuild table with no `ver` column | low | CONFIRMED | low | high | none | 4 |
| EXECUTIONSHARED-C06 | | Session `SET` pre/post_hook can leak `max_threads=1` on mid-build failure | low | CONFIRMED | low | high | none | 4 |
| EXECUTIONSHARED-C07 | | 8/10 UNION ALL branches use positional literal padding | low | CONFIRMED | low | high | none | 4 |
| EXECUTIONSHARED-C08 | P0-18 | `is_lending_user=0` for all 5.8M addresses — lending flag silently broken | critical | CONFIRMED | critical | high | none | 4 |
| EXECUTIONSHARED-C09 | | `is_safe_owner=1` for GPay sentinel `0x...0002` (no guard) | high | CHANGED | medium | high | none | 4 |
| EXECUTIONSHARED-C10 | | `is_lending_user` is point-in-time, not "ever lent"; schema desc unqualified | medium | CONFIRMED | medium | high | none | 4 |
| EXECUTIONSHARED-C11 | | `anyIf(pool_protocol)` non-deterministically drops secondary protocol for multi-protocol LPs | medium | CONFIRMED | medium | high | none | 4 |
| EXECUTIONSHARED-C12 | | `quality_tier: approved` semantic model with "candidate; review" metric descriptions; 3/14 measures | medium | CONFIRMED | medium | high | none | 4 |
| EXECUTIONSHARED-C13 | | `is_validator_depositor` drops `0x00` (and `0x02`) credential validators; undocumented | low | CHANGED | medium | high | none | 4 |
| EXECUTIONSHARED-C14 | | Canonical GPay spender lives only in prose, not seed/var/whitelist | low | CHANGED | low | high | none | 4 |
| EXECUTIONSHARED-C15 | | Grain integrity holds: 5,816,837 rows, all distinct on address | low | CONFIRMED | low | high | none | 4 |
| EXECUTIONSHARED-C16 | | 62,654 addresses carry 2+ role flags | low | CONFIRMED | low | high | none | 4 |
| EXECUTIONSHARED-C17 | | 4,706,904 (81%) have `has_dune_label=1` as sole role | low | CONFIRMED | low | high | none | 4 |
| EXECUTIONSHARED-N01 | | Fresh duplicate-grain double-loads in `int_execution_lending_aave_user_balances_daily` (epochs 20622/20624) | — | NEW | high | high | other | 1 |

## Delta vs baseline

### RESOLVED (0)
- None. No baseline defect was fixed in code between `2026-06-11` and `2026-06-21`.

### CHANGED (4 — all downgrades or magnitude corrections; 0 fixes)
- **C01** high -> medium: `dev` tag still present on `models/consensus/intermediate/int_consensus_validators_labels.sql` line 6 and `int_consensus_validators_withdrawal_addresses.sql` still `ref()`s it (line 22), but **no scheduled/CI/partial selector excludes `tag:dev`**. Production cron selects only `tag:production` batches (the labels/withdrawal/roles chain is none of them); `gap_window_refresh.py`/`dbt_incremental_runner` build via `dbt ls --select` topo order which pulls upstreams. Hazard is latent-governance-only with no live trigger -> medium. `sum(is_validator_depositor)=873` (unchanged).
- **C02** high -> medium: `int_execution_gnosis_app_gpay_wallets.sql` still declares `incremental_strategy='delete+insert'` (banned by `scripts/checks/no_delete_insert.py`) but is suppressed via the allowlist `no_delete_insert.allow` line 19 (CI green). Realized risk absent: grain is clean (`count(*)=1240 = uniqExact(pay_wallet)=1240`, `0` dup keys); the 3 owners with conflicting `is_currently_ga_owned` are legit multi-wallet EOAs correctly masked by the pivot's `GROUP BY`. Policy/maintainability only -> medium.
- **C09** high -> medium: `is_safe_owner=1` for exactly 2 sentinel/burn addresses (`0x...0002` and `0x...dead`) of `418,756` total owners. But `is_safe_owner` has **no approved semantic measure** (only 3/14 flags are measured per C12), so it only surfaces in Graph Explorer node badges — realized exposure is cosmetic -> medium.
- **C13** low -> medium (and magnitude corrected): the `int_consensus_validators_withdrawal_addresses.sql` CASE (lines 17-21) handles **only** `0x01` credentials, dropping `0x02` (`ELSE NULL`). Baseline claimed only unavoidable `0x00` drop. Real coverage gap = **+107 net-new distinct depositors** (`873 -> 980`): `0x02` yields `130` distinct derivable EVM addresses, of which `23` overlap the existing `873`. `0x00` (834) genuinely have no EVM address (unavoidable). Verifier round-2 overstatement of `5,111` was corrected to `+107`. Schema documents only the `0x01` limitation.
- **C14** low (claim refined): original "lives only in a schema.yml description string" is wrong — the GPay spender `0x4822521e6135cd2599199c83ea35179229a172ee` is a hardcoded Jinja `{% set %}` literal in **4 SQL models** (`int_execution_gpay_wallets.sql:15`, `int_execution_gpay_activity.sql:29`, `int_revenue_gpay_fees_daily.sql:1`, `fct_execution_gpay_payments_hourly.sql:57`) plus the schema prose. All 4 SQL literals byte-identical (no divergent copy); not in any seed/var/whitelist. Governance substance holds (worse than claimed) -> stays low.

### STILL CONFIRMED (11)
- **C08** critical (the central live defect): `sum(is_lending_user)=0` across all `5,816,837` roles rows. Root cause healed upstream (`fct_execution_yields_user_lending_positions_latest` recovered from 0 rows to `20,308` rows / `$86.99M`) but the roles mart is **byte-identical to the 10-day baseline** (`5,816,837`), proving it has not been rebuilt — staleness is the sole live cause. The mart is NOT in production cron (tags `[execution,shared,identity,graph_explorer]`, no `production`), so the flag stays `0` indefinitely until a manual/graph-explorer rebuild. Not resolved.
- **C03** medium: `models/execution/shared/marts/schema.yml` has no `meta` block / no `expose_to_mcp` / no `privacy_tier`, while the semantic model is `quality_tier: approved`. Norm on record: `int_execution_gpay_user_identity_bridge.sql:8` sets `meta={'expose_to_mcp': False, 'privacy_tier': 'internal'}`; gpay intermediate `schema.yml` lines 1071-1072 same.
- **C04** low: tags `['execution','shared','identity','graph_explorer']`, no `production`; `check_api_tags.py` short-circuits non-production nodes (`if "production" not in tags: continue`).
- **C05** low: `materialized='table'` with `engine='ReplacingMergeTree()'`, `order_by='(address)'`, no `ver` column; `count(*)=uniqExact(address)=5,816,837` so `0` observable dups — engine choice semantically misleading.
- **C06** low (effectively nil): settings still via session `SET` pre/post_hook. Settled via `profiles.yml`: `type: clickhouse`, `port: 8123` (HTTP stateless), `threads: 1` -> `SET` cannot leak across models.
- **C07** low: 8/10 UNION ALL branches use positional literal padding (e.g. line 145 `SELECT address, 0, 1, 0, '', ...`); only the safes and dex_roles branches name columns. Alignment correct today; lockstep-edit hazard.
- **C10** medium: `is_lending_user` description (`schema.yml` lines 65-67) says only "active ... position" with no closure-zeroes qualifier; `10,460` distinct ever-positive lenders are absent from the latest snapshot and read `0`.
- **C11** medium: double arbitrary-pick (inner `any(protocol) GROUP BY role,address` line 104 + outer `anyIf(pool_protocol,...)` line 98); `143/6,103` LP addresses (`2.34%`) lose secondary protocol. Reproduced: `0x458cd345...` is active in Balancer V2+V3 but resolves to single `'Balancer V2'`. Fix: `groupArrayDistinct(protocol)`.
- **C12** medium: semantic model `quality_tier: approved` (line 35) coexists with all 3 metric descriptions reading "Auto-generated candidate metric; review and promote before relying on it" (lines 44-45/64-65/84-85); only 3 of 14 flags have a measure.
- **C15** low (corroborates staleness): `count(*)=uniqExact(address)=5,816,837`, byte-identical to baseline; `not_null`+`unique` tests wired on `address`.
- **C16** low: `62,654` addresses carry 2+ role flags (product flags, excl `has_dune_label`); `731,651` if `has_dune_label` counted.
- **C17** low: `4,706,904` of `5,816,837` (`80.9%`) are dune-label-only; true product-user denominator is `~1.11M` (addresses with >=1 product role = `1,109,933`), not `5.82M`.

### NEW (1)
- **N01** high (incident attribution: `other` — fresh DQ defect, distinct from C08's stale flag): `int_execution_lending_aave_user_balances_daily` has **duplicate-grain double-loads on epochs 20622 (2026-06-18) and 20624 (2026-06-20)** (`rows = 2x uniqExact(user,reserve,protocol)`) plus a value-doubling on 20623. The `fct` `latest_date` CTE (`date < today()`) currently targets the doubled epoch 20624; the `fct` has not yet rebuilt against it, so a rebuild will inflate positions/USD `~2x`. Same `insert_overwrite`/microbatch partition-double-write family as the June 2026 incident.

### UNVERIFIABLE / UNRESOLVED (0)
- None.

## Evidence appendix

**C01** (`int_execution_address_roles_current` / consensus chain):
```sql
SELECT sum(is_validator_depositor) FROM dbt.int_execution_address_roles_current;
-- 873
```
Code: `int_consensus_validators_labels.sql` line 6 `tags=['dev','consensus','validators']`; `int_consensus_validators_withdrawal_addresses.sql` line 22 `ref('int_consensus_validators_labels')`. Production cron uses `--select tag:production` batches only; chain not production-tagged. `gap_window_refresh.py` builds via `dbt ls --select <sel>` topo (pulls upstreams). No `--exclude tag:dev` in any cron/refresh/CI builder (only read-only `verify_migration.py` / `check_api_tags.py` reference dev).

**C02** (`int_execution_gnosis_app_gpay_wallets`):
```sql
SELECT count(*), uniqExact(pay_wallet),
  (SELECT count(*) FROM (SELECT first_ga_owner_address FROM dbt.int_execution_gnosis_app_gpay_wallets
     WHERE first_ga_owner_address IS NOT NULL GROUP BY first_ga_owner_address
     HAVING uniqExact(is_currently_ga_owned)>1))
FROM dbt.int_execution_gnosis_app_gpay_wallets;
-- count=1240, uniqExact(pay_wallet)=1240 (0 dup keys), conflicting-state owners=3 (legit multi-wallet EOAs)
```
Code: `incremental_strategy='delete+insert'` (line 8); allowlisted at `no_delete_insert.allow` line 19.

**C03** (schema/semantic metadata): `models/execution/shared/marts/schema.yml` — no `meta`/`expose_to_mcp`/`privacy_tier`. `semantic/authoring/execution/shared/semantic_models.yml` — `quality_tier: approved` (line 35), 3 approved metrics. Contrast: `int_execution_gpay_user_identity_bridge.sql:8` `meta={'expose_to_mcp': False, 'privacy_tier': 'internal'}`.

**C04**: `int_execution_address_roles_current.sql` line 6 `tags=['execution','shared','identity','graph_explorer']` (no `production`); `check_api_tags.py` line 53 `if "production" not in tags: continue`.

**C05**:
```sql
SELECT count(*), uniqExact(address), count(*)-uniqExact(address) AS dups FROM dbt.int_execution_address_roles_current;
-- 5816837, 5816837, 0
```
Code: lines 3-5 `materialized='table'`, `engine='ReplacingMergeTree()'`, `order_by='(address)'`; no `ver` in SELECT.

**C06**: `int_execution_address_roles_current.sql` pre_hook lines 7-13 `SET max_threads=1, max_block_size=8192, max_bytes_before_external_group_by=2000000000`; post_hook lines 14-20 reset. `profiles.yml`: `type: clickhouse` (5), `port: 8123` (9), `threads: 1` (13).

**C07**: `int_execution_address_roles_current.sql` lines 135-184; positional literals on gpay (145), wrappers (157), safe_owners (161), lenders (175), validators (179), and others; named columns only on safes (135-141) and dex_roles (164-172).

**C08** (central defect):
```sql
SELECT today(), sum(is_lending_user), count() FROM dbt.int_execution_address_roles_current;
-- 20625, 0, 5816837
SELECT count(), uniqExact(user_address), round(sum(balance_usd),2)
FROM dbt.fct_execution_yields_user_lending_positions_latest;
-- 20308, 19711, 86994143.85
```
`fct` `latest_date` CTE (`fct_execution_yields_user_lending_positions_latest.sql` lines 13-17): `SELECT max(date) ... WHERE date < today()`. `fct` model lines 19-54 have NO `GROUP BY`/dedup. Roles count byte-identical to baseline (`5,816,837`); mart not in production cron.

**C08 + N01** (source daily double-loads):
```sql
SELECT toDate(date) d, count() rows, countIf(balance_usd>0.01) pos, round(sum(if(balance_usd>0.01,balance_usd,0)),2) pos_usd,
  uniqExact(user_address,reserve_address,protocol) grain
FROM dbt.int_execution_lending_aave_user_balances_daily WHERE date>=today()-8 GROUP BY d ORDER BY d;
-- 20622 (06-18): 82,260 rows / 41,138 grain (2x) / $86.3M
-- 20623 (06-19): 41,141 rows / 41,141 grain (clean) / $86.67M (value-doubled ~2x clean ~$43.6M)
-- 20624 (06-20): 82,285 rows / 41,144 grain (2x) / 40,606 pos / $173.9M  <- latest_date target
-- clean ref days 20616-20621: ~41,1xx rows = grain / ~$43.5M
```

**C09**:
```sql
SELECT is_safe_owner, address FROM dbt.int_execution_address_roles_current
WHERE address IN ('0x0000000000000000000000000000000000000002',
                  '0x000000000000000000000000000000000000dead',
                  '0x0000000000000000000000000000000000000001',
                  '0x0000000000000000000000000000000000000000');
-- 0x...0002 -> is_safe_owner=1; 0x...dead -> is_safe_owner=1; 0x...0000 -> is_safe_owner=0; 0x...0001 absent
```
Exactly 2 contaminated of `418,756` owners; `is_safe_owner` has no approved semantic measure.

**C10**:
```sql
SELECT uniqExact(lower(user_address)) FROM dbt.int_execution_lending_aave_user_balances_daily
WHERE balance_usd>0.01 AND lower(user_address) NOT IN
  (SELECT lower(user_address) FROM dbt.fct_execution_yields_user_lending_positions_latest);
-- 10,460 ever-positive lenders absent from latest snapshot
```

**C11**:
```sql
WITH lp AS (SELECT lower(provider) addr, uniqExact(protocol) np
  FROM dbt.int_execution_pools_dex_liquidity_events
  WHERE provider IS NOT NULL AND provider!='' GROUP BY addr)
SELECT count(*), countIf(np>1), round(100.0*countIf(np>1)/count(*),2) FROM lp;
-- 6103, 143, 2.34
```
Code: `dex_roles` inner `any(protocol) GROUP BY role,address` (line 104) + outer `anyIf(pool_protocol, role='lp' AND pool_protocol!='')` (line 98).

**C12**: `semantic_models.yml` `quality_tier: approved` (line 35); 3 measures (`is_safe_value`, `is_gpay_wallet_value`, `is_circles_avatar_value`, lines 22-30); all 3 metric descriptions "Auto-generated candidate metric; review and promote before relying on it." (lines 44-45/64-65/84-85).
```sql
SELECT max(is_safe), max(is_gpay_wallet), max(is_circles_avatar) FROM dbt.int_execution_address_roles_current;
-- 1, 1, 1 (each flag 0/1 per address -> sum = distinct-address count given unique grain)
```

**C13**:
```sql
WITH derived AS (SELECT substring(withdrawal_credentials,1,4) prefix,
  lower(concat('0x',substring(withdrawal_credentials,27,40))) evm
  FROM dbt.int_consensus_validators_withdrawal_addresses)
SELECT countDistinctIf(evm,prefix='0x01'), countDistinctIf(evm,prefix='0x02'), countIf(prefix='0x00'),
  (SELECT count(*) FROM (SELECT DISTINCT evm FROM derived WHERE prefix='0x02')
     WHERE evm NOT IN (SELECT DISTINCT evm FROM derived WHERE prefix='0x01')) FROM derived;
-- 0x01->873 distinct EVM; 0x02->130 distinct EVM; 0x00->834 rows (0 derivable); net-new 0x02 = 107
```
Code: `int_consensus_validators_withdrawal_addresses.sql` lines 17-21 `CASE WHEN startsWith(withdrawal_credentials,'0x01') THEN concat('0x',substring(...,27,40)) ELSE NULL END`.

**C14**: `grep` repo: lowercase `0x4822521e6135cd2599199c83ea35179229a172ee` appears 4x (all SQL: `int_execution_gpay_wallets.sql:15`, `int_execution_gpay_activity.sql:29`, `fct_execution_gpay_payments_hourly.sql:57`, `int_revenue_gpay_fees_daily.sql:1`); checksum-cased once in `gpay/intermediate/schema.yml:9` (prose). Not in seeds/var/whitelist.

**C15**:
```sql
SELECT count(*), uniqExact(address) FROM dbt.int_execution_address_roles_current;
-- 5816837, 5816837
```
`schema.yml` lines 24-27: `address` `tests: [not_null, unique]`.

**C16**:
```sql
SELECT countIf((is_safe+is_gpay_wallet+is_ga_user+is_circles_avatar+is_circles_wrapper+is_safe_owner
  +is_lp_provider+is_pool+is_lending_user+is_validator_depositor)>=2) FROM dbt.int_execution_address_roles_current;
-- 62,654 (product flags, excl has_dune_label); 731,651 incl has_dune_label
```

**C17**:
```sql
SELECT countIf(has_dune_label=1 AND (is_safe+is_gpay_wallet+is_ga_user+is_circles_avatar+is_circles_wrapper
  +is_safe_owner+is_lp_provider+is_pool+is_lending_user+is_validator_depositor)=0) FROM dbt.int_execution_address_roles_current;
-- 4,706,904 (80.9%); addresses with >=1 product role = 1,109,933; tie-out 4,706,904 + 1,109,933 = 5,816,837
```

## Review log (>=3 rounds per case)

- **C01**: r1 CONFIRMED high (dev tag + direct ref present, 873 populated) -> challenge: does any scheduled/CI selector exclude `tag:dev`? -> r2 CONFIRMED high; grepped all .sh/.yml/.py, only read-only verify/lint scripts reference dev -> challenge: any partial refresh build the chain WITHOUT labels? -> r3 CHANGED high->medium (production selects only `tag:production`; refresh/gap topo-include upstreams; no live trigger) -> r4 CHANGED medium (held).
- **C02**: r1 CONFIRMED high (delete+insert present, allowlisted) -> challenge: grain integrity? -> r2 CHANGED high->medium (`1240=1240`, 0 dup keys) -> challenge: stale/conflicting-state rows? -> r3 CHANGED medium (3 conflicting owners are legit multi-wallet EOAs, masked by pivot GROUP BY) -> r4 CHANGED medium (held).
- **C03**: r1 CONFIRMED medium (no expose_to_mcp/privacy_tier; approved semantic model) -> challenge: show approved relationships + a contrasting privacy-sensitive model -> r2 CONFIRMED medium (entity `address` + 3 approved metrics) -> challenge: quote one sibling's meta block -> r3 CONFIRMED medium (`int_execution_gpay_user_identity_bridge` meta on record) -> r4 CONFIRMED medium.
- **C04**: r1 CONFIRMED medium -> r2 CONFIRMED, downgraded low (pure latent governance gap, no `api:` tag today) -> r3 CONFIRMED low -> r4 CONFIRMED low.
- **C05**: r1 CONFIRMED low -> challenge: any observable pre-merge dups? -> r2 CONFIRMED low (`count==uniqExact`, 0 dups) -> r3 CONFIRMED low -> r4 CONFIRMED low.
- **C06**: r1 CONFIRMED low -> challenge: session/pool semantics? -> r2 CONFIRMED low (couldn't resolve pool, confidence medium) -> challenge: read profiles.yml -> r3 CONFIRMED low (HTTP port 8123 stateless -> leak nil) -> r4 CONFIRMED low.
- **C07**: r1 CONFIRMED low -> challenge: verify column alignment correct today -> r2 CONFIRMED low (sentinels map to intended columns) -> r3 CONFIRMED low -> r4 CONFIRMED low.
- **C08**: r1 CHANGED high (root cause healed upstream; flag still 0 — roles stale) -> challenge: prove complete-day watermark + staleness; do NOT mark resolved -> r2 CONFIRMED critical (fct off complete day 20624; roles byte-identical to baseline) -> challenge: 20624 may be a double-load; will rebuild double-count? -> r3 CONFIRMED critical (verifier claimed 20624 clean — contradicted by live data) -> r4 CONFIRMED critical (retracted: 20624 is a double-load; fct has no dedup; mart not scheduled).
- **C09**: r1 CONFIRMED high (sentinel `0x...0002` is_safe_owner=1) -> challenge: blast radius + downstream exposure -> r2 CONFIRMED high (2 addrs: `0x...0002`, `0x...dead`) -> challenge: served via approved metric or only badges? -> r3 CHANGED high->medium (no approved measure; cosmetic) -> r4 CHANGED medium.
- **C10**: r1 CONFIRMED medium (unqualified "active position") -> challenge: quantify former lenders -> r2 CONFIRMED medium (mechanism confirmed, count unmeasured) -> challenge: bounded count query -> r3 CONFIRMED medium (`10,460` former lenders) -> r4 CONFIRMED medium.
- **C11**: r1 CONFIRMED medium (anyIf) -> challenge: prove membership loss + size impact -> r2 CONFIRMED medium (`0x458cd345` loses Balancer V3) -> challenge: full population -> r3 CONFIRMED medium (`143/6,103`, 2.34%; double collapse) -> r4 CONFIRMED medium.
- **C12**: r1 CONFIRMED medium -> challenge: validate sum=distinct-count + enumerate uncovered flags -> r2 CONFIRMED medium (3/14 measures, 8 boolean flags uncovered) -> r3 CONFIRMED medium -> r4 CONFIRMED medium.
- **C13**: r1 CONFIRMED low (0x00 undocumented; noted 0x02 also drops) -> challenge: quantify 0x00 vs 0x02 prefix distribution -> r2 CHANGED low->medium (claimed 5,111 0x02 dropped) -> challenge: count distinct derivable EVM, net-new -> r3 CHANGED medium (corrected to +107 net-new) -> r4 CHANGED medium.
- **C14**: r1 CHANGED (prose-only claim wrong; 4 SQL literals) -> challenge: confirm byte-identical literals -> r2 CONFIRMED low (4 byte-identical) -> r3 CONFIRMED low (refined to Jinja `{% set %}` in 4 models) -> r4 CHANGED low.
- **C15**: r1 CONFIRMED low (`5,816,837=uniqExact`) -> challenge: prove test-enforced -> r2 CONFIRMED low (not_null+unique wired) -> r3 CONFIRMED low -> r4 CONFIRMED low.
- **C16**: r1 CONFIRMED low (`62,654`) -> challenge: settle exclude-dune definition -> r2 CONFIRMED low (62,654 product; 731,651 incl-dune) -> r3 CONFIRMED low -> r4 CONFIRMED low.
- **C17**: r1 CONFIRMED low (`4,706,904`, 81%) -> challenge: quantify inverse denominator -> r2 CONFIRMED low (`1,109,933` product users; ties out) -> r3 CONFIRMED low -> r4 CONFIRMED low.
- **N01**: discovered r4 — NEW high (epochs 20622/20624 double-loads, 20623 value-doubled; distinct from C08); ratified by orchestrator r4.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 — ESCALATE | Rebuild `int_execution_address_roles_current` to repopulate `is_lending_user` (currently `0` for all 5.8M addresses), and add it to a scheduled refresh so the flag does not silently re-stale. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P0 — ADD (NEW) | Drop the duplicate partitions on epochs `20622` and `20624` (and fix the `20623` value-doubling) in `int_execution_lending_aave_user_balances_daily` BEFORE any `fct`/roles rebuild, else positions/USD double-count `~2x`. Investigate the recurring `insert_overwrite`/microbatch partition-double-write. | `models/execution/yields/intermediate/int_execution_lending_aave_user_balances_daily.sql` |
| P1 — KEEP | Harden the lending watermark: `latest_date = max(date) WHERE date < today()` lands on partial/zero/double-loaded days. Require a complete-day guard (and dedup in the `fct`, which currently has no `GROUP BY`). | `models/execution/yields/marts/fct_execution_yields_user_lending_positions_latest.sql` |
| P1 — KEEP | Add a `0x02` branch to the withdrawal-credential CASE to recover `+107` net-new depositors; document the `0x00` exclusion in the schema. | `models/consensus/intermediate/int_consensus_validators_withdrawal_addresses.sql`, `models/execution/shared/marts/schema.yml` |
| P2 — KEEP | Add `expose_to_mcp` + `privacy_tier` meta to the address-role model (norm: `int_execution_gpay_user_identity_bridge`); reconcile `quality_tier: approved` vs "candidate; review" metric descriptions and expand measure coverage beyond 3/14. | `models/execution/shared/marts/schema.yml`, `semantic/authoring/execution/shared/semantic_models.yml` |
| P2 — KEEP | Guard `is_safe_owner` against sentinel/burn addresses (`0x...0002`, `0x...dead`). | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P2 — KEEP | Replace `anyIf(pool_protocol)` / inner `any(protocol)` with `groupArrayDistinct(protocol)` to stop dropping secondary protocol for `143` multi-protocol LPs. | `models/execution/shared/marts/int_execution_address_roles_current.sql` |
| P3 — KEEP | Remove banned `delete+insert` from `int_execution_gnosis_app_gpay_wallets` (and its allowlist entry); grain is currently clean but the strategy is policy-banned. | `models/execution/gnosis_app/intermediate/int_execution_gnosis_app_gpay_wallets.sql`, `scripts/checks/no_delete_insert.allow` |
| P3 — KEEP | Qualify `is_lending_user` schema description as point-in-time (`10,460` former lenders read `0`). | `models/execution/shared/marts/schema.yml` |
| P3 — KEEP | Centralize the GPay spender `0x4822...172EE` in a seed/var (currently 4 hardcoded Jinja literals + prose). | `int_execution_gpay_wallets.sql`, `int_execution_gpay_activity.sql`, `int_revenue_gpay_fees_daily.sql`, `fct_execution_gpay_payments_hourly.sql` |
| P3 — KEEP | Either remove the `dev` tag from `int_consensus_validators_labels` or document that no build excludes `tag:dev`; convert UNION positional padding to named columns; replace session `SET` hooks with `query_settings={}`; align engine choice (RMT vs full-rebuild table); add `production`/`api:` governance if the model is ever served. | `int_consensus_validators_labels.sql`, `int_execution_address_roles_current.sql` |
