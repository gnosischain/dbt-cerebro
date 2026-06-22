# Model review (revisit 2026-06-21): crawlers_data

Baseline: `docs/model_review/crawlers_data.md` (2026-06-11); 23 cases re-verified over 3 rounds. Headline: `0` resolved, `2` changed, `20` still confirmed, `1` new live freshness failure (cow ingestor stall); the two highs (month-partition cap, bridge-as-aggregator misattribution) and the cow staleness remain the only above-low findings.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| CRAWLERSDATA-C01 | — | `int_crawlers_data_labels` partitions `toStartOfMonth` over 94 months, ~6 from CH Cloud 100-partition full-refresh cap (code 252) | high | CONFIRMED | high | high | none | 3 |
| CRAWLERSDATA-C02 | — | `stg_crawlers_data__dune_bridge_flows_v2` references non-existent `date`/`txs` on a tx-level source; runtime failure | high | CONFIRMED | medium | high | none | 3 |
| CRAWLERSDATA-C03 | — | `stg_crawlers_data__dune_prices` `anyLast(price)` non-deterministic over 2,577 dup grain pairs; no ingestion ts for argMax | medium | CONFIRMED | medium | high | none | 3 |
| CRAWLERSDATA-C04 | — | `dune_labels` inherits 18h/30h freshness vs documented weekly cadence | medium | CHANGED | low | high | none | 3 |
| CRAWLERSDATA-C05 | — | dedup tie-break `lower(project)='gpay'` mismatched to canon `'Gnosis Pay'` title case | medium | CONFIRMED | medium | high | none | 3 |
| CRAWLERSDATA-C06 | — | `int_crawlers_data_labels_dex` schema.yml says DEX-only but 72.4% non-DEX | medium | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C07 | — | `stg_crawlers_data__dune_labels` schema.yml documents 13 phantom CTE columns not in output | medium | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C08 | — | `fct_crawlers_data_distinct_projects_sectors` RMT, no version col, no unique test | medium | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C09 | — | `unique_key` cosmetic in dbt-clickhouse on the two int label models | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C10 | — | `api_..._totals` `as_of_date=today()` vs doc max-date; opaque `value1/value2`; semantic `sum()` should be `max()` | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C11 | — | `stg_crawlers_data__dune_gno_supply` passthrough, no `lower(label)`, no accepted_values | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C12 | — | `int_crawlers_data_labels_dex` strips `introduced_at` from output | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C13 | — | 98 Bridges addresses pass the dex filter and populate `api_execution_live_trades.aggregator` | high | CONFIRMED | high | high | none | 3 |
| CRAWLERSDATA-C14 | — | gpay dedup latently unreliable; guard does not protect the 6 canonical `Gnosis Pay` rows | medium | CONFIRMED | medium | medium | none | 3 |
| CRAWLERSDATA-C15 | — | api totals view reads RMT fct without FINAL; tier0 countDistinct KPI inflation risk | medium | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C16 | — | `stg_crawlers_data__dune_prices` Chainlink-fallback demotion not tagged/commented in staging | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C17 | — | `api_crawlers_data_gno_supply_daily` label categories undocumented/unnormalised | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C18 | — | data: `dune_labels` duplicate-address pattern handled in int layer | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C19 | — | data: `dune_labels` FINAL raises code 181 (SharedMergeTree), missing-FINAL risk moot | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C20 | — | data: `dune_gno_supply` clean (3 labels, 0 null/zero, 0 dup grain), fresh | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C21 | — | data: `cow_api_trade_fees` FINAL 0 dup on `(order_uid,tx_hash,log_index)` grain | low | CONFIRMED | low | high | none | 3 |
| CRAWLERSDATA-C22 | — | data: not all sources fresh — `cow_api_trade_fees` stale | low | CHANGED | medium | high | other | 3 |
| CRAWLERSDATA-N01 | — | NEW: cow fees loader stalled, 140h+ stale, breaches 48h `error_after` | — | NEW (CONFIRMED) | medium | high | other | 3 |

Final severity map: high = `C01`, `C13`; medium = `C03`, `C05`, `C14`, plus `C22`+`N01` (one shared cow-freshness failure, rated once); low = all others.

## Delta vs baseline

### RESOLVED (0)
None. No model SQL, schema.yml, or config touched between `2026-06-11` and `2026-06-21`; every code-level defect persists verbatim.

### CHANGED (2)
- `CRAWLERSDATA-C04` (medium -> low): the "weekly source permanently breaches 30h" premise is refuted. `crawlers_data.dune_labels` lands new `introduced_at` rows every one of the last 30 days (contiguous `20595..20624`, `609`-`67,087` rows/day); currently `21h` stale, so it trips the inherited `18h` WARN but not the `30h` ERROR. The config-vs-docs mismatch (no per-table override; siblings got `36h`/`48h`) persists, and the freshness job IS wired (`scripts/run_dbt_observability.sh` line 154 `dbt source freshness --select source:*`, listed in `cron.sh` MANDATORY_STEPS), so the alert path is real but intermittent, not chronic. Not incident A/B.
- `CRAWLERSDATA-C22` (low -> medium): baseline "all sources max `2026-06-10`, within thresholds" no longer holds. 4 of 5 sources advanced to D-0/D-1 and are within thresholds (`dune_labels 21h`, `circles_blacklisted 17h`, `dune_prices`/`dune_gno_supply 44h`), but `cow_api_trade_fees` is the sole breach at `140h`. Attribution = other (cow ingestor stall); incident A (`insert_overwrite`/`toStartOfMonth`, `06-18`/`06-19`) and incident B (logs gap `05-30`/`06-14`) ruled out by mechanism+date. This is the same failure as N01 — medium attaches ONCE.

### STILL CONFIRMED (20)
- `CRAWLERSDATA-C01` (high): `int_crawlers_data_labels.sql` still `materialized='table'` + `partition_by='toStartOfMonth(introduced_at)'` (lines 3, 8); `94` distinct months (`2018-08`..`2026-05`) over `5,452,340` rows. A full `--full-refresh` writes all 94 partitions in one INSERT — `~6` months from the CH Cloud 100-partition hard block (code 252 TOO_MANY_PARTITIONS). Convention `feedback_ch_cloud_partition_cap.md` mandates `toStartOfYear` for wide-history rebuild tables. Distinct from ESG P0-01. Not incident A.
- `CRAWLERSDATA-C13` (high): `int_crawlers_data_labels_dex` still passes `98` Bridges-sector addresses (Hop/Stargate/LI.FI/Bungee) into `api_execution_live_trades.aggregator`. Blast radius now quantified over a fixed 48h window: `143/1,542` = `9.3%` of aggregator-attributed tx count and `$34,733.64/$619,611.74` = `5.6%` of attributed USD map to bridges — above the 1-2% materiality bar. Undocumented broad "aggregator" definition persists.
- `CRAWLERSDATA-C02` (high -> medium): `stg_crawlers_data__dune_bridge_flows_v2.sql` still references `date`/`txs` (and `net_usd`) absent from the 8-column tx-level `crawlers_data.dune_bridge_flows`; proven runtime failure (code 47 UNKNOWN_IDENTIFIER). Down-rated to medium because it is `dev`-tagged, its sole consumer `int_bridges_flows_daily_v2` is also non-production WIP, and all cron/CI paths select `tag:production` only — zero production blast radius.
- `CRAWLERSDATA-C03` (medium): `anyLast(price)` (line 11) over `2,577` dup `(block_date,symbol)` pairs, `2,576` divergent (e.g. WETH `[3052.18, 3125.965451388889]`). Source still exactly 3 columns, no ingestion ts — argMax infeasible. `842` Dune-only divergent pairs reach served prices via `int_execution_token_prices_daily` priority-3 fallback (Chainlink does not supersede them); `13,396/36,629` = `36.6%` of the feed is live Dune fallback.
- `CRAWLERSDATA-C05` (medium): guard `lower(project)='gpay' DESC, project` (lines 95-96) unchanged; canon emits title-case `'Gnosis Pay'` (stg line 144). Data carries `38,144` `gpay` rows (guard fires) and `6` `Gnosis Pay` rows (guard does NOT protect). The split surfaces as 2 distinct projects in the tier0 `countDistinct(project)` KPI.
- `CRAWLERSDATA-C14` (medium): same gpay root; latent override hole for the 6 uncovered `Gnosis Pay` rows. Int layer is exactly 1 row/address (`5,452,340 = 5,452,340`), so `0` collisions today; the guard fix `lower(project) IN ('gpay','gnosis pay')` is a safe no-op.
- `CRAWLERSDATA-C06` (medium -> low): `int_crawlers_data_labels_dex` is `11,844` rows / `3,269` DEX (`27.6%`) / `98` Bridges; WHERE excludes only 4 sectors, schema.yml (line 35) still says "DEX-only ... restricted to sector = DEX". Doc drift only; sole consumer applies no DEX assumption; no served surface re-states it.
- `CRAWLERSDATA-C07` (medium -> low): `stg_crawlers_data__dune_labels` final SELECT emits 4 columns; schema.yml documents 13 phantom CTE columns (`agg`, `label_raw`, `s1`-`s7`, `looks_like_token_tail`, `wl_symbol`, `project_canon`, `label`). `contract.enforced=false`, no build break, no served surface — non-breaking doc drift.
- `CRAWLERSDATA-C08` (medium -> low): `fct_crawlers_data_distinct_projects_sectors` still RMT no-version, `order_by=(project,sector)`, no unique test; `305` rows, `0` dup grain today. `materialized='table'` single-INSERT rebuild + `SELECT DISTINCT` = single part, no realistic transient-dup window.
- `CRAWLERSDATA-C09` (low): both int label models `unique_key='address'` + `materialized='table'`; on pinned `dbt-clickhouse==1.9.1` `unique_key` is ORDER-BY-only — cosmetic/misleading but harmless; `row_number()` dedup does the work.
- `CRAWLERSDATA-C10` (low): `api_..._totals` `today() AS as_of_date` (line 7) vs schema.yml "max date" (line 44); `value1/value2` opaque `toFloat64` casts; view returns exactly 1 row so semantic `sum()==max()` (latent).
- `CRAWLERSDATA-C11` (low): `stg_crawlers_data__dune_gno_supply` passthrough, no `lower(label)`, no accepted_values; 3 stable labels. Defensive-hardening gap. Same column/fix as C17 — rate once.
- `CRAWLERSDATA-C12` (low): dex output is only `(address, project)`; `introduced_at` stripped; sole consumer applies no age filter — future-facing limitation.
- `CRAWLERSDATA-C15` (medium -> low): api totals view reads RMT fct without FINAL via `countDistinct`. Same defect as C08, one layer up; `countDistinct` self-protects against dup rows and the fct is a single-INSERT table rebuild — no realistic inflation path. Rate ONCE with C08.
- `CRAWLERSDATA-C16` (low): `stg_crawlers_data__dune_prices` still `tags=['production','staging','crawlers_data']`, no deprecation/fallback tag or comment; demotion documented only in `int_execution_token_prices_daily` + `docs/native_token_prices_build_plan.md`.
- `CRAWLERSDATA-C17` (low): `api_crawlers_data_gno_supply_daily` passes label through unnormalised; 3 stable labels, no accepted_values. Same defect as C11 — rate once.
- `CRAWLERSDATA-C18` (low): `dune_labels` `5,467,745` rows / `5,452,340` distinct `lower(address)` (`15,405` dups, up from `13,423`); int layer collapses to exactly `5,452,340` rows = source distinct — dedup exact, benign growth.
- `CRAWLERSDATA-C19` (low): `SELECT count(*) FROM crawlers_data.dune_labels FINAL` raises code 181 ILLEGAL_FINAL — engine is SharedMergeTree, missing-FINAL risk moot.
- `CRAWLERSDATA-C20` (low): `dune_gno_supply` `8,821` rows, 3 labels, `0` null/zero, `0` dup grain; max block_date `2026-06-20` (`44h`, within its `48h` error_after override).
- `CRAWLERSDATA-C21` (low): `cow_api_trade_fees` FINAL `2,587,425` rows, `0` dup on `(order_uid,tx_hash,log_index)` grain (backed by `dbt_utils.unique_combination_of_columns`); `100,806` order_uid repeats are legitimate partial fills. Staleness carved to N01/C22.

### NEW (1)
- `CRAWLERSDATA-N01` (medium): discovered while re-measuring C21/C22. `crawlers_data.cow_api_trade_fees` last ingested `2026-06-16`, `~140h` stale as of `2026-06-21`, breaching its `48h` error_after (`sources.yml` lines 54-71). Clean cutoff (no partial trickle: `2026-06-16`=`336,015`, `2026-06-15`=`3,262,594`, `2026-06-14`=`2,328,646`, nothing after), grain still `0` dups — a freshness/coverage gap, not corruption. Attribution = other (cow ingestor stall, inferred from cutoff pattern + sources.yml "Ingested by click-runner (cow ingestor)" note, not log-confirmed). Corroborates Execution CoW REPORT P0-10. SAME failure as C22 — rate the medium once.

### UNVERIFIABLE / UNRESOLVED (0)
None. C19 was flagged UNVERIFIABLE in round 1 (verifier declined to re-trigger a known-erroring FINAL) and resolved to CONFIRMED in round 2 by executing it (code 181). All 23 cases settled.

## Evidence appendix

C01 — `SELECT count(DISTINCT toStartOfMonth(introduced_at)) AS distinct_months, min(...), max(...), count(*) FROM int_crawlers_data_labels` -> `94` distinct months, first=`17744` (`2018-08`), last=`20605` (`2026-05`), `5,452,340` rows. Read: `int_crawlers_data_labels.sql` line 3 `materialized='table'`, line 8 `partition_by='toStartOfMonth(introduced_at)'`.

C02 — `SELECT date, txs FROM crawlers_data.dune_bridge_flows LIMIT 1` -> code `47` UNKNOWN_IDENTIFIER "Unknown expression identifier `date`". `describe_table crawlers_data.dune_bridge_flows` = `{timestamp, bridge, source_chain, dest_chain, token, amount_token, amount_usd, net_usd}` (8 cols, no `date`/`txs`/`net_usd` ref needed). Model `tags=['dev','staging','dune','bridges','v2']`; cron/CI select `tag:production` only.

C03 — `SELECT countIf(np>1), countIf(np>1 AND dp>1) FROM (SELECT block_date,symbol,count(*) np,count(DISTINCT price) dp FROM crawlers_data.dune_prices GROUP BY block_date,symbol)` -> `39,206` rows, `36,629` distinct grain, `2,577` dup pairs, `2,576` divergent. Native overlap: `13,396/36,629` Dune-only; `842/2,576` divergent pairs are Dune-only. DESCRIBE = 3 cols (`block_date Date`, `symbol LowCardinality(String)`, `price Float64`). SQL line 11 `anyLast(toFloat64(price))`.

C04 — `SELECT max(introduced_at), dateDiff('hour',max(introduced_at),now()) FROM crawlers_data.dune_labels` -> max `2026-06-20 23:59:55`, `21h`. 30-day cadence: contiguous `20595..20624`, `609`-`67,087` rows/day. `sources.yml` dune_labels has no table-level freshness override (inherits `warn 18h`/`error 30h`). Freshness job: `run_dbt_observability.sh` line 154, `cron.sh` line 5 MANDATORY_STEPS.

C05 / C14 — `SELECT countIf(lower(project)='gpay'), countIf(project='Gnosis Pay') FROM int_crawlers_data_labels` -> `38,144` / `6`. Int total `5,452,340` rows = `5,452,340` distinct addresses. Guard `int_crawlers_data_labels.sql` lines 95-96 `lower(project)='gpay' DESC, project`; canon `stg_crawlers_data__dune_labels.sql` line 144 emits `'Gnosis Pay'` (no `^gpay$` rule).

C06 / C13 — `SELECT count(*), countIf(sector='DEX'), countIf(sector='Bridges') FROM int_crawlers_data_labels_dex JOIN int_crawlers_data_labels USING(address)` -> `11,844` / `3,269` (`27.6%`) / `98`. Blast-radius (48h fixed window): per-tx `int_live__dex_trades_raw FINAL` joined to `execution_live.transactions` on normalized `to_address`, joined to dex slice + label sector -> `1,542/7,710` aggregator-attributed; bridges `143` txs `$34,733.64`, DEX `1,395` txs `$584,877.07`; bridges = `9.3%` count / `5.6%` USD of attributed. schema.yml line 35 still "DEX-only ... restricted to sector = DEX".

C07 — Read `stg_crawlers_data__dune_labels.sql` final SELECT (lines 185-197) = 4 cols (`address, project, project_raw, introduced_at`); `staging/schema.yml` lines 12-74 list 13 phantom CTE cols. No `contract` block (`contract.enforced=false`).

C08 / C15 — `SELECT count(*), count(*)-count(DISTINCT (project,sector)) FROM fct_crawlers_data_distinct_projects_sectors` -> `305` / `0`. `fct...sql` lines 3-6 `materialized='table'`, `engine='ReplacingMergeTree()'`, `order_by='(project,sector)'`, no version col; line 10 `SELECT DISTINCT`. `api_..._totals.sql` lines 10-12 `countDistinct(project)/countDistinct(sector)`, no FINAL. `marts/schema.yml` not_null only, no unique test.

C09 — `requirements.txt`: `dbt-clickhouse==1.9.1`, `dbt-core==1.9.4`. Both int models `config(materialized='table', unique_key='address')` (lines 3, 5).

C10 — `SELECT count(*) FROM dbt.api_crawlers_data_distinct_projects_sectors_totals` -> `1` row. SQL line 7 `today() AS as_of_date`; lines 10-11 `value1=toFloat64(countDistinct(project))`, `value2=toFloat64(countDistinct(sector))`. `marts/schema.yml` line 44 as_of_date = "max date in the underlying data". `semantic_models.yml` lines 21-26 measures agg `sum`.

C11 / C17 / C20 — `SELECT count(*), count(DISTINCT label), groupUniqArray(label), countIf(supply IS NULL OR supply=0), count(*)-count(DISTINCT (label,block_date)), max(block_date) FROM crawlers_data.dune_gno_supply` -> `8,821` rows, `3` labels (`Gnosis Circ. Supply`, `Ethereum Circ. Supply`, `Non-Circ. Supply`), `0` null/zero, `0` dup grain, max `2026-06-20`. `stg...dune_gno_supply.sql` lines 10-14 passthrough (no `lower`); `api_crawlers_data_gno_supply_daily.sql` lines 7-11 passthrough; neither has an accepted_values test.

C12 — Read `int_crawlers_data_labels_dex.sql` final SELECT (lines 12-15) = `(address, project)`; `describe_table` confirms `introduced_at` absent. Sole consumer `api_execution_live_trades` joins `address`, reads `project`, no age filter.

C16 — Read `stg_crawlers_data__dune_prices.sql` (lines 1-15): `materialized='view'`, `tags=['production','staging','crawlers_data']`, no deprecation tag/comment. `staging/schema.yml` lines 78-98 no annotation.

C18 — `SELECT count(*), count(DISTINCT lower(address)) FROM crawlers_data.dune_labels` -> `5,467,745` / `5,452,340` (`15,405` dups). `SELECT count(*), count(DISTINCT address) FROM dbt.int_crawlers_data_labels` -> `5,452,340` / `5,452,340`.

C19 — `SELECT count(*) FROM crawlers_data.dune_labels FINAL` -> code `181` "Storage SharedMergeTree doesn't support FINAL. (ILLEGAL_FINAL)".

C21 / N01 — `SELECT count(*), count(*)-count(DISTINCT (order_uid,tx_hash,log_index)), count(*)-count(DISTINCT order_uid) FROM crawlers_data.cow_api_trade_fees FINAL` -> `2,587,425` / `0` / `100,806`. `SELECT toDate(ingested_at) d, count(*) ... GROUP BY d ORDER BY d DESC` -> `2026-06-16`=`336,015`, `2026-06-15`=`3,262,594`, `2026-06-14`=`2,328,646`, nothing after; `dateDiff('hour', max(ingested_at), now())` = `~140h`. `staging/schema.yml` declares `dbt_utils.unique_combination_of_columns: [order_uid, tx_hash, log_index]`. `sources.yml` lines 54-71 cow override `warn 36h`/`error 48h`.

C22 — Per-source max: `dune_labels 2026-06-20 23:59:55` (`21h`), `dune_prices 2026-06-20` (`44h`), `dune_gno_supply 2026-06-20` (`44h`), `circles_blacklisted 2026-06-21 03:00:03` (`17h`), `cow_api_trade_fees` ingested `2026-06-16` (`140h`, sole breach). Today `2026-06-21`.

## Review log (>=3 rounds per case)

- C01: r1 CONFIRMED high (code unchanged, 94 months) -> challenge: anchor convention text + system.parts cross-check -> r2 CONFIRMED (system.parts blocked by MCP SYSTEM guard; quoted `feedback_ch_cloud_partition_cap.md` verbatim, established per-INSERT cap by construction) -> challenge: confirm materialized='table' vs incremental determines reachability -> r3 CONFIRMED high (line 3 `materialized='table'` = single INSERT of all 94 partitions; reachability deterministic).
- C02: r1 CONFIRMED high (code_only, inferred from describe) -> challenge: execute the failing select + grep consumers/tags -> r2 CONFIRMED (code 47 executed; sole consumer dev-tagged) -> challenge: justify high vs medium given zero scheduled inclusion -> r3 CONFIRMED, re-rated medium (cron/CI select tag:production only; latent code bug, zero blast radius).
- C03: r1 CONFIRMED medium -> challenge: prove non-determinism observable -> r2 CONFIRMED (2,576 divergent dup pairs, WETH example) -> challenge: does divergence reach served numbers -> r3 CONFIRMED medium (842 Dune-only divergent pairs flow via priority-3 fallback; 36.6% feed is Dune-only).
- C04: r1 CONFIRMED medium -> challenge: pull 30-day cadence + confirm freshness job wired -> r2 CHANGED low (daily cadence refutes "permanent breach") -> challenge: confirm a freshness job actually runs -> r3 CHANGED low (job wired via run_dbt_observability.sh + cron.sh; intermittent WARN not chronic ERROR).
- C05: r1 CHANGED low (premise refuted, guard fires on 38,144) -> challenge: does split surface in served numbers -> r2 CHANGED low (2 distinct projects in tier0 KPI) -> r3 CONFIRMED medium (guard string permanently mismatched to canon output; split surfaces in countDistinct KPI; upheld at medium).
- C06: r1 CONFIRMED medium -> challenge: does any consumer assume sector='DEX' -> r2 CONFIRMED (sole consumer reads only lbl.project, no DEX assumption) -> challenge: is false description served anywhere -> r3 CONFIRMED, re-rated low (doc drift confined to schema.yml; no served surface).
- C07: r1 CONFIRMED medium -> challenge: does it break parse/contract/CI guard -> r2 CONFIRMED (contract.enforced=false, api_tag guard targets api_ not staging) -> challenge: does any surface render phantom cols -> r3 CONFIRMED, re-rated low (non-breaking doc drift, no served surface).
- C08: r1 CONFIRMED medium -> challenge: show real exposure via system.parts -> r2 CONFIRMED (system.parts blocked; table single-INSERT, 0 dups today) -> challenge: confirm no incremental/post-hook path -> r3 CONFIRMED, re-rated low (single-INSERT rebuild + SELECT DISTINCT; no realistic trigger).
- C09: r1 CONFIRMED low -> challenge: confirm adapter treats unique_key as ORDER-BY-only -> r2 CONFIRMED (pinned dbt-clickhouse==1.9.1, both materialized='table') -> r3 CONFIRMED low (no challenge; reconfirmed).
- C10: r1 CONFIRMED low -> challenge: how many rows does the view return (sum-vs-max) -> r2 CONFIRMED (1 row, sum==max latent) -> r3 CONFIRMED low (no challenge; reconfirmed today()/value1-value2/single-row).
- C11: r1 CONFIRMED low -> challenge: label-value stability over time -> r2 CONFIRMED (3 stable labels) -> r3 CONFIRMED low (no challenge; deduped with C17).
- C12: r1 CONFIRMED low -> challenge: any consumer needs introduced_at -> r2 CONFIRMED (sole consumer no age filter) -> r3 CONFIRMED low (no challenge; future-facing only).
- C13: r1 CONFIRMED high (98 addresses) -> challenge: quantify aggregator blast radius -> r2 CONFIRMED (structural path proven, volume not yet quantified — ephemeral live view) -> challenge: split aggregator volume Bridges-vs-DEX over fixed window -> r3 CONFIRMED high (9.3% count / 5.6% USD over 48h, above 1-2% bar).
- C14: r1 CHANGED low (premise refuted) -> challenge: simulate latent risk concretely -> r2 CONFIRMED medium (1 row/address, 0 collisions, fix is safe no-op) -> r3 CONFIRMED medium (no challenge; latent override hole for 6 uncovered rows).
- C15: r1 CONFIRMED medium -> challenge: show exposure window / part count -> r2 CONFIRMED (system.parts blocked; same as C08, countDistinct mitigates) -> challenge: does countDistinct neutralize RMT dup risk -> r3 CONFIRMED, re-rated low (countDistinct self-protects; rate ONCE with C08).
- C16: r1 CONFIRMED low -> challenge: confirm staging view still live/scheduled -> r2 CONFIRMED (materialized='view', tag production, live fallback) -> r3 CONFIRMED low (no challenge; no deprecation marker).
- C17: r1 CONFIRMED low -> challenge: dedup with C11 -> r2 CONFIRMED (same column/defect, api passthrough) -> r3 CONFIRMED low (no challenge; rate once with C11).
- C18: r1 CHANGED low (benign growth 13,423->15,405) -> challenge: confirm int collapses to source distinct -> r2 CONFIRMED (int rows = source distinct = 5,452,340) -> r3 CONFIRMED low (no challenge; dedup exact).
- C19: r1 UNVERIFIABLE low (declined to re-trigger error) -> challenge: run non-erroring SHOW CREATE / settle engine -> r2 CONFIRMED (FINAL -> code 181 ILLEGAL_FINAL; engine = SharedMergeTree) -> r3 CONFIRMED low (settled by error code; carried forward).
- C20: r1 CONFIRMED low -> challenge: anchor "within freshness" to config -> r2 CONFIRMED (44h vs 48h error_after override) -> r3 CONFIRMED low (no challenge; clean and fresh).
- C21: r1 CONFIRMED low (note: cow stale -> N01) -> challenge: tie grain to declared unique test -> r2 CONFIRMED (unique_combination_of_columns on full grain) -> r3 CONFIRMED low (no challenge; 0 dups, repeats are partial fills).
- C22: r1 CHANGED medium (cow D-5 stale) -> challenge: confirm same failure as N01, verify other 5 sources vs thresholds -> r2 CONFIRMED (only cow breaches; same as N01) -> r3 CHANGED medium (4/5 fresh, cow 140h; rate medium ONCE with N01; attribution other).
- N01: r1 NEW medium (140h stale, breaches 48h) -> challenge: substantiate attribution + clean cutoff + rule out incidents A/B -> r2 CONFIRMED (clean cutoff at 2026-06-16; A/B ruled out by mechanism+date; job-level root cause inferred not log-confirmed) -> r3 CONFIRMED medium (reconfirmed 140h, clean cutoff, 0 grain dups; SAME failure as C22, rate once).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 | ESCALATE/INVESTIGATE: restart the cow fees ingestor — `cow_api_trade_fees` is `~140h` stale, breaching `48h` error_after (clean cutoff `2026-06-16`); corroborates Execution CoW REPORT P0-10. Live freshness failure on a tier source feeding CoW fee marts. | `crawlers_data.cow_api_trade_fees` (source); cross-ref `sources.yml` lines 54-71 |
| P1 | KEEP: change `partition_by` from `toStartOfMonth(introduced_at)` to `toStartOfYear(introduced_at)` before the `94`-month history reaches the 100-partition full-refresh cap (~6 months of headroom; code 252). Per `feedback_ch_cloud_partition_cap.md`. | `models/crawlers_data/intermediate/int_crawlers_data_labels.sql` |
| P1 | KEEP: document or correct the "aggregator" definition — `98` Bridges-sector addresses carry `9.3%` of attributed tx count / `5.6%` of attributed USD in the live-trades feed. Either filter to true DEX/aggregator sectors or formally document the broad interpretation. | `models/crawlers_data/intermediate/int_crawlers_data_labels_dex.sql`, `models/execution/live/marts/api_execution_live_trades.sql` |
| P2 | KEEP: harden the gpay dedup guard — change `lower(project)='gpay'` to `lower(project) IN ('gpay','gnosis pay')` (safe no-op on current data; closes the latent override hole for the 6 canonical `Gnosis Pay` rows). Consider unifying `gpay`/`Gnosis Pay` to one canonical project to fix the tier0 KPI split. | `models/crawlers_data/intermediate/int_crawlers_data_labels.sql` (lines 95-96); `stg_crawlers_data__dune_labels.sql` line 144 |
| P2 | KEEP: replace `anyLast(price)` with a deterministic tie-break (or fix upstream Dune ETL to add an ingestion timestamp for `argMax`). `2,576` divergent dup pairs; `842` Dune-only divergent pairs reach served prices. | `models/crawlers_data/staging/stg_crawlers_data__dune_prices.sql` |
| P3 | KEEP: fix/remove the broken dev model `stg_crawlers_data__dune_bridge_flows_v2` (references non-existent `date`/`txs` on a tx-level source; code 47). Low urgency — dev-tagged, never selected by cron/CI — but it will break if promoted. | `models/crawlers_data/staging/stg_crawlers_data__dune_bridge_flows_v2.sql` |
| P3 | KEEP: add a per-table freshness override on `dune_labels` (`warn 7d`/`error 8d`, or align with observed daily cadence) so it stops intermittently tripping the inherited `18h` WARN. | `models/crawlers_data/sources.yml` (dune_labels block) |
| P3 | KEEP (doc/hardening cluster): fix `int_crawlers_data_labels_dex` schema.yml "DEX-only" description (C06); trim 13 phantom CTE columns from `stg_crawlers_data__dune_labels` schema.yml (C07); fix `api_..._totals` as_of_date doc + rename `value1/value2` + change semantic agg `sum`->`max` (C10); add `lower(label)` + accepted_values on `dune_gno_supply.label` (C11/C17, rate once); add a deprecation/fallback note on `stg_crawlers_data__dune_prices` (C16). | crawlers_data intermediate/marts/staging schema.yml + listed models |
| P4 | KEEP (low/latent cleanup): add a unique_combination test on `(project,sector)` for `fct_crawlers_data_distinct_projects_sectors` and/or document the RMT-no-version design (C08/C15, rate once); add an explanatory comment that `unique_key` is ORDER-BY-only here (C09); optionally retain `introduced_at` in the dex slice for future label-age filters (C12). | `fct_crawlers_data_distinct_projects_sectors.sql`, `api_crawlers_data_distinct_projects_sectors_totals.sql`, both int label models |
| — | DROP: none. No case resolved; nothing to retire. | — |
