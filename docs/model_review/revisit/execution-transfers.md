# Model review (revisit 2026-06-21): execution/transfers

Baseline `docs/model_review/execution-transfers.md` (dated `2026-06-11`); `18` cases re-verified over `3` rounds. Headline: `14` still confirmed, `2` changed (staleness recovered, defects persist), `1` resolved, `0` new — the critical `join_use_nulls` bridge defect (`C01`/`C09`/`C16`) remains fully unfixed.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONTRANSFERS-C01 | - | bridges LEFT JOIN with no `join_use_nulls=1`; unmatched cols `''` not NULL, direction always `out` | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONTRANSFERS-C02 | - | whitelisted_daily schema.yml lists 5 phantom cols; `amount_raw` undocumented; `transfer_count` typed String not UInt64 | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONTRANSFERS-C03 | - | whitelisted_daily decodes uint256 with signed `reinterpretAsInt256`/`toInt256` (latent sign-flip > 2^255) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONTRANSFERS-C04 | - | whitelisted_raw carries `dev` tag, 0 ref() consumers, yet full schema+semantic entry — orphan | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTRANSFERS-C05 | - | bridges append-only watermark `date > max(date)`; label-set changes never backfilled, no rebuild doc | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTRANSFERS-C06 | - | bridges 8 days stale vs whitelisted_daily | medium | CHANGED | low | high | microbatch_insert_overwrite | 3 |
| EXECUTIONTRANSFERS-C07 | - | date window guard duplicated in INNER JOIN ON and WHERE (dead code) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTRANSFERS-C08 | - | whitelisted_raw schema.yml documents 4 phantom cols absent from SELECT | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTRANSFERS-C09 | - | Graph Explorer bridge_user_flows + semantic model serve whole whitelisted set as outbound (consequence of C01) | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONTRANSFERS-C10 | - | semantic volume gap: `volume_usd` hardcoded NULL & registered as weight; real `amount_raw_sum` unregistered | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONTRANSFERS-C11 | - | WxDAI wrap double-count (Deposit mint + Transfer) — unconfirmed | medium | RESOLVED | resolved | high | none | 3 |
| EXECUTIONTRANSFERS-C12 | - | all metrics `quality_tier: candidate`; nonsensical auto-measures `decimals_sum`/`transaction_index_sum` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTRANSFERS-C13 | - | composite `from:to:token:date` entity named `address` collides with bare-wallet `address` entity | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTRANSFERS-C14 | - | ~4.5% of daily rows `amount_raw=0` inflate `transfer_count` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTRANSFERS-C15 | - | whitelisted_daily ~20.4M rows, intentional buffer, 0 nulls, 0 grain dupes | low | CONFIRMED | low | high | microbatch_insert_overwrite | 3 |
| EXECUTIONTRANSFERS-C16 | - | bridges 6.16M rows, stale, 100% NULL volume_usd, 100% `out`, 98.6% empty bridge_contract | high | CHANGED | high | high | microbatch_insert_overwrite | 3 |
| EXECUTIONTRANSFERS-C17 | - | 0 negative `amount_raw`; logs.data has no 0x prefix (unhex correct); raw scan timed out | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTRANSFERS-C18 | - | GPay spender `0x4822...72ee` hardcoded in 3 model files, not a var/seed; no bridge seed | low | CONFIRMED | low | high | none | 3 |

## Delta vs baseline

### RESOLVED (1)
- `EXECUTIONTRANSFERS-C11` — WxDAI wrap double-count disproven. Over May+June, `0` of `2,683,651` WxDAI Transfer events have `src=0x0` despite `16,227` Deposits in May alone; the synthesized Deposit mint leg (`from='0x0'` hardcoded, `int_execution_transfers_whitelisted_daily.sql` lines ~110-119) and the unioned Transfer leg (`from=decoded_params['src']`, lines ~133-143) can never collide on the `(from,to)` grain. No double-count is structural, not lucky. Incident: none.

### CHANGED (2) — staleness recovered, logic defects persist
- `EXECUTIONTRANSFERS-C06` — staleness materially improved: bridges lag shrank from `8 days` (baseline `2026-06-03` vs `2026-06-07`) to a stable `2 days` (`2026-06-15` vs whitelisted_daily `2026-06-17`), with contiguous daily coverage and no gaps. The `2-day` gap is now the deterministic design buffer (`block_timestamp < today()` ~1-day buffer + append watermark `date > max(date)`), not a scheduling break. Severity lowered `medium -> low`. Incident attribution: `microbatch_insert_overwrite` June re-run advanced dates only.
- `EXECUTIONTRANSFERS-C16` — mixed: the staleness sub-claim is fixed (`max(date)` advanced `2026-06-03 -> 2026-06-15`, contiguous daily), but the three join/USD logic defects persist across ALL `6,311,866` rows: `volume_usd` `100%` NULL, `direction='in'` = `0` (100% `out`), `bridge_contract=''` = `98.45%` on the latest day. `git log` on the bridges SQL shows only commits `0d261e1f` (`2026-06-02`) and `fe0b4491` (`2026-04-17`), both predating the `2026-06-11` baseline — so incident-A advanced the data only; the defects are pre-existing and untouched. Severity stays `high`. Incident: `microbatch_insert_overwrite` (date advance only).

### STILL CONFIRMED (14)
- `EXECUTIONTRANSFERS-C01` (critical) — root cause intact. `6,311,866` rows (was `6,160,919`), `direction='in'` = `0` across full history, `bridge_contract IS NULL` = `0` (empties are `''`, the `join_use_nulls=0` signature). On `2026-06-15`, `6,737` of `6,843` rows (`98.45%`) have `bridge_contract='' AND bridge_name=''`, all `out`. No `SET join_use_nulls=1` pre_hook in config (lines 1-10). Incident: none.
- `EXECUTIONTRANSFERS-C09` (critical) — direct consequence of C01 unchanged. On `2026-06-15` the `bridge_user_flows` graph profile (`weight_column: volume_usd`) serves `6,843` edges of which only `106` (`1.55%`) have a real `bridge_contract`; the other `6,737` (`98.45%`) are the empty-string join artifact, all `out`, weight `100%` NULL. Incident: none.
- `EXECUTIONTRANSFERS-C10` (high) — `volume_usd = CAST(NULL AS Nullable(Float64))` (line 50), `100%` NULL over `6,311,866` rows; only `volume_usd_value` + `transfer_count_value` registered as measures; `amount_raw_sum` carries real signal (`6,249,309` of `6,311,866` rows `>0`, max `1.51e27`) but is unregistered (`grep amount_raw_sum semantic/` = 0 hits). Incident: none.
- `EXECUTIONTRANSFERS-C02` (high) — schema.yml lists 5 phantom cols (`decimals`/`date_start`/`date_end`/`amount`/`amount_usd`) absent from the 7-col live table; `transfer_count` documented String but is `UInt64`, `date` documented DateTime but is `Date`; `amount_raw` (`Nullable(Int256)`) undocumented. Pure doc rot — only test is `elementary.schema_changes` (warn) + a grain `unique_combination` test; no consumer selects the phantoms. Incident: none.
- `EXECUTIONTRANSFERS-C03` (high) — `reinterpretAsInt256` (line 63) and `toInt256` (lines 116/128/140) vs sibling `reinterpretAsUInt256`. Dormant: head-to-head on `2026-06-15` gives `0` disagreements over `11,776` overlapping non-WxDAI rows; `countIf(toInt256 != toUInt256)=0` over `221,599` raw rows; max `amount_raw` ~`3.06e27` is far below `2^255` (~`5.79e76`). `0` negatives. Incident: none.
- `EXECUTIONTRANSFERS-C04` (medium) — still `tags=['dev',...]` (line 21), `0` ref() consumers, full schema.yml + semantic entry. Table materializes (`229,657,570` rows, max date `2026-06-15`) — a built, refreshed orphan; cron_preview.sh / scripts/refresh contain no whitelisted_raw or dev-tag selector, so it is refreshed as collateral of a broader path. Incident: none.
- `EXECUTIONTRANSFERS-C05` (medium) — append-only watermark `date > (SELECT max(date) FROM this)` (line 38), present in `scripts/checks/no_delete_insert.allow` (line 11), no rebuild doc. Demonstrated: `93` of `104` labeled bridges absent from flows over 30 days; labeled-but-absent addresses `0x24afdca4...`, `0x5c32143c...`, `0x3aa637d6...` DO appear as from/to in whitelisted_daily on recent dates — concrete backfill failures. Incident: none.
- `EXECUTIONTRANSFERS-C12` (medium) — `decimals_value`/`transaction_index_value`/`log_index_value` registered as sum measures AND candidate simple metrics with full `allowed_dimensions`/time grains; all transfers metrics `quality_tier: candidate`. Incident: none.
- `EXECUTIONTRANSFERS-C13` (low) — composite `concat(ifNull(from,''),':',ifNull(to,''),':',token_address,':',toString(date))` primary entity still named `address`, name-colliding with bridges' bare-wallet `address` entity. Latent: the only relationship `transfer_endpoint_dune_label` joins on `left_keys ['from']`, not the composite entity, so no query path unifies them. Incident: none.
- `EXECUTIONTRANSFERS-C07` (low) — window guard duplicated in INNER JOIN ON (raw `block_timestamp`, lines 69-70) and WHERE (`toDate(block_timestamp)`, lines 72-73). Benign: all 46 tokens_whitelist bounds are pure midnight; row-level differential on `2026-06-15` returns `0` divergent rows. Dead code. Incident: none.
- `EXECUTIONTRANSFERS-C08` (low) — whitelisted_raw schema.yml documents `token_address_raw`/`symbol_upper`/`date_start`/`date_end` (lines 78/90/94/98), all absent from the materialized 14-col table. Pure doc rot (only `elementary.schema_changes` warn). Incident: none.
- `EXECUTIONTRANSFERS-C14` (low) — zero-amount share recomputed `5.39%` (`29,398` of `544,845` over 30d, was `4.5%`). Genuine zero-value Transfer logs: on-chain spot check (block `46751128`, log `158`, WETH) has `data` all-zeros, `reinterpretAsUInt256(reverse(unhex(data)))=0`. Inflates `transfer_count` only `0.48%`. Incident: none.
- `EXECUTIONTRANSFERS-C15` (low) — `20,645,855` rows (was ~20.4M), max `2026-06-17` (4-day intentional buffer), `0` nulls on `amount_raw`/`transfer_count`, `0` duplicate groups WITH FINAL month-wide. Healthy. Incident: `microbatch_insert_overwrite` (date advance).
- `EXECUTIONTRANSFERS-C17` (low) — `0` negative `amount_raw` over 30d; logs.data sample (block `46751128`) is bare hex, no 0x prefix (unhex correct); whitelisted_raw now materializes within budget (`229,657,570` rows, `0` negatives, full sign-agreement). Ties to C03 dormant risk. Incident: none.
- `EXECUTIONTRANSFERS-C18` (low) — spender `0x4822521e6135cd2599199c83ea35179229a172ee` hardcoded in `4` byte-identical model files (one more than baseline's 3: `fct_execution_gpay_payments_hourly.sql:57` added), none wrapped in `var()`; `dbt_project.yml` has a `vars:` block and seeds/ has address-mapping CSVs, so centralization is actionable. No bridge addresses in any seed. Incident: none.

### NEW (0)
- None.

### UNVERIFIABLE / UNRESOLVED (0)
- None. Two minor angles were explicitly budget-deferred (C09 and C12 live `query_metrics`/`discover_metrics` calls) but settled on the underlying-table distribution and authored YAML, which are decisive on their own.

## Evidence appendix

### C01 / C09 / C16 (shared bridges defect — join_use_nulls)
- Code: `models/execution/transfers/intermediate/int_execution_bridges_address_flows_daily.sql` — config lines 1-10 have NO `join_use_nulls` pre/post_hook; `coalesce(ba_to.address, ba_from.address)` (lines 43-44); `direction = if(ba_to.address IS NOT NULL,'out','in')` (line 48); `amount_raw_sum` (line 49); `volume_usd = CAST(NULL AS Nullable(Float64))` (line 50); two LEFT JOINs (lines 53-54); `WHERE ... IS NOT NULL` guard (line 55); append watermark (line 38).
- SQL: `SELECT count(), countIf(direction='in'), countIf(direction='out'), countIf(volume_usd IS NULL) FROM dbt.int_execution_bridges_address_flows_daily` -> `6,311,866` rows; `0` in; `6,311,866` out; `6,311,866` NULL volume_usd.
- SQL (latest day `2026-06-15`): `count()=6,843`; `countIf(bridge_contract='' AND bridge_name='')=6,737` (`98.45%`); real bridges `106` (`1.55%`); all `out`.
- SQL (sentinel): `countIf(bridge_contract='')=5,895,195`, `countIf(bridge_contract IS NULL)=0` — empties are `''`, proving `join_use_nulls=0`.
- SQL (signal): `countIf(amount_raw_sum>0)=6,249,309` of `6,311,866`, max `1.51e27`.
- `git log` on the bridges SQL: only `0d261e1f` (`2026-06-02` 'large refactor') and `fe0b4491` (`2026-04-17`), both predate baseline.

### C02 / C08 (schema drift)
- `describe_table dbt.int_execution_transfers_whitelisted_daily` -> 7 cols: `date Date, token_address String, symbol String, from Nullable(String), to Nullable(String), amount_raw Nullable(Int256), transfer_count UInt64`. `decimals/date_start/date_end/amount/amount_usd` absent.
- `SELECT toTypeName(transfer_count), toTypeName(amount_raw), toTypeName(date) ...` -> `UInt64`, `Nullable(Int256)`, `Date` (vs documented String/—/DateTime).
- `describe_table dbt.int_execution_transfers_whitelisted_raw` -> 14 cols; `token_address_raw/symbol_upper/date_start/date_end` absent (CTE-internal only).

### C03 / C17 (signed decode, dormant)
- `SELECT countIf(amount_raw<0) FROM dbt.int_execution_transfers_whitelisted_daily` -> `0` (30d window `544,845` rows).
- Head-to-head on `2026-06-15`: `11,776` overlapping non-WxDAI rows, `0` disagreements; `countIf(toInt256(value_raw)!=toUInt256(value_raw))=0` over `221,599` raw rows.
- max `amount_raw` = `3,057,526,169,450,000,691,131,842,560` (~`3.06e27`) << `2^255` (~`5.79e76`).
- whitelisted_raw: `229,657,570` rows, `0` negative value_raw; logs.data sample (block `46751128`, log `158`) length 64, no 0x prefix.

### C04 (orphan)
- `grep ref('int_execution_transfers_whitelisted_raw') models/` -> 0 consumers. `tags=['dev','execution','transfers','erc20','whitelisted']` (line 21). Table: `229,657,570` rows, max `2026-06-15`. No whitelisted_raw/dev selector in cron_preview.sh or scripts/refresh.

### C05 (append-watermark backfill failure)
- 104 bridge-labeled addresses; `93` never appear as `bridge_contract` over 30d. Sampled labeled-but-absent: `0x24afdca4653042c6d08fb1a754b2535dacf6eb24`, `0x5c32143c8b198f392d01f8446b754c181224ac26`, `0x3aa637d6853f1d9a9354fe4301ab852a88b237e7` appear as from/to in whitelisted_daily on dates `20596-20624` (`2026-06-04..2026-06-17`).

### C06 / C15 / C16 (freshness)
- `SELECT max(date) FROM whitelisted_daily` = `2026-06-17` (`20624`); `SELECT max(date) FROM bridges` = `2026-06-15` (`20622`); lag `2 days`. Bridges contiguous `2026-06-04..2026-06-15`.
- C15: `count()=20,645,855`; max `2026-06-17`; `0` nulls; dup check WITH FINAL on `(date,token_address,from,to)` for current month = `0`.

### C07 (dead code)
- Row-level differential over execution.logs x tokens_whitelist for `2026-06-15`: `0` rows where ON-predicate (`block_timestamp` bounds) and WHERE-predicate (`toDate` bounds) disagree. tokens_whitelist: 46 tokens, `0` non-midnight bounds.

### C10 / C12 / C13 (semantic layer)
- `semantic/authoring/bridges/semantic_models.yml`: measures block registers only `volume_usd_value` + `transfer_count_value`; `amount_raw_sum` not registered; `bridge_user_flows` profile `weight_column: volume_usd` (line ~557). `address` entity = `user_address`.
- whitelisted_daily semantic: measures register only `transfer_count_value`; entity `address` = `concat(ifNull(from,''),':',ifNull(to,''),':',token_address,':',toString(date))`.
- whitelisted_raw semantic: `decimals_value`/`transaction_index_value`/`log_index_value` as sum measures + candidate simple metrics. All transfers metrics `quality_tier: candidate`.
- Only cross-model relationship touching the composite entity: `transfer_endpoint_dune_label` (`execution_graph.yml` lines ~111-120), joins `left_keys ['from']` not the composite entity.

### C11 (WxDAI, resolved)
- `SELECT countIf(event_name='Transfer' AND decoded_params['src']='0x000...000') FROM dbt.contracts_wxdai_events` over May+June -> `0` of `2,683,651` Transfer events; May had `16,227` Deposits, `1,519,694` Transfers, `0` with src=0x0.

### C14 (zero-amount)
- `SELECT count(), countIf(amount_raw=0), sum(transfer_count), sumIf(transfer_count, amount_raw=0) FROM whitelisted_daily WHERE date>=today()-30` -> `544,845` rows; `29,398` zero (`5.39%`); total transfer_count `7,344,275`; zero-bucket `35,144` (`0.48%`). On-chain: block `46751128` log `158` data all-zeros -> decodes to `0`.

### C18 (hardcoded config)
- `grep -rino 0x4822521e6135cd2599199c83ea35179229a172ee models/ --include=*.sql` -> 4 byte-identical lowercase hits: `int_execution_gpay_activity.sql:29`, `int_execution_gpay_wallets.sql:15`, `fct_execution_gpay_payments_hourly.sql:57`, `int_revenue_gpay_fees_daily.sql:1`. None wrapped in `var()`. `dbt_project.yml` has `vars:` (line 10); seeds/ has address-mapping CSVs; no bridge address in any seed.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED (6.31M rows, 0 'in', no pre_hook) -> challenge: prove mechanism via join sentinel (`''` vs NULL) and re-read config -> R2 CONFIRMED (`bridge_contract=''`=5,895,195, NULL=0; no pre_hook) -> challenge: count both-legs-failed rows and tie to always-'out' -> R3 CONFIRMED (6,737/6,843 both-empty, all 'out'). Settled critical.
- **C02**: R1 CONFIRMED (code+warehouse) -> challenge: paste describe_table for amount_raw/transfer_count, confirm 5 absent -> R2 CONFIRMED (7 cols, types match) -> challenge: is drift test/consumer-breaking? -> R3 CONFIRMED (pure doc rot, only elementary.schema_changes warn). Settled high.
- **C03**: R1 CONFIRMED (signed decode, 0 neg) -> challenge: quantify exposure vs 2^255, check sibling sign agreement -> R2 CONFIRMED (max 3.06e27 << 5.79e76) -> challenge: head-to-head on highest day -> R3 CONFIRMED (0 disagreements over 11,776 rows). Settled high (dormant).
- **C04**: R1 CONFIRMED (dev tag, 0 ref) -> challenge: does it materialize? -> R2 CONFIRMED (5.18M current-month rows) -> challenge: resolve dev-tag vs fresh-rows refresh path -> R3 CONFIRMED (no explicit selector; refreshed as collateral, 229.6M rows). Settled medium.
- **C05**: R1 CONFIRMED (watermark + allow-list, no rebuild doc) -> challenge: demonstrate empirically -> R2 CONFIRMED (71/104 absent) -> challenge: distinguish backfill-failure from no-activity -> R3 CONFIRMED (93/104 absent; 3 sampled have whitelisted transfers but no flows row). Settled medium.
- **C06**: R1 CHANGED (lag 8d->2d) -> challenge: confirm durable steady-state, not transient -> R2 CHANGED (2-day, contiguous) -> challenge: tie lag to today()-buffer + watermark, not catch-up -> R3 CONFIRMED-as-CHANGED (stable structural 2-day). Settled low; incident A.
- **C07**: R1 CONFIRMED (ON/WHERE duplicate, differ) -> challenge: any non-midnight bound? -> R2 CONFIRMED (all 46 midnight) -> challenge: row-level differential on real data -> R3 CONFIRMED (0 divergent rows). Settled low.
- **C08**: R1 CONFIRMED (4 phantom cols) -> challenge: describe_table confirm absent -> R2 CONFIRMED (14-col table, 4 absent) -> challenge: any test/consumer hard-fail? -> R3 CONFIRMED (pure doc rot). Settled low.
- **C09**: R1 CONFIRMED (98.45% empty, all 'out') -> challenge: show served graph edge query with NULL weight -> R2 CONFIRMED (weight_column volume_usd 100% NULL) -> challenge: quantify valid-edge fraction -> R3 CONFIRMED (106/6,843 = 1.55% valid). Settled critical.
- **C10**: R1 CONFIRMED (CAST(NULL), amount_raw_sum unregistered) -> challenge: query semantic measure returns NULL + confirm no measure -> R2 CONFIRMED (only volume_usd_value/transfer_count_value) -> challenge: prove amount_raw_sum has real signal -> R3 CONFIRMED (6.25M rows >0, max 1.51e27). Settled high.
- **C11**: R1 RESOLVED (0 Transfer src=0x0, 11d) -> challenge: extend to full month -> R2 RESOLVED (May: 0 of 1.52M) -> challenge: confirm at model layer (union legs can't collide) -> R3 RESOLVED (0 of 2.68M; structural). Settled resolved.
- **C12**: R1 CONFIRMED (nonsensical sum measures, all candidate) -> challenge: confirm registry-discoverable -> R2 CONFIRMED (simple metrics with dims) -> challenge: live discover_metrics (budget-deferred) -> R3 CONFIRMED (authored as query-able simple metrics). Settled medium.
- **C13**: R1 CONFIRMED (composite 'address' entity) -> challenge: does another model name 'address' as bare wallet? -> R2 CONFIRMED (bridges uses user_address) -> challenge: is collision join-reachable? -> R3 CONFIRMED (latent; only relationship joins on 'from'). Settled low.
- **C14**: R1 CONFIRMED (5.40%) -> challenge: sample raw logs + quantify count inflation -> R2 CONFIRMED (0.48% of transfer_count) -> challenge: on-chain spot check decodes to 0 -> R3 CONFIRMED (block 46751128 all-zeros). Settled low.
- **C15**: R1 CONFIRMED (20.65M, 0 nulls/dupes) -> challenge: month-wide grain dup check -> R2 CONFIRMED (0 dups month-wide) -> challenge: re-check WITH FINAL -> R3 CONFIRMED (0 under FINAL). Settled low; incident A.
- **C16**: R1 CHANGED (date recovered, 3 defects persist) -> challenge: confirm defects across full history -> R2 CHANGED (100% NULL/out/93-98% empty full-history) -> challenge: git attribution boundary -> R3 CHANGED (only pre-baseline commits; incident A advanced data only). Settled high.
- **C17**: R1 CONFIRMED (0 neg, no 0x prefix) -> challenge: decode one high-value row both ways -> R2 CONFIRMED (analytic) -> challenge: fetch concrete on-chain row -> R3 CONFIRMED (logs.data bare hex; raw now materializes 229.6M). Settled low.
- **C18**: R1 CONFIRMED (4 files, not 3) -> challenge: vars block + seed pattern exist? -> R2 CONFIRMED (vars block + address seeds exist) -> challenge: confirm byte-identical (no drift) -> R3 CONFIRMED (4 identical lowercase, no var()). Settled low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 — ESCALATE | Add `SET join_use_nulls=1` pre_hook to the bridges model, then full-rebuild history. The `'in'` direction has never existed (`0` of `6,311,866` rows) and `98.45%` of latest-day rows are the empty-string join artifact. This unblocks C01, C09 (Graph Explorer bridge_user_flows is unusable), and C16 in one fix. | `models/execution/transfers/intermediate/int_execution_bridges_address_flows_daily.sql` |
| P1 — KEEP | Stop hardcoding `volume_usd = CAST(NULL ...)` and register `amount_raw_sum` as a semantic measure. Real volume signal exists (`6,249,309` of `6,311,866` rows `>0`) but every semantic/MCP volume query returns NULL. | `int_execution_bridges_address_flows_daily.sql`, `semantic/authoring/bridges/semantic_models.yml` |
| P1 — KEEP | Replace signed `reinterpretAsInt256`/`toInt256` with unsigned `reinterpretAsUInt256`/`toUInt256` to match the sibling raw model. Latent today (max `3.06e27` << `2^255`) but a single value above `2^255` flips sign and turns `sum(amount_raw)` negative. | `int_execution_transfers_whitelisted_daily.sql` |
| P2 — KEEP | Document and schedule a rebuild procedure for the append-only watermark; `93` of `104` labeled bridges are absent from flows and label-set changes are never backfilled. | `int_execution_bridges_address_flows_daily.sql`, `scripts/checks/no_delete_insert.allow` |
| P2 — KEEP | Promote-or-delete the dev-tagged orphan: `0` ref() consumers yet `229,657,570` rows materialized at cost with a full schema + semantic entry. | `int_execution_transfers_whitelisted_raw.sql`, `semantic/authoring/execution/transfers/semantic_models.yml` |
| P3 — KEEP | Fix schema.yml drift: remove `5` phantom cols (daily) and `4` phantom cols (raw), document `amount_raw`, correct `transfer_count` (String->UInt64) and `date` (DateTime->Date). Pure doc rot today but misleads consumers. | `models/execution/transfers/intermediate/schema.yml` |
| P3 — KEEP | Prune nonsensical auto-measures `decimals_sum`/`transaction_index_sum`/`log_index_sum`; nothing in the sector is promotion-ready (`quality_tier: candidate`). | `semantic/authoring/execution/transfers/semantic_models.yml` |
| P3 — KEEP | Rename or scope the composite `address` entity to avoid name collision with bridges' bare-wallet `address`; latent today (no cross-model join path) but a footgun. | `semantic/authoring/execution/transfers/semantic_models.yml` |
| P3 — KEEP | Remove the duplicated date-window predicate from the WHERE clause (equivalent to the INNER JOIN ON; `0` divergent rows). | `int_execution_transfers_whitelisted_daily.sql` |
| P3 — KEEP | Centralize the GPay spender `0x4822...72ee` as a dbt var/seed; `4` byte-identical hardcoded copies (drift risk). | `int_execution_gpay_wallets.sql`, `int_execution_gpay_activity.sql`, `int_revenue_gpay_fees_daily.sql`, `fct_execution_gpay_payments_hourly.sql` |
| INFO | C14 zero-amount rows (`5.39%`, `0.48%` of transfer_count) are genuine zero-value Transfer logs — acceptable; optionally filter for count-based KPIs. | `int_execution_transfers_whitelisted_daily.sql` |
| DROP | WxDAI wrap double-count concern (C11) — resolved; no further action. `0` of `2,683,651` WxDAI Transfers have `src=0x0`. | `int_execution_transfers_whitelisted_daily.sql` |
| MONITOR | Bridges staleness (C06/C15) recovered to a stable `2-day` structural buffer via the June `microbatch_insert_overwrite` re-run — no action, monitor only. | `int_execution_bridges_address_flows_daily.sql`, `int_execution_transfers_whitelisted_daily.sql` |
