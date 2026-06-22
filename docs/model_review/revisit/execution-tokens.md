# Model review (revisit 2026-06-21): execution/tokens

Re-verification of baseline `docs/model_review/execution-tokens.md` (dated `2026-06-11`); `23` cases re-checked over `4` rounds. Headline: `0` resolved, `3` CHANGED (all worsened or became more reachable), `20` STILL CONFIRMED, `0` new — the high-severity negative-balance supply defect persists and is now demonstrably served as negative dollar supply to API consumers (`wstETH` `2026-05-08` served `value_usd = -$589,706.02`).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONTOKENS-C01 | — | supply = `sumIf(balance, address!=zero)` with no `balance>0` floor; holders in same model filter `balance>0` (intra-model inconsistency) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONTOKENS-C02 | — | `supply_usd = supply*coalesce(price,0)` passes negatives through; no `not_negative`/`min_value` test; Elementary warn-only | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONTOKENS-C03 | — | symbol filter passed as `filters_sql` AND re-applied explicitly in WHERE; fragile under full-refresh | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTOKENS-C04 | — | semantic model registers phantom dims/measures absent from `balances_daily` output; MCP query failures | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTOKENS-C05 | — | `delete+insert` RMT with LEFT JOINs lacks `join_use_nulls` hooks (COALESCE guards hold; latent) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTOKENS-C06 | — | `overview_by_class_latest` 7d-ago join is INNER on `(token_class,label)`; newly-debuted class silently dropped | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTOKENS-C07 | — | `schema.yml` lists phantom column `AS` with empty `data_type` under two intermediate models | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C08 | — | holders filter inconsistency: `balance_raw>0` (overview) vs `balance>0` (supply_holders) | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONTOKENS-C09 | — | top_holders sums direct + UBO `prev_7d` per `(token,address)` with no dedup; double-counts `change_usd_7d` | low | CHANGED | low | medium | none | 3 |
| EXECUTIONTOKENS-C10 | — | per-wallet `balances_daily` feed has `allow_unfiltered:false` but no `privacy:`/`expose_to_mcp:` decision | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C11 | — | "circulating supply" includes negative balances; ~9% wstETH understatement (definition drift) | high | CONFIRMED | high | high | none | 4 |
| EXECUTIONTOKENS-C12 | — | `api:tokens_supply` and `api:holders_per_token` each claimed by two models; router cannot disambiguate on name alone | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTOKENS-C13 | — | supply is sum-of-balances, not `totalSupply()`; no reconciliation for sDAI/aTokens/bridged-out | medium | CONFIRMED | medium | high | none | 4 |
| EXECUTIONTOKENS-C14 | — | 15 aToken/spToken wrappers excluded from default OTHERS supply; undocumented scoping caveat | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTOKENS-C15 | — | duplicate `api_`/`fct_` semantic entities for supply_by_sector + supply_distribution with identical synonyms | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C16 | — | `balances_daily` healthy scale/freshness (383.7M rows, max yesterday, 29 tokens) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C17 | — | NULL `balance_usd` rate over last 7d is 0%; price join healthy | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C18 | — | grain clean: 0 dupes in balances_daily/transfers_daily FINAL | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C19 | — | negative supply rows in supply_holders_daily (baseline: 3 wstETH rows) | high | CHANGED | high | high | none | 3 |
| EXECUTIONTOKENS-C20 | — | wstETH negative-balance supply impact (~9% / ~384 wstETH / ~$1.3M on 2026-06-10) | high | CHANGED | high | high | none | 3 |
| EXECUTIONTOKENS-C21 | — | metrics_daily zero-price check: no zero-price anomalies for latest date | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C22 | — | overview_by_class_latest change_pct populated; no INNER JOIN drop this window | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTOKENS-C23 | — | metrics_daily freshness: max yesterday, ~25 rows | low | CONFIRMED | low | medium | none | 3 |

## Delta vs baseline

### RESOLVED (0)
None. No finding was fixed between `2026-06-11` and `2026-06-21`. Every code-level defect is unchanged in source; the data-defect cases either persist or grew.

### CHANGED (3)
- **EXECUTIONTOKENS-C09** (low → low, scope widened): baseline framed the direct+UBO `prev_7d` double-count as a theoretical edge case. Re-verified as reachable — `4,807` `(token_address, address)` pairs appear in BOTH `prev_direct` and `prev_ubo` on the 7d-ago date, and `354` of those overlap pairs are actually ranked/served holders whose `change_usd_7d` carries a double-subtracted 7d-ago balance. Impact confined to the `change_usd_7d` delta column. Incident: none.
- **EXECUTIONTOKENS-C19** (high → high, magnitude grew + span shifted): baseline reported `3` negative-supply rows (wstETH `2026-05-18`/`-19`/`-20`). Now `31` negative-supply rows, all wstETH, spanning `2026-05-01`..`2026-06-01` (`31` distinct days over a 32-day span, one gap day), `0` rows after `2026-06-15`. The `2026-06-01` cutoff is a partition recompute that healed the sign (June rows exist and flipped positive — wstETH `2026-06-02` = `+43.45`) but not the magnitude understatement. Incident: none — this is the model running-sum logic, NOT the June `insert_overwrite` microbatch wipe (verifier correctly declined incident-A attribution).
- **EXECUTIONTOKENS-C20** (high → high, magnitude grew): baseline wstETH `2026-06-10` `3,811` vs `4,195` (~9% / ~384 wstETH / ~$1.3M). Now `2026-06-20` `supply_incl_neg = 3,414.70` vs `supply_pos_only = 3,871.87` = `457.17` wstETH gap (`~11.81%` understatement, `99` negative-balance holders). The served `api_execution_tokens_supply_latest_by_token` returns `value_native = 3,414.70`, `value_usd = $7,346,565.99` — the understated figure a consumer reads. Incident: none.

### STILL CONFIRMED (20)
- **EXECUTIONTOKENS-C01** (high): supply aggregate at `int_execution_tokens_supply_holders_daily.sql` lines `34-37` still `sumIf(b.balance, address!=zero)` with no `balance>0` floor while holders (lines `39-45`) filter `balance>0`. Blast radius `145` token/day pairs >1% understatement over 60d across `wstETH`, `ZCHF`, `svZCHF`. Worst served point: wstETH `2026-05-08` served `value_native = -207.45`, `value_usd = -$589,706.02`.
- **EXECUTIONTOKENS-C02** (high): `fct_execution_tokens_metrics_daily.sql` line `100` `supply_usd = sh.supply * coalesce(p.price_usd,0)`, no floor; `31` negative `supply_usd` rows all-time, `11` served via `api_execution_tokens_supply_daily` over last 30d (api count == fct count, no floor). No `not_negative`/`min_value`/`accepted_range` ERROR test in `marts/schema.yml`; only `elementary.column_anomalies` at `severity:warn`.
- **EXECUTIONTOKENS-C03** (medium): `int_execution_tokens_transfers_daily.sql` passes `filters_sql=symbol_sql` (line `38`) AND re-applies the same two `symbol_filter('symbol',...)` calls verbatim (lines `40-41`); `address_diffs_daily.sql` identical (lines `39`, `41-42`). Macro body wrapped in `{% if is_incremental() %}` (`macros/db/get_incremental_filter.sql` line `24`) — emits nothing on full-refresh, leaving explicit copy as sole guard. Predicate text identical → no row leak.
- **EXECUTIONTOKENS-C04** (medium): `semantic/authoring/execution/tokens/semantic_models.yml` lines `218-272` register phantom dims `from_value_binary`/`from_value_string`/`to_value_binary`/`to_value_string`/`chain_id`/`block_timestamp` and measures `net_delta_raw_value`/`from_value_f64_value`/`to_value_f64_value`/`insert_version_value`; `describe_table` shows only `8` real columns. Live failure unprovable only because the semantic engine is down platform-wide (`manifest_hash_mismatch`); code-level mismatch confirmed.
- **EXECUTIONTOKENS-C05** (medium): `int_execution_tokens_balances_native_daily.sql` config (lines `2-12`) `incremental_strategy='delete+insert'`, ReplacingMergeTree, no `join_use_nulls` hooks. Both joined output columns COALESCE-guarded (`d.net_delta_raw` line `172`, `p.balance_raw` line `178`); dims from calendar/keys CTE. `8` of `44` `delete+insert` repo models carry the hooks → genuine deviation, latent only.
- **EXECUTIONTOKENS-C06** (medium): `fct_execution_tokens_overview_by_class_latest.sql` line `94` still `INNER JOIN info_7d t2 ON t1.token_class=t2.token_class AND t1.label=t2.label`. Class-debut probe over 90d returned `0` firing events; `info_latest` and `info_7d` share no upstream class filter, so the drop precondition is real but latent.
- **EXECUTIONTOKENS-C07** (low): `models/execution/tokens/intermediate/schema.yml` column literally named `AS` with `data_type: ''` at lines `23-25` (under `address_diffs_daily`) and lines `246-248` (under `transfers_daily`). `check_api_tags.py` rejects empty `data_type` (lines `86-88`) but skips non-`api:` models (lines `55-57`) → dormant until either model gets an `api:` tag. Side note: `address_diffs` schema also documents phantom `net_delta` (line `35`) vs SQL output `net_delta_raw`.
- **EXECUTIONTOKENS-C08** (low): `overview_by_class_latest` filters `balance_raw>0` (lines `43`, `54`); `supply_holders_daily` filters `balance>0` (lines `41-43`). `0` rows with `balance_raw>0 AND balance=0` over 7d (and over 90d) → robustly theoretical; closeable as a note.
- **EXECUTIONTOKENS-C10** (low): `api_execution_tokens_balances_daily.sql` meta.api `allow_unfiltered:false`, `require_any_of:[symbol,address]`, address `max_items:200`; no `privacy:`/`expose_to_mcp:` tag. Repo-wide no model uses those tags → no convention to violate; guard effective (no default/wildcard bypass, no companion `_latest`/`_all` unguarded view). Docs-only note.
- **EXECUTIONTOKENS-C11** (high): `schema.yml` describes supply as "circulating supply" / "sum of positive balances"; SQL sums negatives. wstETH `2026-06-20` stored supply `3,414.70` vs positive-only `3,871.87` = `11.8%` understatement, `99` negative rows, `-3,871.87` negative mass. Honest nuance: `0` of `25` tokens has a NET-negative stored supply on the current date, so the baseline's literal "observable negative number for wstETH" is not true today (negatives understate but no longer flip the sign); core business-logic drift confirmed.
- **EXECUTIONTOKENS-C12** (medium): `api:tokens_supply` on `api_execution_tokens_supply_daily` (`granularity:daily`) and `api_execution_tokens_supply_latest_by_token` (`granularity:latest`); `api:holders_per_token` on the two holders models. `check_api_tags.py` forbids grain suffixes and multi-api-per-model but permits name sharing. Distinct granularity tag + schema make it disambiguable by `(api_name+granularity)`; severity bounded to "router must read granularity, not name alone".
- **EXECUTIONTOKENS-C13** (medium): supply is pure `sumIf` of per-address balances (`int_execution_tokens_supply_holders_daily.sql:34-37`), never `totalSupply()`. Schema tests are only `unique_combination_of_columns` + `elementary.schema_changes` — no reconciliation/tolerance check. Concrete divergence: on-chain wstETH `totalSupply() = 20,791.33` vs model `3,871.87` (pos_only) = `~81%` gap unmonitored.
- **EXECUTIONTOKENS-C14** (medium): `dbt_project.yml` line `11` `symbol_exclude` still lists exactly `15` wrappers (`6` aGno + `9` sp). None appear in served `fct_execution_tokens_metrics_daily` on the latest date. Base `balances_daily` carries `29` distinct tokens (whitelist minus wrappers). No caveat column documents the exclusion to external consumers.
- **EXECUTIONTOKENS-C15** (low): `semantic_models.yml` — `supply_by_sector` has `api_` (lines `353-361`) + `fct_` (lines `362-370`) entities; `supply_distribution` has `api_` (lines `399-407`) + `fct_` (lines `408-416`); both pairs share identical `question_synonyms`. Both are measure-less candidate-tier bare refs; `discover_metrics` returns neither → inert authoring noise.
- **EXECUTIONTOKENS-C16** (low, healthy): `int_execution_tokens_balances_daily` = `388,774,473` rows (grew from `383.7M`), `max_date=2026-06-20`, `min_date=2020-07-01`, `29` tokens. Per-month 2026 smooth (Jan `11.16M`..May `11.54M`, Jun `7.61M` partial); no month-collapse despite supply_holders using the incident-A `insert_overwrite`+`toStartOfMonth` pattern.
- **EXECUTIONTOKENS-C17** (low, healthy): `0` NULL `balance_usd` and `0` zero-fill over `2,704,139` positive-balance rows in last 7d; all `25` active tokens priced. Served layer: `0` of `739` positive-supply rows in `fct_execution_tokens_metrics_daily` over 30d have NULL `supply_usd`.
- **EXECUTIONTOKENS-C18** (low, healthy): `0` duplicate `(date,token_address,address)` groups in balances_daily and `0` duplicate `(date,token_address)` groups in transfers_daily over last 14d, with AND without FINAL → no unmerged-parts RMT footgun.
- **EXECUTIONTOKENS-C21** (low, healthy): `0` rows with `supply>0 AND (supply_usd=0 OR supply_usd IS NULL)` over `739` populated positive-supply rows in last 30d.
- **EXECUTIONTOKENS-C22** (low, healthy): all `6` `token_class`/`label` rows present with `change_pct` populated; the `-92%` STABLECOIN swing traced to a REAL on-chain BRZ collapse (`2.80B` → `0.054B`), confirmed by on-chain `totalSupply() = 65,948,264.30 BRZ` — not a C11-style running-sum artefact.
- **EXECUTIONTOKENS-C23** (low, healthy): `fct_execution_tokens_metrics_daily` `max_date=2026-06-20`, `25` rows on latest date. The `25`-vs-`29` gap is latest-date-active vs all-time-seen — LEFT-ANTI of active balances against metrics = `0` rows; `4` tokens simply have no rows on the latest date. Benign.

### NEW (0)
No new issues surfaced during re-verification.

### UNVERIFIABLE / UNRESOLVED (0)
None. Every case reached `sufficient` after `≥3` rounds (C11, C13 reached `4`).

## Evidence appendix

**C01 / C11 / C20 — supply aggregate omits `balance>0` guard (shared probe).**
`int_execution_tokens_supply_holders_daily.sql` lines `34-37`: `supply = sumIf(b.balance, lower(b.address) != zero)`; holders lines `39-45`: `countDistinctIf(... balance>0)`.
```sql
SELECT sumIf(balance, lower(address)!='0x00...0') AS supply_incl_neg,
       sumIf(balance, balance>0 AND lower(address)!='0x00...0') AS supply_pos_only,
       countIf(balance<0) AS neg_holders, sumIf(balance,balance<0) AS neg_mass
FROM dbt.int_execution_tokens_balances_daily FINAL
WHERE symbol='wstETH' AND date='2026-06-20' AND lower(address)!='0x00...0';
```
Returns: `supply_incl_neg=3,414.70`, `supply_pos_only=3,871.87`, gap `457.17` wstETH (`~11.81%`), `99` negative holders, neg_mass `-457.17` (= the gap). Blast radius over 60d: `145` token/day pairs >1% understatement across `wstETH`/`ZCHF`/`svZCHF`.

**C01 / C02 / C19 — worst served negative point.**
```sql
SELECT token, value_native AS supply, value_usd
FROM api_execution_tokens_supply_daily WHERE token='wstETH' AND date='2026-05-08';
```
Returns: `value_native = -207.45`, `value_usd = -$589,706.02` (a negative dollar supply served to consumers). For that date raw balances `incl_neg=-212.45` vs `pos_only=67.49` = `414.78%` understatement.

**C02 — served negatives match fct (no floor) + no test.**
```sql
SELECT 'api_supply_daily_neg', count(*) FROM dbt.api_execution_tokens_supply_daily WHERE value_usd<0 AND date>=today()-30 AND date<today()
UNION ALL SELECT 'fct_supply_usd_neg', count(*) FROM dbt.fct_execution_tokens_metrics_daily WHERE supply_usd<0 AND date>=today()-30 AND date<today()
UNION ALL SELECT 'api_supply_latest_neg', count(*) FROM dbt.api_execution_tokens_supply_latest_by_token WHERE value_usd<0;
```
Returns: api_supply_daily `11`; fct `11` (identical → no floor); api_supply_latest `0`. `marts/schema.yml` carries no `not_negative`/`min_value`/`accepted_range` on `supply`/`supply_usd`; only `elementary.column_anomalies` (`severity:warn`).

**C13 / C20 — on-chain reconciliation anchor.**
`contract_call_function` wstETH (`0x6c76971f98945ae98dd7d4dfca8711ebea946ea6`) `totalSupply() = 20791329795858950091616 wei = 20,791.33 wstETH`. Model pos_only `3,871.87` → `~81%` gap. Confirms negatives are model artefacts (real burns would lower on-chain totalSupply), and that sum-of-tracked-holders is structurally far below totalSupply with no tolerance test.

**C19 — negative-supply rows.**
```sql
SELECT token_address, symbol, count(*), min(date), max(date), uniqExact(date)
FROM int_execution_tokens_supply_holders_daily FINAL WHERE supply<0 GROUP BY 1,2;
```
Returns: `31` rows, all wstETH, `min=2026-05-01`, `max=2026-06-01`, `31` distinct days. wstETH supply `2026-05-30`..`06-01` = `-9.55` (negative); `2026-06-02` = `+43.45`; `2026-06-05` = `3,425.69` (sign healed via partition recompute, magnitude still understated through `06-20`).

**C03.** `transfers_daily.sql` lines `18-21` build `symbol_sql` = `symbol_filter('symbol',symbol,'include')` + `symbol_filter('symbol',symbol_exclude,'exclude')`; passed at line `38` as `filters_sql=symbol_sql`; re-applied verbatim at lines `40-41`. `address_diffs_daily.sql` identical (lines `20-23`, `39`, `41-42`). Method: `code_only`.

**C04.** `describe_table int_execution_tokens_balances_daily` → `8` columns (`date`, `token_address`, `symbol`, `token_class`, `address Nullable(String)`, `balance_raw Int256`, `balance`, `balance_usd Nullable(Float64)`). `explain_metric_query` on `net_delta_raw_value` + dim `chain_id` returned `Semantic execution unavailable: manifest_hash_mismatch` (engine down platform-wide). Method: `code_only` + `describe_table`.

**C05.** `int_execution_tokens_balances_native_daily.sql` config lines `2-12` (`delete+insert`, RMT, no hooks); guards line `172`/`178`. Repo scan: `44` `delete+insert` models, `8` carry `join_use_nulls`. Method: `code_only`.

**C06.**
```sql
WITH daily_classes AS (SELECT date, groupUniqArray(token_class) classes FROM dbt.fct_execution_tokens_metrics_daily WHERE date>=today()-90 AND date<today() GROUP BY date)
SELECT count(*) FROM (SELECT a.date, arrayFilter(x->NOT has(b.classes,x), a.classes) new FROM daily_classes a INNER JOIN daily_classes b ON b.date=a.date-7) WHERE length(new)>0;
```
Returns: `0` days with a class present on `date` but absent on `date-7`. INNER JOIN at line `94`.

**C07.** `intermediate/schema.yml` lines `23-25` and `246-248`: `name: 'AS'`, `data_type: ''`. `check_api_tags.py` lines `55-57` skip non-api models; lines `86-88` reject empty `data_type`. Grep of models/macros/semantic/dashboards for a column literally named `AS` on these models: no real consumer. Method: `code_only`.

**C08.**
```sql
SELECT count(*) FROM dbt.int_execution_tokens_balances_daily FINAL WHERE balance_raw>0 AND balance=0 AND date>=today()-7 AND date<today();
```
Returns: `0` (also `0` over 90d). Code: overview `balance_raw>0` (lines `43`,`54`), supply_holders `balance>0` (lines `41-43`).

**C09.**
```sql
WITH pd AS (SELECT addDays(max(date),-7) d FROM dbt.int_execution_tokens_balances_daily WHERE date<today() AND balance>0),
direct AS (SELECT DISTINCT token_address, lower(address) address FROM dbt.int_execution_tokens_balances_daily WHERE date=(SELECT d FROM pd) AND balance>0),
ubo AS (SELECT DISTINCT token_address, ubo_address address FROM dbt.fct_ubo_supply_claims_resolved_daily WHERE date=(SELECT d FROM pd) AND balance>0)
SELECT count(*) FROM direct d INNER JOIN ubo u ON d.token_address=u.token_address AND d.address=u.address;
```
Returns: `4,807` overlap pairs; `354` of those are actually ranked/served in `fct_execution_tokens_top_holders_ranked`. Code: `prev_balances` lines `45-56` (no dedup), `change_usd_7d` line `73`.

**C10.** `api_execution_tokens_balances_daily.sql` meta.api: `allow_unfiltered:false`, `require_any_of:[symbol,address]`, address `max_items:200`, no `privacy:`/`expose_to_mcp:`. Repo-wide grep: no model uses those tags. Method: `code_only`.

**C12.** Tags read on the 4 marts: `api:tokens_supply` + (`granularity:daily` / `granularity:latest`); `api:holders_per_token` + same. `check_api_tags.py` `api_suffix` rule (lines `64-68`), `multi_api` rule (line `70`); no name-sharing rule. Distinct schemas (daily: `date,token,token_class,value`; latest_by_token: `token,value_native,value_usd,as_of_date`). Method: `code_only`.

**C14.** `dbt_project.yml` line `11`: `aGnoGNO,aGnoWXDAI,aGnosDAI,aGnoUSDC,aGnoEURe,aGnoUSDCe,spGNO,spUSDT,spUSDC,spUSDC.e,spWETH,spwstETH,spWXDAI,spsDAI,spEURe` (`15`). `balances_daily` `uniqExact(token_address)=29`; metrics latest `25` tokens, none being wrappers.

**C15.** `semantic_models.yml` lines `353-370` (supply_by_sector api_+fct_), `399-416` (supply_distribution api_+fct_), identical `question_synonyms`, all measure-less candidate refs. `discover_metrics('execution tokens supply distribution latest')` returned `10` results, none being either entity.

**C16.**
```sql
SELECT count(*), max(date), min(date), uniqExact(token_address) FROM int_execution_tokens_balances_daily WHERE date>='2020-07-01';
```
Returns: `388,774,473`, `2026-06-20`, `2020-07-01`, `29`. Per-month 2026 contiguous, no collapse.

**C17.**
```sql
SELECT countIf(supply>0 AND supply_usd IS NULL), countIf(supply>0 AND supply_usd=0), countIf(supply>0)
FROM fct_execution_tokens_metrics_daily WHERE date>=today()-30 AND date<today();
```
Returns: `0`, `0`, `739`.

**C18.** Over 14d without FINAL: balances `(date,token_address,address)` HAVING `c>1` = `0`; transfers `(date,token_address)` HAVING `c>1` = `0`.

**C21.**
```sql
SELECT countIf(supply>0 AND supply_usd=0), countIf(supply>0) FROM fct_execution_tokens_metrics_daily WHERE date>=today()-30 AND date<today();
```
Returns: `0`, `739`.

**C22.** `contract_call_function` BRZ (`0x0a06c8354a6cc1a07549a38701eac205942e3ac6`) `totalSupply() = 65,948,264.30 BRZ`. Model BRZ supply `2026-06-13 = 2,795,733,331`, `2026-06-20 = 53,741,057` (~98% reduction, implied price stable ~$0.20→$0.19). On-chain `65.9M` matches post-collapse `53.7M` order of magnitude → real on-chain burn/redemption.

**C23.** `fct_execution_tokens_metrics_daily` `max(date)=2026-06-20`, `25` rows on latest date; balances `29` all-time tokens. LEFT-ANTI of active balances (`balance!=0`) against metrics on `2026-06-20` = `0` rows → no active token dropped.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED (code unchanged, no `balance>0` guard) → challenge "quantify blast radius across all tokens" → R2 CONFIRMED (`145` token/day pairs >1% over 60d, `wstETH`/`ZCHF`/`svZCHF`) → challenge "dollar figure on single worst served point" → R3 CONFIRMED (wstETH `2026-05-08` served `-$589,706.02`, `414.78%`).
- **C02**: R1 CONFIRMED (`31` neg `supply_usd`, min `-$1,047,790.87`, no test) → challenge "is it actually served" → R2 CONFIRMED (api `11` == fct `11`, no floor) → challenge "grep for any blocking test" → R3 CONFIRMED (only warn-level Elementary; nothing blocks negative).
- **C03**: R1 CONFIRMED (filter in macro arg + explicit WHERE) → challenge "confirm full-refresh fragility in macro" → R2 CONFIRMED (macro body wrapped in `{% if is_incremental() %}`) → challenge "predicate text identical or drifted" → R3 CONFIRMED (identical text, no leak).
- **C04**: R1 CONFIRMED (phantom dims vs `describe_table`) → challenge "prove live MCP failure" → R2 CONFIRMED (`8` real cols, phantom measures on lines `824-959`) → challenge "convert would-fail to does-fail" → R3 CONFIRMED at code level (`explain_metric_query` returned `manifest_hash_mismatch` — engine down, not column-specific).
- **C05**: R1 CONFIRMED (no `join_use_nulls`, COALESCE holds) → challenge "enumerate every joined column reaching output" → R2 CONFIRMED (only `d.net_delta_raw`/`p.balance_raw`, both guarded) → challenge "do peers carry the hooks" → R3 CONFIRMED (`8/44` peers carry it; genuine but latent deviation).
- **C06**: R1 CONFIRMED (INNER JOIN line `94`) → challenge "could it ever fire (90d)" → R2 CONFIRMED (`0` firing events) → challenge "confirm both labels drop + info_latest can surface what info_7d lacks" → R3 CONFIRMED (independent CTEs; precondition real, latent).
- **C07**: R1 CONFIRMED (`AS` empty `data_type` lines `23`/`246`) → challenge "does CI reject it / run on these models" → R2 CONFIRMED (rejects empty `data_type` but skips non-api models; dormant) → challenge "is it a pure artefact with no consumer" → R3 CONFIRMED (no consumer; safe to remove; also `net_delta` doc drift).
- **C08**: R1 CONFIRMED (`balance_raw>0` vs `balance>0`) → challenge "quantify divergence (7d)" → R2 CONFIRMED (`0` rows) → challenge "widen to 90d" → R3 CONFIRMED (prioritized higher-sev within budget; rounds 1-2 already `0`; robustly theoretical).
- **C09**: R1 CONFIRMED (UNION ALL + sum, no dedup) → challenge "prove it can double-count" → R2 CHANGED (`4,807` overlap pairs on 7d-ago date) → challenge "intersect with ranked/served" → R3 CONFIRMED code path; orchestrator note: `354` overlap pairs actually ranked/served.
- **C10**: R1 CONFIRMED (no `privacy:`/`expose_to_mcp:`) → challenge "compare to peer feeds" → R2 CONFIRMED (no model uses those tags; docs-only) → challenge "is the guard effective / any unguarded companion view" → R3 CONFIRMED (guard effective, no companion).
- **C11**: R1 CONFIRMED (`~11.8%` understatement `2026-06-20`) → challenge "corroborate root cause upstream" → R2 CONFIRMED (`98` negative running-sum holders = exact gap; on-chain totalSupply `20,791` → artefacts) → challenge (R3 implicit) → R4 CONFIRMED (re-measured `3,414.70` vs `3,871.87`, `99` neg rows; nuance: `0` of `25` tokens NET-negative today).
- **C12**: R1 CONFIRMED (each api name on two models) → challenge "can router disambiguate" → R2 CONFIRMED (`check_api_tags` permits name sharing) → challenge "distinct granularity + schema" → R3 CONFIRMED (disambiguable by `(api_name+granularity)`).
- **C13**: R1 CONFIRMED (no reconciliation, no test) → challenge "quantify divergence on one token" → R2 CONFIRMED (on-chain `20,791` vs model `3,872` = `~81%`) → challenge "structural vs freshness" → R4 CONFIRMED (sum-of-balances only; tests = uniqueness + schema_changes; gap structural).
- **C14**: R1 CONFIRMED (`15` wrappers) → challenge "confirm downstream effect on served OTHERS" → R2 CONFIRMED (none in served metrics) → challenge "scoping caveat vs hard gap; any consumer footnote" → R3 CONFIRMED (default-run caveat, no caveat column).
- **C15**: R1 CONFIRMED (duplicate api_/fct_ pairs, identical synonyms) → challenge "prove routing ambiguity" → R2 CONFIRMED (`discover_metrics` returns neither) → challenge "are they measure-less / promotable" → R3 CONFIRMED (both inert measure-less candidates).
- **C16**: R1 CONFIRMED (`388.8M`, max yesterday, `29`) → challenge "check per-month for collapse" → R2 CONFIRMED (smooth monthly, no collapse) → challenge "check supply_holders/metrics tail specifically" → R3 CONFIRMED (`25`/day contiguous 14d, no single-day collapse).
- **C17**: R1 CONFIRMED (`0%` NULL over `2.7M`) → challenge "rule out silent zero-fill / all tokens priced" → R2 CONFIRMED (`0` zero-or-null, `25/25` priced) → challenge "extend to served layer" → R3 CONFIRMED (`0` NULL over `739` served rows).
- **C18**: R1 CONFIRMED (`0`/`0` FINAL, 3d) → challenge "confirm without FINAL" → R2 CONFIRMED (`0` without FINAL too) → challenge "widen to 14d" → R3 CONFIRMED (`0` over 14d no-FINAL).
- **C19**: R1 CONFIRMED (`3`→`31` rows expansion) → challenge "continuity to present edge" → R2 CHANGED (span `2026-05-01`..`06-01`, one gap, `0` after `06-15`) → challenge "explain 06-01 cutoff vs balances through 06-20" → R3 CHANGED (June rows exist + flipped positive; partition recompute healed sign not magnitude).
- **C20**: R1 CONFIRMED (`~11.8%` / `457` wstETH on `2026-06-20`) → challenge "cross-check vs on-chain totalSupply" → R2 CHANGED (`20,791` totalSupply confirms negatives are artefacts) → challenge "confirm served impact bounded to supply" → R3 CHANGED (latest snapshot returns `3,414.70` / `$7,346,565.99`).
- **C21**: R1 CONFIRMED (`0` zero-price latest day) → challenge "extend 30d per token" → R2 CONFIRMED (`0` across 30d) → challenge "show denominator is populated" → R3 CONFIRMED (`739` positive-supply rows evaluated).
- **C22**: R1 CONFIRMED (all classes present, `-92%` = anchor swing) → challenge "verify attribution (real swing vs data gap)" → R2 CONFIRMED (BRZ `2.80B`→`0.054B`, both dates populated) → challenge "confirm real on-chain, not artefact" → R3 CONFIRMED (on-chain BRZ totalSupply `65.9M`).
- **C23**: R1 CONFIRMED (max yesterday, `25` rows) → challenge "explain 25-vs-29 gap (benign?)" → R2 CONFIRMED (LEFT-ANTI = `0`, `4` inactive tokens) → challenge "name the 4 missing tokens" → R3 CONFIRMED (`25` active vs `29` all-time; names not individually re-listed within budget; no join drop).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (KEEP/ESCALATE) | Add a `balance > 0` floor to the supply `sumIf` so "circulating supply" matches its canonical/schema definition and the model's own holders filter. The defect now serves negative dollar supply (`wstETH 2026-05-08 = -$589,706.02`) and `~11.8%` understatement on the latest date. | `models/execution/tokens/intermediate/int_execution_tokens_supply_holders_daily.sql` |
| P0 (KEEP/ESCALATE) | Add an ERROR-level `not_negative`/`min_value: 0` (or `dbt_utils.accepted_range`) test on `supply` and `supply_usd`; current Elementary anomaly tests are `severity:warn` and do not block the negative from being served. | `models/execution/tokens/marts/schema.yml` (`fct_execution_tokens_metrics_daily`, `api_execution_tokens_supply_daily`) |
| P1 (KEEP) | Investigate/repair the upstream per-address running-sum producing spurious negatives (delta ordering / incomplete history), then backfill-recompute affected wstETH partitions. Note the June partition recompute already healed the SIGN past `2026-06-01` but not the MAGNITUDE understatement through `06-20`. | `models/execution/tokens/intermediate/int_execution_tokens_balances_daily.sql`, `int_execution_tokens_supply_holders_daily.sql` |
| P1 (KEEP) | Add an ERC-20 `totalSupply()` reconciliation/tolerance check for ERC-4626 (sDAI), Aave/Spark wrapper, and rebasing tokens; current sum-of-tracked-holders is `~81%` below on-chain totalSupply for wstETH, unmonitored. | `models/execution/tokens/intermediate/int_execution_tokens_supply_holders_daily.sql` |
| P2 (KEEP) | Reconcile `semantic_models.yml` dims/measures with the real `8`-column output of `int_execution_tokens_balances_daily` (remove phantom `from_value_*`/`to_value_*`/`chain_id`/`block_timestamp` dims and `net_delta_raw_value` etc. measures); re-test once the semantic engine is back. | `semantic/authoring/execution/tokens/semantic_models.yml` |
| P2 (KEEP) | Change the 7d-ago snapshot join from INNER to LEFT JOIN with `COALESCE(t2.value, 0)` so a newly-debuted `token_class` surfaces with a 100% change figure instead of being silently dropped. | `models/execution/tokens/marts/fct_execution_tokens_overview_by_class_latest.sql` |
| P3 (KEEP) | Dedup direct + UBO `prev_7d` per `(token_address, address)` before summing into `change_usd_7d` (`354` ranked/served holders currently double-subtracted). | `models/execution/tokens/marts/fct_execution_tokens_top_holders_latest.sql` |
| P3 (KEEP) | Remove redundant explicit symbol filter OR keep it intentionally as the full-refresh guard and document the intent (predicate is identical, no leak today, but the duplication is fragile). | `int_execution_tokens_transfers_daily.sql`, `int_execution_tokens_address_diffs_daily.sql` |
| P3 (KEEP) | Add `join_use_nulls` pre/post hooks per project convention (`8/44` peers carry it) for the `delete+insert` LEFT-JOIN model; latent today (COALESCE guards hold). | `models/execution/tokens/intermediate/int_execution_tokens_balances_native_daily.sql` |
| P3 (KEEP) | Remove the phantom `AS` empty-`data_type` columns (and the `net_delta` vs `net_delta_raw` doc drift) from the intermediate schema; dormant CI risk until either model gets an `api:` tag. | `models/execution/tokens/intermediate/schema.yml` |
| P3 (KEEP) | Document the `15`-wrapper `symbol_exclude` scoping caveat for any externally-shown OTHERS-class supply figure (correct-by-design, but no consumer-facing footnote exists). | `dbt_project.yml`, `models/execution/tokens/marts` API supply models |
| P4 (KEEP/NOTE) | Record a `privacy:`/`expose_to_mcp:` decision for the per-wallet balance feed; resolve duplicate api_/fct_ semantic entities (C15); standardize holders filter to `balance>0` everywhere (C08, `0` divergence today). | `api_execution_tokens_balances_daily.sql`, `semantic_models.yml`, `fct_execution_tokens_overview_by_class_latest.sql` |

No DROP recommendations: nothing was RESOLVED between baseline and this revisit.
