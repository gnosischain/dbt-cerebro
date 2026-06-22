# Model review (revisit 2026-06-21): execution/blocks

Baseline `docs/model_review/execution-blocks.md` (dated `2026-06-11`); 15 cases re-verified across 3 rounds. Headline: 1 resolved (freshness lag recovered), 2 changed (both tempered to low after blast-radius/heuristic-quality evidence), and 12 still confirmed — the three high-severity defects (partial-month gas ratio served as final, twice; semantic-layer sum-of-ratios) all remain intact.

## Status summary

| Case | P0 | Claim (short) | Orig sev | Status | New sev | Confidence | Incident | Rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONBLOCKS-C01 | — | Monthly gas models have no in-progress-month guard; latest row serves partial current month as final, mutates daily | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONBLOCKS-C02 | — | fct/monthly layer lacks grain unique/not_null test (intermediates have it) | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONBLOCKS-C03 | — | `int_execution_blocks_clients_version_daily` uses unguarded `decode_hex_tokens` not `decode_hex_tokens2` | medium | CHANGED | low | high | none | 3 |
| EXECUTIONBLOCKS-C04 | — | schema.yml declares `DateTime64` for date col that is actually `DateTime` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONBLOCKS-C05 | — | Monthly api_ mart missing volume/freshness anomaly tests its daily siblings carry | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONBLOCKS-C06 | — | Unknown-client bucket (8.5% of rows) has no catch-all and no Elementary monitor | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONBLOCKS-C07 | — | Source pipeline 3 days behind today; intermediates max(date)=T-3 vs 26h SLA | low | RESOLVED | resolved | high | data-recovery | 3 |
| EXECUTIONBLOCKS-C08 | — | Monthly gas-utilization served on api_ view is partial-month, simultaneously wrong + silently mutating | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONBLOCKS-C09 | — | Semantic `fraq_value` measure agg:sum over a pre-computed ratio → sum-of-ratios | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONBLOCKS-C10 | — | Monthly semantic model declares day grain over a `month AS date` column | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONBLOCKS-C11 | — | `base_fee_per_gas` in source, surfaced by no model despite EIP-1559; title over-states scope | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONBLOCKS-C12 | — | Client identification is positional heuristic, no caveat, unmonitored Unknown bucket | medium | CHANGED | low | high | none | 3 |
| EXECUTIONBLOCKS-C13 | — | All 8 semantic models quality_tier:candidate yet api_ models serve public API | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONBLOCKS-C14 | — | `fct_execution_blocks_gas_usage_monthly` tagged `transactions` not `blocks` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONBLOCKS-C15 | — | `gas_used_fraq` min 1.2e-7 on earliest blocks — reviewed, no data quality issue | low | CONFIRMED | low | high | none | 3 |

Net: 3 high (C01, C08, C09), 3 medium (C04, C10, C11), 8 low, 1 resolved (C07). No NEW, UNVERIFIABLE, or UNRESOLVED cases.

## Delta vs baseline

### RESOLVED (1)

- **C07 — source freshness lag gone.** Baseline: both intermediates `max(date)=2026-06-08` (T-3). Now `int_execution_blocks_gas_usage_daily` and `int_execution_blocks_clients_version_daily` both have `max(date)=2026-06-21` (today), with a fully contiguous 21-day series and healthy ~`285.97e9`-`287.91e9` gas_limit_sum per interior day (today partial at `92.2e9`). Lag is now ~0d, within the 26h warn / 48h error SLA. **Incident attribution: data-recovery (none).** The baseline symptom was a 3-day freshness lag, not a month-collapse/single-day signature, so it does not match the insert_overwrite wipe pattern; `docs/incidents/` holds only `logs_ingestion_gap_2026.md`, scoped to `execution.logs` on 2026-05-30 and 2026-06-14 (not `execution.blocks`). Routine cron catch-up.

### CHANGED (2) — both tempered downward

- **C12 — heuristic client identification, medium → low.** Code unchanged (positional `decoded_extra_data[1]`=client, `[2]='Ethereum'?[3]:[2]`=version; no caveat in schema.yml). Two evidence shifts: (1) the baseline's pre/post-Merge convention-drift rationale is *refuted* — pre-Merge years 2018-2022 are `0%` Unknown by both block and row weight, while Unknown peaks post-Merge (2024 block-weighted `9.19%`, row `43.3%`); the heuristic classifies the pre-Merge era cleanly. (2) Top-15 client/version pairs are all clean, recognizable clients (Nethermind, Parity, OpenEthereum) with sane semver — the heuristic works in practice. Residual concern narrows to the missing caveat + unmonitored Unknown bucket.
- **C03 — unguarded decode macro, medium → low.** Code unchanged (`int_execution_blocks_clients_version_daily.sql` line 22 still calls `decode_hex_tokens`, not the guarded `decode_hex_tokens2`). But blast radius is zero across full pre-Merge/Merge history: blocks `0`-`5,000,000` = `0` non-hex / `0` odd-length; blocks `5,000,000`-`30,000,000` (spans entire PoA era + Merge boundary ~26.5M) = `0`/`0`. 30M consecutive clean blocks → the unguarded `unhex()` never mislabelled a historical row. Latent-only defect.

### STILL CONFIRMED (12)

High:
- **C01 — monthly gas model has no in-progress-month guard.** `fct_execution_blocks_gas_usage_monthly.sql` GROUP BY month over `int_execution_blocks_gas_usage_daily`; the intermediate has *no* `WHERE date < today()` filter, so the latest monthly row exposes incomplete June (21 of 30 days incl. today). June `2026-06-01` = `45.83%` (num `2,668,169,008,949` < May `3,454,008,353,041`; den `5,822,227,192,877` < May `8,878,932,091,554`), served beside complete May `38.9%`. Daily models *do* guard (`api_execution_blocks_gas_usage_pct_daily.sql` line 12, `fct_execution_blocks_clients_daily.sql` line 17 carry `WHERE date < today()`). Consumer-visible overstatement ~`6.9pp` that drifts daily.
- **C08 — partial-month served as a final figure (consumer view of C01).** Served api_ value `45.83%` *exactly equals* the source June-MTD ratio (`sum(gas_used_sum)/sum(gas_limit_sum)` over `date >= toStartOfMonth(today())` = `45.83%`). Mutation proof: baseline `46.08%`@8d → now `45.83%`@21d, same "final" cell changed as the month progressed. `api_execution_blocks_gas_usage_pct_monthly.sql` SELECT list is exactly `(month AS date, ROUND(used*100,2) AS value)` — no is_partial / completeness / day-count column, so a consumer cannot tell the latest row is incomplete.
- **C09 — sum-of-ratios in the semantic layer.** `fraq_value` measure on `execution_blocks_clients_daily` is `agg: sum, expr: fraq` where `fct_execution_blocks_clients_daily.sql` line 25 defines `fraq = cnt / SUM(cnt) OVER (PARTITION BY date)` (a pre-computed ratio). Demonstrated: per-day `sum(fraq) = 1.0` over 5 clients, so a MetricFlow N-day query returns ~N (meaningless). It is a *registered* simple metric (`semantic_models.yml` lines 259-283) with supported_time_grains day..year, wired into the MCP metrics registry — live blast radius.

Medium:
- **C04 — DateTime64 vs DateTime.** `describe_table` returns `DateTime` for `int_execution_blocks_clients_version_daily.date`; `intermediate/schema.yml` line 10 declares `DateTime64`. The model carries an `elementary.schema_changes` test (warn), so correcting the type would fire a schema_changes alert — a coordinated-change item. Isolated to one intermediate (downstream marts declare date as `Date`).
- **C10 — day grain over month-start date.** `execution_blocks_gas_usage_pct_monthly` declares `agg_time_dimension: date` with `time_granularity: day` and lists `day`/`week` in supported_time_grains (lines 511-517), but the mart does `SELECT month AS date`. All 93 rows have `toDayOfMonth(date)=1` (non_month_start `0`), monthly spacing. MetricFlow silently mis-buckets day/week queries (one populated day per month, rest empty).
- **C11 — base_fee_per_gas unsurfaced.** `execution_sources.yml` line 74 declares `base_fee_per_gas` (UInt64, wei); grep of all `models/execution/` finds it in *no* model. Source is live: last 7 days all `129,075` blocks have `base_fee_per_gas>0`, max `1,560,204,201` wei (~1.56 Gwei). Unit title literally claims "Block-level metrics: production, gas, base fee" — base fee covered nowhere despite EIP-1559.

Low:
- **C02 — missing grain test at fct layer.** `marts/schema.yml`: `fct_execution_blocks_gas_usage_monthly` carries only `elementary.schema_changes` (warn); `fct_execution_blocks_clients_daily` carries volume/freshness/schema_changes + a column_anomalies on cnt, but neither has `dbt_utils.unique_combination_of_columns` or not_null on grain (intermediates do). Tempered to low: both fcts are pure GROUP BY over already-unique intermediate grains with no fan-out join (`93` rows = `93` distinct months; `6,962` rows = `6,962` distinct (date,client)) — missing test is belt-and-suspenders.
- **C05 — monthly api_ mart missing volume/freshness tests.** `api_execution_blocks_gas_usage_pct_monthly` carries only `elementary.schema_changes`; daily siblings carry volume_anomalies + freshness_anomalies + schema_changes. Mart is a live 93-row view (2018-10-01..2026-06-01) so gap is latent. The grandparent `int_execution_blocks_gas_usage_daily` *does* carry volume/freshness, so a source stall is caught upstream before reaching the monthly leaf.
- **C06 — unmonitored Unknown bucket.** `1,126/13,219` rows = `8.52%` (row-weighted, matches baseline 8.6%); `898,370/46,807,199` = `1.92%` block-weighted. multiIf allow-list has no catch-all; schema.yml has no Unknown monitor. The gap has demonstrated teeth: 2024 block-weighted Unknown rose `0.14%` (Jan) → `14.89%` (Jul), held 13-14% through Dec — a 100x spike the absent monitor would have missed. Kept low (monitoring-completeness gap, not corruption) but a firmly-justified low.
- **C13 — candidate tier served publicly.** All 8 blocks semantic models + every metric are `quality_tier: candidate`; api_ marts carry `tier1` + `api:blocks_gas_usage_pct` tags wiring them to live endpoints with no provisional signal. Peer `execution/transactions/semantic_models.yml` carries `quality_tier: approved` on multiple metrics, so candidate is *not* a forced platform default — this is a blocks-specific governance gap.
- **C14 — wrong sector tag.** `fct_execution_blocks_gas_usage_monthly.sql` line 4: `tags=['production','execution','transactions','gas']` — `blocks` absent, peers use it. Latent: production cron / refresh select `tag:production` and the model carries it, so it still builds; a `tag:blocks` selection would silently skip it. `check_api_tags.py` only inspects `api:`-tagged models (line 55) so the guard never examines it.
- **C15 — benign min fraq (re-confirmed non-issue).** `min(gas_used_fraq)=1.22e-7` on `2018-10-08` (Gnosis genesis era), max `0.947`, `0` values <0 and `0` values >1. Earliest-block minimal-activity artifact, all in-range — exactly the baseline's own conclusion.

### NEW (0)

None.

### UNVERIFIABLE / UNRESOLVED (0)

None — all 15 cases closed at the 3-round floor.

## Evidence appendix

**C01 / C08 — partial-month gas ratio (shared source).**
```sql
SELECT toStartOfMonth(date) AS m, count() AS days, sum(gas_used_sum) AS used,
       sum(gas_limit_sum) AS lim, round(sum(gas_used_sum)/sum(gas_limit_sum)*100,2) AS pct
FROM dbt.int_execution_blocks_gas_usage_daily
WHERE date >= toDate('2026-05-01') GROUP BY m ORDER BY m
```
Returned: May 2026 = 31 days, `38.9%`; June 2026 = 21 days (MTD), `45.83%`.
```sql
SELECT round(sum(gas_used_sum)/sum(gas_limit_sum)*100,2)
FROM dbt.int_execution_blocks_gas_usage_daily WHERE date >= toStartOfMonth(today())
```
Returned `45.83%` (== served api_ value). Monthly row num/den: `2,668,169,008,949` / `5,822,227,192,877`; May `3,454,008,353,041` / `8,878,932,091,554` (June both strictly smaller → partial). `api_execution_blocks_gas_usage_pct_monthly.sql` SELECT list = `(month AS date, ROUND(used*100,2) AS value)`, no completeness column. Grep of `int_execution_blocks_gas_usage_daily.sql`: no `WHERE date < today()` filter.

**C02 — grain uniqueness.**
```sql
SELECT 'gas_monthly', count(), uniqExact(month) FROM dbt.fct_execution_blocks_gas_usage_monthly
UNION ALL
SELECT 'clients_daily', count(), uniqExact((date,client)) FROM dbt.fct_execution_blocks_clients_daily
```
Returned `93`/`93` and `6,962`/`6,962`. `marts/schema.yml` lines 296-322 (monthly, only schema_changes) and 142-214 (clients_daily, no grain unique/not_null). Both fcts are pure GROUP BY over single refs.

**C03 — decode macro blast radius.**
```sql
SELECT count() AS n,
  countIf(extra_data!='' AND NOT match(extra_data,'^[0-9A-Fa-f]+$')) AS non_hex,
  countIf(extra_data!='' AND length(extra_data)%2=1) AS odd_len
FROM execution.blocks WHERE block_number < 5000000   -- and 5000000..30000000
```
Returned: blocks 0-5M = `5,000,100` rows, `0` non_hex, `0` odd_len; blocks 5M-30M = `25,000,100` rows, `0` non_hex, `0` odd_len. Code: `int_execution_blocks_clients_version_daily.sql` line 22 calls `decode_hex_tokens` (macros/execution/decode_hex_split.sql lines 21-40, unguarded) not `decode_hex_tokens2` (lines 42-94).

**C04 — type mismatch.** `describe_table dbt.int_execution_blocks_clients_version_daily` → `date` type = `DateTime` (live). `intermediate/schema.yml` line 10 declares `DateTime64`. schema_changes test present at lines 73-76 (severity warn).

**C05 — monthly api_ test gap.**
```sql
SELECT count(), toString(min(date)), toString(max(date)) FROM dbt.api_execution_blocks_gas_usage_pct_monthly
```
Returned `93`, `2018-10-01`, `2026-06-01`. `marts/schema.yml` lines 291-295: only `elementary.schema_changes`. Grandparent `int_execution_blocks_gas_usage_daily` carries volume + freshness + schema_changes (intermediate/schema.yml lines 110-143).

**C06 — Unknown bucket.**
```sql
SELECT count() row_total, countIf(client='Unknown') row_unknown,
       sum(cnt) blk_total, sumIf(cnt,client='Unknown') blk_unknown
FROM dbt.int_execution_blocks_clients_version_daily
```
Returned `1,126`/`13,219` rows (`8.52%`), `898,370`/`46,807,199` blocks (`1.92%`). 2024 monthly block-weighted Unknown: Jan `0.14%` → Jul `14.89%`, holding 13-14% through Dec. No catch-all in multiIf; no Unknown monitor in schema.yml.

**C07 — freshness recovery.**
```sql
SELECT date, gas_used_sum, gas_limit_sum FROM dbt.int_execution_blocks_gas_usage_daily
WHERE date >= today()-14 ORDER BY date
```
Both intermediates `max(date)=2026-06-21`. Contiguous Jun 7-21; interior days `285.97e9`-`287.91e9` gas_limit_sum, today partial `92.2e9`, no near-zero interior day. `docs/incidents/` contains only `logs_ingestion_gap_2026.md` (execution.logs, 2026-05-30 / 2026-06-14).

**C09 — sum-of-ratios.**
```sql
SELECT toString(date), round(sum(fraq),4), count()
FROM dbt.fct_execution_blocks_clients_daily WHERE date >= today()-3 GROUP BY date ORDER BY date DESC LIMIT 3
```
Returned `1.0`, `1.0`, `1.0` (5 clients/day). `semantic_models.yml`: measure `fraq_value` lines 44-46 (`agg: sum, expr: fraq`); registered metric lines 259-283. `fct_execution_blocks_clients_daily.sql` line 25: `fraq = cnt / SUM(cnt) OVER (PARTITION BY date)`.

**C10 — month-start shape.**
```sql
SELECT count(), toString(min(date)), toString(max(date)), countIf(toDayOfMonth(date)!=1)
FROM dbt.api_execution_blocks_gas_usage_pct_monthly
```
Returned `93`, `2018-10-01`, `2026-06-01`, `0`. `semantic_models.yml` dimension `date` `time_granularity: day` (lines 190-195), supported_time_grains `[day, week, month, quarter, year]` (lines 511-517); mart `SELECT month AS date`.

**C11 — base fee unsurfaced.**
```sql
SELECT count(), countIf(base_fee_per_gas>0), max(base_fee_per_gas)
FROM execution.blocks WHERE block_timestamp >= today()-7
```
Returned `129,075`, `129,075`, `1,560,204,201` wei. Grep `base_fee_per_gas` over `models/execution/`: only hit is `execution_sources.yml:74`.

**C12 — heuristic quality.**
```sql
SELECT toYear(toDate(date)), round(sumIf(cnt,client='Unknown')/sum(cnt)*100,2),
       round(countIf(client='Unknown')/count()*100,2)
FROM dbt.int_execution_blocks_clients_version_daily GROUP BY 1 ORDER BY 1
```
Returned: 2018-2022 `0%`/`0%`; 2023 `0.14%`/`46.56%`; 2024 `9.19%`/`43.3%`; 2025 `5.4%`/`6.02%`; 2026 `0%`/`0%`.
```sql
SELECT client, version, sum(cnt) FROM dbt.int_execution_blocks_clients_version_daily
GROUP BY client,version ORDER BY 3 DESC LIMIT 15
```
Top pairs: Nethermind/'' (`22.2M`), Parity/1.41.0 (`4.42M`), Parity/1.34.1 (`2.13M`), Parity/1.31.1 (`2.12M`), OpenEthereum/1.47.0 (`1.93M`), Unknown/'' (`898k`) — all clean, sane semver.

**C13 — tiers.** `semantic_models.yml`: all 8 models + metrics `quality_tier: candidate`. `execution/transactions/semantic_models.yml` lines 1121, 1141: `quality_tier: approved`. api_ marts carry `tier1` + `api:blocks_gas_usage_pct`.

**C14 — tag.** `fct_execution_blocks_gas_usage_monthly.sql` line 4: `tags=['production','execution','transactions','gas']`. `scripts/checks/check_api_tags.py` line 55 filters to `api:`-tagged models.

**C15 — min fraq.**
```sql
SELECT round(min(gas_used_fraq),9), argMin(date,gas_used_fraq), round(max(gas_used_fraq),6),
       countIf(gas_used_fraq<0), countIf(gas_used_fraq>1)
FROM dbt.int_execution_blocks_gas_usage_daily
```
Returned `1.22e-7`, epoch-day `17851` (=`2018-10-08`), `0.947`, `0`, `0`.

## Review log (>=3 rounds per case)

- **C01:** R1 CONFIRMED high (code: no guard; June 45.83% vs May 38.9%) → challenge: corroborate from consumer angle (num/den smaller than May) + grep daily-guard asymmetry → R2 CONFIRMED (num/den both < May; daily models carry `WHERE date<today()`) → challenge: grep intermediate for guard + size the error → R3 CONFIRMED high (intermediate has no `date<today()` filter; ~6.9pp drift) → closed.
- **C02:** R1 CONFIRMED medium (no grain test at fct) → challenge: prove materially exploitable (rows==distinct?) → R2 CONFIRMED medium (rows==distinct, latent) → challenge: GROUP BY vs JOIN fan-out → R3 CONFIRMED, tempered **low** (pure GROUP BY over unique intermediate grain, belt-and-suspenders) → closed.
- **C03:** R1 CONFIRMED medium (unsafe macro in use) → challenge: quantify blast radius → R2 CHANGED low (0/247,385 over 14 days) → challenge: full-history check (pre-Merge PoA era) → R3 CHANGED **low** (0 malformed across 30M pre-Merge/Merge blocks) → closed.
- **C04:** R1 CONFIRMED medium (describe DateTime vs DateTime64 declared) → challenge: does mismatch propagate? → R2 CONFIRMED medium (isolated to one intermediate; downstream is Date) → challenge: does it carry a schema_changes test (sets severity)? → R3 CONFIRMED **medium** (schema_changes warn test present → coordinated-change item) → closed.
- **C05:** R1 CONFIRMED low (only schema_changes on monthly api_) → challenge: confirm non-empty live view → R2 CONFIRMED low (93 rows, latent) → challenge: is a stall caught upstream? → R3 CONFIRMED **low** (grandparent carries volume/freshness; monthly chain itself unmonitored) → closed.
- **C06:** R1 CONFIRMED low (8.52% Unknown, no catch-all/monitor) → challenge: trend block-weighted, stable vs spike? → R2 CONFIRMED low (recent ~0% block-weighted) → challenge: 2024 block-weighted spike check → R3 CONFIRMED **low** (2024 spiked 0.14%→14.89% — gap has teeth, firmly-justified low) → closed.
- **C07:** R1 RESOLVED, attribution=microbatch_insert_overwrite asserted from end-state → challenge: substantiate transition/incident link or drop attribution; compute lag vs SLA → R2 RESOLVED, attribution dropped to **data-recovery** (no incident doc; T-3 lag ≠ insert_overwrite signature) → challenge: confirm recovery durable (per-day counts, no zero interior day) → R3 RESOLVED (contiguous healthy Jun 7-21, only today partial) → closed.
- **C08:** R1 CONFIRMED high (latest row 45.83% partial vs May 38.9%) → challenge: prove served value == source MTD; mutation proof → R2 CONFIRMED high (45.83%==45.83%; 46.08%@8d→45.83%@21d) → challenge: any is_partial/completeness column in served schema? → R3 CONFIRMED **high** (SELECT list is just date+value, no flag) → closed.
- **C09:** R1 CONFIRMED high (agg:sum over pre-computed fraq) → challenge: demonstrate sum(fraq)~=1.0/day → R2 CONFIRMED high (sum(fraq)=1.0 over 5 clients) → challenge: is metric actually queryable/exposed? → R3 CONFIRMED **high** (registered simple metric in MCP registry, not orphaned) → closed.
- **C10:** R1 CONFIRMED medium (day grain over month-start) → challenge: confirm data shape (all day=01, monthly spacing) → R2 CONFIRMED medium (93 rows, non_month_start=0) → challenge: quote exact supported_time_grains; error vs mis-bucket? → R3 CONFIRMED **medium** (lists day+week; silently mis-buckets, no guard) → closed.
- **C11:** R1 CONFIRMED medium (base_fee_per_gas in source, 0 model refs) → challenge: is source column populated? → R2 CONFIRMED medium (all 129,075 blocks >0, max 1.56 Gwei) → challenge: title literally claims base fee? covered elsewhere? → R3 CONFIRMED **medium** (title quotes base fee; surfaced in no execution sector) → closed.
- **C12:** R1 CONFIRMED medium (positional heuristic, no caveat) → challenge: pre/post-Merge Unknown share supports convention-drift? → R2 CHANGED medium (data inverts hypothesis: pre-Merge 0% Unknown) → challenge: are decoded client/version pairs sane? → R3 CHANGED **low** (top-15 pairs clean/recognizable; residual is missing caveat) → closed.
- **C13:** R1 CONFIRMED low (all 8 candidate, api_ public) → challenge: grep api_ marts for tier1/api: tags wired live → R2 CONFIRMED low (tier1 + api:blocks_gas_usage_pct wired) → challenge: is candidate a platform default or blocks-specific? → R3 CONFIRMED **low** (peer transactions promotes to approved → blocks-specific gap) → closed.
- **C14:** R1 CONFIRMED low (tagged transactions not blocks) → challenge: CI/selection impact real? → R2 CONFIRMED low (check_api_tags skips it; tag:blocks would exclude) → challenge: does production cron still build it? → R3 CONFIRMED **low** (production+execution tags still build it → latent footgun) → closed.
- **C15:** R1 CONFIRMED low (min 1.215e-7, max 0.947, 0 out-of-range) → challenge: tie min to date (resolve 2018-10-08 vs 2018-11-16) → R2 RESOLVED (lowest days all 2018 launch-era) → challenge: (none) → R3 CONFIRMED **low** (min 1.22e-7 on 2018-10-08, in-range, benign) → closed.

## Refreshed recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P1 (KEEP) | Add an in-progress-month guard (`WHERE month < toStartOfMonth(today())` or an `is_complete_month` flag) so the latest monthly row is not a partial-MTD aggregate served as final | `models/execution/blocks/marts/fct_execution_blocks_gas_usage_monthly.sql`, `models/execution/blocks/marts/api_execution_blocks_gas_usage_pct_monthly.sql` |
| P1 (KEEP) | Replace the `agg: sum` measure over the pre-computed `fraq` ratio with a derived `sum(num)/sum(den)` metric (ratio-of-sums) so MetricFlow/MCP multi-day queries are correct | `semantic/authoring/execution/blocks/semantic_models.yml`, `models/execution/blocks/marts/fct_execution_blocks_clients_daily.sql` |
| P2 (KEEP) | Fix the monthly semantic model time grain: bind the time dimension at `month` (not `day`) and drop `day`/`week` from supported_time_grains for the month-start `date` column | `semantic/authoring/execution/blocks/semantic_models.yml`, `models/execution/blocks/marts/api_execution_blocks_gas_usage_pct_monthly.sql` |
| P2 (KEEP) | Correct `data_type: DateTime64` → `DateTime` for the date column; coordinate with the `elementary.schema_changes` (warn) snapshot to avoid an alert | `models/execution/blocks/intermediate/schema.yml` |
| P2 (KEEP) | Either surface `base_fee_per_gas` in a blocks model (EIP-1559 base-fee metric) or narrow the unit title to drop the "base fee" scope claim | `models/execution/blocks/intermediate/int_execution_blocks_gas_usage_daily.sql`, unit doc title |
| P3 (KEEP) | Fix the sector tag: change `transactions` → `blocks` so `tag:blocks` selection includes the model | `models/execution/blocks/marts/fct_execution_blocks_gas_usage_monthly.sql` line 4 |
| P3 (KEEP) | Add `dbt_utils.unique_combination_of_columns` + not_null grain tests at the fct layer (belt-and-suspenders; intermediates already have them) | `models/execution/blocks/marts/schema.yml` |
| P3 (KEEP) | Add `elementary.volume_anomalies` + `freshness_anomalies` to the monthly api_ mart to match its daily siblings | `models/execution/blocks/marts/schema.yml` |
| P3 (KEEP) | Add a catch-all + Elementary monitor on the Unknown-client bucket (2024 saw a 0.14%→14.89% block-weighted spike that would have gone unseen) | `models/execution/blocks/intermediate/int_execution_blocks_clients_version_daily.sql`, `models/execution/blocks/intermediate/schema.yml` |
| P3 (KEEP) | Add a schema.yml caveat noting client identification is a positional `extra_data` heuristic, not an EIP standard | `models/execution/blocks/intermediate/schema.yml` |
| P4 (KEEP, low) | Promote blocks semantic models from `quality_tier: candidate` (or add a consumer-facing provisional signal) since api_ marts serve public endpoints | `semantic/authoring/execution/blocks/semantic_models.yml` |
| P4 (KEEP, low) | Switch `decode_hex_tokens` → `decode_hex_tokens2` for defense-in-depth (no live impact: 0 malformed across 30M historical blocks) | `models/execution/blocks/intermediate/int_execution_blocks_clients_version_daily.sql` line 22 |
| — (DROP) | Freshness lag recommendation — RESOLVED: both intermediates current to 2026-06-21, within SLA (data-recovery, no code change needed) | `int_execution_blocks_gas_usage_daily`, `int_execution_blocks_clients_version_daily` |
| — (NO ACTION) | `gas_used_fraq` min — confirmed benign (2018 launch-era artifact, all values in [0,1]) | `int_execution_blocks_gas_usage_daily` |
