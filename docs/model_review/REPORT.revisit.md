# dbt-cerebro Model Review — Revisit (2026-06-21)

**Baseline:** `docs/model_review/REPORT.md` (2026-06-11) — 20 P0, 56 High, ~98 Medium/Low
**Revisit:** 37 sector reports re-verified over 3–4 adversarial rounds each, against live code + warehouse, 10 days after the baseline
**Two June 2026 incidents intervened** between baseline and revisit: (A) the `microbatch_insert_overwrite` / REPLACE-PARTITION month-wipe, and (B) the short `logs_ingestion_gap` (`2026-05-30` ~5.5 min / `2026-06-14` ~8.5 min). Several baseline data-state symptoms cleared as a side effect of incident-A recovery refills, not code fixes.

---

## 1. Executive summary — what changed since 2026-06-11

**Headline: of the 20 P0 incidents, 2 P0 ids are fully clean (P0-09, P0-15), 4 are PARTIALLY resolved (a critical half cleared, a critical half still open: P0-08, P0-10, P0-12, P0-19), and 14 remain STILL-BROKEN at critical severity.** No P0 got worse in count, but two got worse in magnitude (Prices SAFE 3.1x→4.32x; State staleness 132d→142d) and the Yields `least()` epoch bug spread from 6,055 to 23,416 wallets after the lending recovery widened the wallet universe.

The dominant finding is that **almost every RESOLVED or improved P0 was healed by the June incident-A recovery refill (data restored), not by a code fix.** The underlying code defects in those sectors are still present and re-arm on the next misaligned refresh (explicitly flagged in accounts, lending, dao_treasury, shared, gnosis_app). Conversely, the genuinely code-rooted P0s (semantic column drift, forward-fill caps, unit mislabels, join_use_nulls, partition grain) are essentially untouched — the review window saw very few committed remediations.

Aggregate movement across all ~174 baseline cases plus newly-surfaced ones:

| Outcome | Count (approx.) | Notes |
|---|---|---|
| RESOLVED | ~24 cases | mostly staleness recoveries + a handful of real code fixes (CoW source-freshness test, CoW production tags, Pools dev-tag removal, Pools partition_by, Revenue partition grain, Mixpanel agg:max, accounts/yields/lending data refills) |
| CHANGED | ~50 cases | overwhelmingly severity right-sizing (down) after blast-radius measurement; a minority are status reframes |
| STILL-CONFIRMED | ~95 cases | every baseline systemic code defect that was not data-state-only |
| NEW | 9 cases | surfaced during re-verification (see §5); 2 critical-or-high (MMM join fanout, Safe owner-drop re-rooted) |
| UNVERIFIABLE | 0 fully | a handful of privacy/registry-gated sub-claims settled statically; mixpanel_ga data was queryable in this cluster (the feared privacy block did not apply) |

Two systemic patterns are materially better: **dev-tags on production** (Pools removed all 13, CoW added production to 18 marts) and **silent freshness** (CoW now has a wired `36h/48h` source-freshness test). The other six systemic patterns are essentially unchanged.

---

## 2. P0 status board

| P0 | Sector | Original incident (short) | Current status | One-line proof (the number) | Incident attribution |
|---|---|---|---|---|---|
| P0-01 | ESG | `int_esg_carbon_intensity_ensemble` `toStartOfYear` evicts 11/12 months/yr | STILL-BROKEN (critical) | only `4` distinct months survive `>=2023-12-01`; partition_by unchanged | none (standalone toStartOfYear defect) |
| P0-02 | ESG | semantic measures reference nonexistent fct columns | STILL-BROKEN (critical) | `13` measures (not 2) bind to phantom columns | none |
| P0-03 | Consensus | `apy_30d` ~30x overstated in validator explorer | STILL-BROKEN (critical) | API view max `apy_30d = 3,322,693.3%`, p90 `266.5%` (inflated = countDistinct(date)x) | none |
| P0-04 | Consensus | `int_consensus_validators_labels` dev-tagged, bare table ref | STILL-CONFIRMED (critical code / medium live) | dev tag + bare FROM intact; `dbt ls ...,tag:production` EMPTY; serves stale 873-row labels | none |
| P0-05 | Contracts AMM-DEX | 7 new UniswapV3 pools never backfilled | STILL-BROKEN (critical) | the 7 May-2026 pool addresses each `count()=0`; watermark gate `block_number > 46806996` | none |
| P0-06 | Contracts Circles | 5 calls models read `transactions` not `traces` | STILL-BROKEN (critical) | all 5 `_calls` = 0 rows vs events thousands; StandardTreasury traces recoverable (`7` Mar-2026, `732` offer-family) | none |
| P0-07 | Contracts Lending | Agave `LendingPool_events` 100% undecoded | STILL-BROKEN (critical) | `63,381,065` rows, `100%` blank `event_name`; address has `0` event_signatures rows | none |
| P0-08 | Contracts Prediction | OmenAgentResultMapping 309d stale / runner 30-slice cap | CHANGED + STILL-CONFIRMED | OmenARM reframed CHANGED critical→low (genuine chain retirement, last on-chain `2025-08-06`); BUT runner silent-drop (C02 high) + FPMM calls `81d` stale (C12 high) persist; FPMM clone trading (C10) still critical | none |
| P0-09 | Execution Accounts | `fct_execution_account_token_movements_daily` 0 rows | RESOLVED | `0 → 40,116,990` rows, contiguous, `0` dup keys; profile token_transfer_count positive on 41% | microbatch_insert_overwrite (recovery) |
| P0-10 | Execution CoW | `cow_api_trade_fees` 42d stale, fee KPIs NULL; top-pairs semantic mismatch | PARTIAL | staleness RESOLVED (`42d→5d`, KPIs `90.37`/`24.02`); semantic column-drift `execution_cow_top_pairs_weekly` STILL-CONFIRMED high (mart `date/label/value` vs semantic `week/pair/volume_usd/num_trades`) | other (P0-10 ingestor fix) |
| P0-11 | Execution GBCDeposit | raw-Gwei approved metric (~1e9x), no `0x01` BLS guard | STILL-BROKEN (high) | June raw sum `552,009,832,341,274` Gwei = `552,009.83` GNO served as GNO sum; `6,667/14,563` (`45.8%`) 0x00-BLS address nodes | none |
| P0-12 | Execution Gnosis App | user activity truncated to 2 months; swap fee `$0` | PARTIAL | onboard anchor RESOLVED (`2,477 → 24,020` rows / 8 months); swap fee `$0` STILL-CONFIRMED critical (`44,904` filled trades all `fee_amount=0`, `$34,665` recoverable) | batch-vars truncation (recovery) |
| P0-13 | Execution GPay | cashback native GNO published as "USD" | STILL-BROKEN (critical) | endpoints return `sum(amount)` GNO; schema.yml says "in USD"; `21,201.31` GNO cashback, GNO-only | none |
| P0-14 | Execution GPay | activity-spine RMT order_by omits `direction` | STILL-BROKEN (critical) | net silently-dropped USD = `$291,742.20` across `372` two-direction keys (was 234 groups) | none |
| P0-15 | Execution Lending | WxDAI utilization Int256 underflow ~4.5e27 | RESOLVED (data) | WxDAI `util>1000 EVER=0`, `e27 EVER=0`; served yields max utilization `98.07%`; Int256 code still latent | microbatch_insert_overwrite (recovery) |
| P0-16 | Execution Prices | SAFE forward-fill 3.1x overstatement | STILL-BROKEN (critical, worse) | hub SAFE `0.366661` vs Dune `0.084782` = `4.32x` (was 3.1x); `~$1.95M/day` overstatement into `fct_execution_tokens_metrics_daily` | none |
| P0-17 | Execution State | `bytes_diff` counts overwrites as new allocations, ~2.5x | STILL-BROKEN (critical) | API serves `70.710707424 GB` vs corrected ~`32 GB` (`2.208x` on Jan-2026); source `142d` stale (was 132d) | none |
| P0-18 | Execution Shared | `is_lending_user` = 0 for all 5.8M addresses | STILL-BROKEN (critical) | `sum(is_lending_user) = 0` across `5,816,837` rows; roles mart byte-identical to baseline (not rebuilt) despite upstream `fct_execution_yields_user_lending_positions_latest` healing to `20,308` rows | none (mart not in production cron) |
| P0-19 | Execution Yields | `least(DateTime,NULL)`→1970 dates; `active_lending_positions=0` | PARTIAL | `active_lending_positions` RESOLVED (`0 → 19,709` wallets); `least()` epoch STILL-CONFIRMED critical and WORSE (`23,416/24,614` wallets `first_yield_date=1970-01-01`, up from 6,055) | C02 other (table refresh) / C01 none |
| P0-20 | Mixpanel GA | `matched_mp=1` for all rows; GP flags 100% | STILL-BROKEN (critical) | `matched_mp=1` for all `23,239` rows; `13,253` are join-default `mp_user_id_hash=0`; true match rate `9,986/23,239 = 43%` | none |

**Tally: fully-clean P0 ids = 2 (P0-09, P0-15); PARTIAL = 4 (P0-08, P0-10, P0-12, P0-19, each with a resolved half + an open critical/high half); STILL-BROKEN/CONFIRMED = 14 (P0-01, P0-02, P0-03, P0-04, P0-05, P0-06, P0-07, P0-11, P0-13, P0-14, P0-16, P0-17, P0-18, P0-20).** 18 of 20 P0 ids retain at least one open critical or high finding.

---

## 3. Aggregate delta

### Severity tier x status

| Tier | RESOLVED | CHANGED | STILL-CONFIRMED | UNVERIFIABLE |
|---|---|---|---|---|
| P0 (20 ids) | 2 fully (P0-09, P0-15) | 4 reframed/partial (P0-08, P0-10, P0-12, P0-19) | 14 retain a critical defect | 0 |
| High | ~7 | ~14 (mostly down-rated) | ~33 | 0 |
| Medium | ~6 | ~22 | ~40 | 0 |
| Low | ~9 | ~11 | ~60 | 0 |

(High-row resolutions include: CoW C01/C02/C03/C17, Pools C05, Yields C16, Safe C04, Accounts C20. The bulk of CHANGED-High is severity right-sizing after blast-radius measurement, e.g. Lending C01/C02/C04/C16, Pools C01/C18/C19, Tokens C19/C20, Safe C01/C05/C11, GPay/GnosisApp NULL-propagation cases.)

### Still-open P0 (critical) — the priority list

1. ESG `int_esg_carbon_intensity_ensemble` `toStartOfYear` partition eviction (P0-01)
2. ESG semantic measures 13 phantom columns (P0-02)
3. Consensus `apy_30d` window-count inflation, served in API view (P0-03)
4. Consensus `int_consensus_validators_labels` dev-tag + bare ref (P0-04)
5. Contracts AMM-DEX 7 UniswapV3 pools never backfilled (P0-05)
6. Contracts Circles 5 `_calls` models on `transactions` not `traces` (P0-06)
7. Contracts Lending Agave `LendingPool_events` 63.4M rows 100% undecoded (P0-07)
8. Contracts Prediction FPMM clone trading entirely uncaptured (P0-08 / C10)
9. GBCDeposit raw-Gwei ~1e9x approved metric + BLS-unguarded entity (P0-11)
10. Gnosis App swap fee `$0` on all 44,904 filled trades (P0-12 / C02)
11. GPay cashback native GNO published as "USD" (P0-13)
12. GPay activity-spine direction collapse, `$291,742` dropped (P0-14)
13. Prices SAFE forward-fill 4.32x overstatement, `~$1.95M/day` (P0-16)
14. State overwrite overcount + 142d staleness, tier-1 `70.71 GB` (P0-17)
15. Shared `is_lending_user=0` (roles mart not rebuilt) (P0-18)
16. Yields `least()` epoch dates on 23,416 wallets (P0-19 / C01)
17. Mixpanel GA `matched_mp=1` for all rows (P0-20)
18. MMM week-only join fanout (NEW N01, see §5)

### Still-open High (selected, by sector)

Consensus (Staked GNO `/32` 32x, unweighted APY KPI, 0x02 Pectra gap), AMM-DEX (BalancerV2 omission), Lending (bC3M 59d-stale price served, lenders STOCK vs borrowers FLOW), Prediction (runner silent-drop, FPMM calls 81d stale, data-terminus), Accounts (no-FINAL RMT, silent-empty balances rebuild, non-reproducible old-cohort retention, zero semantic coverage), CoW (incremental-lookback solver/cow_ratio corruption, top-pairs semantic break), Gnosis App (high-confidence threshold 2 vs 3, identity-bridge drop), GPay (price coalesce-to-0, undocumented tier1 columns, total_funded conflation, non-complementary churn/retention), Live (Balancer V3 empty-address cascade, no CoW coverage), MMM (collinearity NaN, half-empty registry, plaintext-wallet exposure), Pools (BalancerV2 omission, LVR `$500` floor), Prices (no forward-fill cap, hub grain test), RWA (unbounded forward-fill, freshness-blind), Safe (v1.4.1 owner-drop, 8-dup fan-out, dead v2 artifact), Tokens (negative supply served, no not_negative test, circulating-supply drift, totalSupply reconciliation), Transactions (Int32 gas-price truncation), Transfers (bridges join_use_nulls, volume_usd hardcoded NULL), Yields (token_address grain collapse, overview forward-ref, broken sDAI APY measures, plaintext-wallet exposure), Circles v2 (v1 transfers schema drift, wrapper_share sawtooth, dev-tagged v1 stack, layer inversion, 7d KPI window mismatch, broken semantic columns), P2P (vacuous discv4 pct_successful, geo `''` pollution, topology edge drop, discv5 geo cap, OR-vs-AND split, phantom docs), Probelab (5 marts missing api: tags), Revenue (OOM-hook gap, dual cohort/totals threshold, gnosis_app semantic gap), Quarterly (peak-swappers no guard, no completeness flag), DAO Treasury (unguarded max(date) anchor, no tests, ETH-class misclassification), Zodiac (140k Delay/Roles proxies outside registry), Crawlers (94-month partition cap, bridge-as-aggregator misattribution).

---

## 4. Systemic patterns revisited

| # | Pattern | Baseline scope | Revisit status |
|---|---|---|---|
| 1 | `join_use_nulls` absent on LEFT JOINs | 12+ confirmed | **Unchanged — 0 fixed.** Still biting in P2P (discv4 geo `48.4%` empty, topology `~46%`/`~58%` edges dropped), Transfers bridges (direction always `out`, `98.45%` empty bridge_contract), Mixpanel GA (`matched_mp=1`), GPay (`coalesce(p.price,0)` zeros 47 GBPe rows), ESG (`13.61%` zero-CI). Latent-but-unfixed in Accounts, Tokens, Safe, Zodiac, UBO, Prices, Bridges, Pools. No platform-wide `join_use_nulls=1` set. |
| 2 | Semantic column drift (YAML vs live SQL) | 10 sectors | **Unchanged — broken approved-tier metrics persist.** ESG (13), CoW (top_pairs_weekly), Yields (apy_7DMA/30DMA), GBCDeposit (Gwei), Bridges (`d` time dim), Tokens (phantom dims + Transactions broken `address` dim), State (agg:sum over cumulative), Circles v2 (3 models bind nonexistent columns), RWA (triple-registration). No CI catalog-vs-YAML check added. |
| 3 | dev tags bypassing production CI | 5 sectors | **Materially improved.** Pools removed all 13 (RESOLVED); CoW added production to 18 marts + 4 staging (RESOLVED). STILL-OPEN: Consensus labels (P0-04), Lending top_lenders (3), Transfers whitelisted_raw, Circles v2 (6 v1), Circles contracts v1 Hub, Bridges v2, DAO Treasury 4 KPI marts. |
| 4 | ReplacingMergeTree read without FINAL | 8 sectors | **Unchanged — all latent.** Accounts (2 api views), Safe (8-dup fan-out — the one with a proven divergent served value: `deployment_timestamp`), UBO (3 cumsum regular path), Bridges, Prices oracle, Pools (prev_balances + v3 registry), Consensus performance views, RWA fct, Revenue weekly. 0 dups today except Safe. |
| 5 | Phantom schema.yml columns | 14 sectors | **Unchanged — pervasive.** All 9 BackedFi + Agave + GBCDeposit, Transfers (5+4), P2P (compiled phantom test nodes proven from manifest), Bridges (phantom `d` reaches `get_model_details`), State, Transactions (phantom + live-broken semantic dim), Consensus (6), ESG (`peer_id` tests break CI), Tokens, Crawlers (13 CTE cols), Probelab, RWA. |
| 6 | Silent freshness failures | 9 sectors | **Improved once, unchanged elsewhere.** CoW wired `36h/48h` source-freshness (RESOLVED). State `142d` filter-masked (worse). RWA bC3M `59d` blind (worse). Crawlers cow re-stalled `140h` (NEW N01). Prediction runner silent-drop unfixed. Consensus all `severity:warn`. GBCDeposit/Zodiac/Live/Transactions staleness recovered via incident-A but warn-only controls unchanged. |
| 7 | Partition cap (toStartOfMonth wide history) | 3 active + 3 approaching | **Mixed.** Pools balances_daily RESOLVED (42 months, ~2029). Revenue monthly RESOLVED (`toStartOfYear`→`'month'`, baseline P0-14). ESG STILL the `toStartOfYear` eviction (P0-01). State `88` (~12mo), Crawlers `94` (~6mo) STILL high. Prediction re-scoped down (refresh.py batches by 6). |
| 8 | Candidate-tier on public endpoints | 9 sectors | **Essentially unchanged.** ESG (195/195), Blocks (8), Probelab (5), Tokens, P2P (20), Bridges (1 YAML-promoted but registry still rejects), Circles, RWA, Transfers all candidate; GBCDeposit `approved` on a self-described "auto-generated candidate"; CoW 4 candidate marked approved. No candidate→tier0/1 quality gate added. |

---

## 5. Newly introduced since 2026-06-11

| Sector | Case | Severity | Summary |
|---|---|---|---|
| Execution MMM | N01 | **critical** | Un-keyed week-only LEFT JOINs cross-product the 3 long-form intermediates; every spine/API/baseline magnitude inflated by a fixed factor — KPI x72, media x117, control x104 (measured exact). `api_execution_mmm_spine_weekly` is `SELECT *` over the inflated spine. Pre-existing structural bug missed at baseline. |
| Execution Safe | C01 re-rooted | **critical** | v1.4.1/v1.4.1L2 AddedOwner/RemovedOwner NULL-owner (`115,731` rows, 100% of cohort) re-attributed: seed `indexed:false` is correct; `decode_logs` reads owner from the empty data slot when it is actually in `topic1`. Same symptom, corrected root cause; remains critical. |
| Execution Shared | N01 | high | Fresh duplicate-grain double-loads in `int_execution_lending_aave_user_balances_daily` (epochs `20622`/`20624` = `2x`; `20623` value-doubled). fct `latest_date` targets the doubled epoch — a rebuild inflates positions/USD `~2x`. Incident-A double-write family. |
| Contracts AMM-DEX | N01 | high | Deployed `dbt.contracts_whitelist` (34 rows) diverged from `seeds/contracts_whitelist.csv` (41) — `dbt seed` never re-run after May commits. Operational root of P0-05. |
| Execution Lending | N01 | medium | Doubled `2026-06-18` partition (`82,260`/`41,138`; `310` value-conflict grains). Recovery-refill's own un-deduped append side-effect; unreachable today. |
| Crawlers Data | N01 | medium | `cow_api_trade_fees` loader re-stalled `~140h` (last ingest `2026-06-16`), breaching `48h` error_after. Corroborates CoW P0-10 ingestor instability re-emerging. |
| Execution Gnosis App | N01 | medium | `2026-05` token_offer_claims partition not reprocessed after offer became priced; `346` real GNO claims permanently `$0`/NULL cycle. Residual behind C06's half-fixed state. |
| Execution Live | N01 | resolved | "decode tables 25-45x sparser" surfaced then overturned in-round as a methodology artifact (2h buffer vs 48h retention; in-buffer ratio `1.0x`). Not a defect. |

The consequential NEW items are the MMM fanout (most impactful, a baseline miss) and the Safe/Shared/Lending defects that are direct fallout of the June incident-A recovery process.

---

## 6. Recommended priority fix list (refreshed) — still-open only

### P0 — critical, data actively wrong in production

| # | Action | Sector | File(s) |
|---|---|---|---|
| 1 | `int_esg_carbon_intensity_ensemble` partition `toStartOfYear`→`toStartOfMonth`; backfill evicted months | ESG | `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql` |
| 2 | Repoint 13 ESG semantic measures to live aliased columns | ESG | `semantic/authoring/ESG/semantic_models.yml` |
| 3 | Divide `apy_30d` by `countDistinct(date)`; fix API view (max `3,322,693%`) | Consensus | `fct_consensus_validators_explorer_latest.sql`, `_members_table.sql`, `api_consensus_validators_explorer_latest.sql` |
| 4 | Drop `dev` tag + `ref()` on `int_consensus_validators_labels` | Consensus | `models/consensus/intermediate/int_consensus_validators_labels.sql` |
| 5 | `dbt seed` (N01) then `--full-refresh` `contracts_UniswapV3_Pool_events` (C01) | Contracts AMM-DEX | `seeds/contracts_whitelist.csv`, `contracts_UniswapV3_Pool_events.sql` |
| 6 | Switch 5 Circles `_calls` models `tx_table` to `traces` (one-line each) | Contracts Circles | `models/contracts/Circles/*_calls.sql` (5) |
| 7 | Register Agave ABI for `0x5E15...6d9c` in event_signatures, or disable | Contracts Lending | `seeds/event_signatures.csv`, `contracts_agave_LendingPool_events.sql` |
| 8 | Build per-market EIP-1167 clone trading layer (Omen FPMM trading is 0) | Contracts Prediction | `contracts_FPMMDeterministicFactory_events.sql` + new clone model |
| 9 | Make `dbt_incremental_runner.py` exit non-zero on `>max_slices_per_stage`; backfill FPMM calls 81d gap | Contracts Prediction | `scripts/refresh/dbt_incremental_runner.py`, `contracts_FPMMDeterministicFactory_calls.sql` |
| 10 | Fix MMM week-only join fanout (pivot to 1 row/week before join); re-materialize | Execution MMM | `fct_execution_mmm_spine_weekly.sql`, `api_execution_mmm_spine_weekly.sql`, `fct_execution_mmm_baseline_latest.sql` |
| 11 | Divide GBCDeposit `amount` by `1e9`; fix `wei`→`GNO`; add `0x01` BLS guard | GBCDeposit | `int_GBCDeposit_deposists_daily.sql`, `semantic_models.yml`, `models/contracts/GBCDeposit/schema.yml` |
| 12 | Switch GA swap-fee source to `fct_execution_cow_trades.fee_usd` (`$34,665`) | Execution Gnosis App | `int_execution_gnosis_app_swaps.sql`, `int_execution_gnosis_app_swap_fees_daily.sql` |
| 13 | Convert cashback to USD or rename column/desc to GNO (~100-300x) | Execution GPay | `api_execution_gpay_user_total_cashback.sql`, `api_execution_gpay_user_cashback_daily.sql`, `schema.yml` |
| 14 | Add `direction` to RMT order_by AND uniqueness test (`$291,742` dropped) | Execution GPay | `int_execution_gpay_activity_daily.sql` + `marts/schema.yml` |
| 15 | Cap SAFE forward-fill / demote stale native below Dune (`4.32x`, `~$1.95M/day`) | Execution Prices | `int_execution_prices_native_daily.sql`, `int_execution_token_prices_daily.sql` |
| 16 | Fix `bytes_diff` to condition on `from_value`; restore storage_diffs ingestion (142d) | Execution State | `int_execution_state_size_full_diff_daily.sql` (+fct/api), upstream cryo-indexer |
| 17 | Rebuild `int_execution_address_roles_current` (is_lending_user 0/5.8M) + schedule; drop doubled `20622`/`20624` lending partitions first | Execution Shared | `int_execution_address_roles_current.sql`, `int_execution_lending_aave_user_balances_daily.sql` |
| 18 | Wrap `least()` args in `coalesce()` for first_yield_date; full-refresh (23,416/24,614 epoch) | Execution Yields | `fct_execution_yields_user_lifetime_metrics.sql`, `api_execution_yields_user_kpis.sql` |
| 19 | Add `join_use_nulls=1` to `matched_mp`, rebuild (100% vs true 43%) | Mixpanel GA | `fct_mixpanel_ga_gnosis_app_users.sql` |
| 20 | Fix CoW top-pairs semantic column drift + `addDays(max,-3)` incremental lookback (solver/cow_ratio corruption) | Execution CoW | `semantic/authoring/execution/cow/semantic_models.yml`, `api_execution_cow_top_pairs_weekly.sql`, `int_execution_cow_trades.sql`, `int_execution_cow_batches.sql` |

### High — fix within 1 sprint

- Fix v1.4.1/L2 owner argument-mapping in decode_logs (115,731 NULL rows) — Safe
- Dedup the 8-dup Safe fan-out (divergent `deployment_timestamp` served) — Safe
- Staleness guard + `last_oracle_date` on bC3M forward-fill (59d stale served) — RWA/Contracts
- Surface BalancerV2 in fees/volume/TVL or document omission (25.98M events) — Pools/AMM-DEX
- `balance > 0` floor + `not_negative` test on token supply (`wstETH -$589,706`) — Tokens
- `$500` TVL floor + `accepted_range(<=0)` on `lvr_apr_7d` (`5e19` outliers) — Pools
- `join_use_nulls=1` on bridges flows + register `amount_raw_sum` (direction always out) — Transfers
- `join_use_nulls=1` on discv4 peers + topology; fix vacuous discv4 pct_successful; remove 4 phantom docs from live cerebro-docs — P2P
- `gas_price_avg/median` Int32→Float64 (1,509/2,793 zeroed tier-1 rows) — Transactions
- `!= ''` guard + CoW/GPv2 coverage on the live feed (BalV3 92.9% empty; ~25-33% understatement) — Live
- `api:probelab`+`granularity:daily` tags on 5 marts (invisible to registry) — Probelab
- `privacy:tier_*`/`expose_to_mcp:false` on 7 plaintext-wallet yields marts — Yields
- Repartition State + Crawlers labels to `toStartOfYear` before 100-partition cap — State/Crawlers
- Remove `/32` from Staked GNO + fix schema desc (32x) — Consensus
- Completeness guard on DAO Treasury `max(date)` anchor (lending-only `$0` re-arms) — DAO Treasury
- `refill_safe_*` OOM hooks on 3 revenue fee models; add `has_gnosis_app` semantic dim — Revenue
- `token_address` in yields activity RMT ORDER BY (multi-token Balancer collapse, 115,301 groups) — Yields
- `date<today()` guard on gnosis_app peak_swappers; completeness flags on 22 quarterly marts — Quarterly
- Data-driven registry mastercopy filter / alert (139,856 Delay/Roles proxies excluded) — Zodiac
- Fix 4 Circles v2 `*_7d` KPI window predicates + 3 broken semantic models + v1 transfers schema — Circles v2

---

## 7. Method & caveats

**Re-verification approach.** Each of the 37 sectors was re-run by a 3-agent loop (inspector / verifier / orchestrator) over >=3 rounds per case, with live ClickHouse queries and code reads each round. This synthesis aggregates the per-sector verdicts; it does not re-derive from the warehouse. Every per-sector report carries a per-case review log and an evidence appendix with the proving query/number cited in section 2.

**The two June incidents are central to reading the deltas.** Incident A (`microbatch_insert_overwrite` REPLACE-PARTITION month-wipe, ~`2026-06-18`/`06-19`) and its recovery refills are responsible for most data-state improvements: P0-09 (accounts fct repopulated), P0-15 (lending utilization cleared), P0-19's positions half, plus staleness recoveries in transactions, blocks, zodiac, live, gnosis_app, safe, lending, ubo. In every such case the per-sector report verified the code defect was NOT touched (git showed no model change, or the fix lived in a macro), so these are "mitigated-by-data, not fixed-by-code" and re-arm on the next misaligned refresh. Two NEW defects (Shared-N01, Lending-N01) are the recovery's own un-deduped double-write side effects. Incident B (`logs_ingestion_gap`, two ~5-10 min windows) was explicitly ruled out as the cause of any multi-day table lag and left no holes in the verified series.

**Severity right-sizing dominates the CHANGED column.** ~40 cases moved down a tier not because anything was fixed but because the revisit measured the realized blast radius and found it small (Lending Int256 underflow now 0 live rows; Tokens negatives no longer flip a sign today; Safe changeMasterCopy 0 in all scanned windows; many "no FINAL / no join_use_nulls" cases proven 0-dups / 0-empties today). These remain CONFIRMED code defects — latent, not absent.

**Attribution honesty.** Several baseline incident attributions were corrected during revisit: Prices P0-16's 3-day lag was reframed from `microbatch_insert_overwrite` to normal cron catch-up (the SAFE overstatement itself is none-attributed and worsened); Yields P0-19's positions recovery was a plain table refresh, not REPLACE-PARTITION; RWA bC3M and Prediction OmenARM are genuine source/chain silence, not pipeline incidents.

**No sector was fully UNVERIFIABLE.** The mixpanel_ga privacy-tier concern the unit note flagged did not materialize — data queries ran in this cluster, so P0-20's `matched_mp` and the device over-count were measured directly (true match rate `43%`, device over-count `~7x`). Residual unverifiable sub-claims, all settled statically with high confidence: a few semantic-execution paths blocked by an environment-wide `manifest_hash_mismatch` (settled via static column-diff: ESG, CoW, Yields, Circles, Tokens, RWA semantic breaks), Safe C11's single full-history aggregate (exceeds the 30s MCP cap; settled on consistent ~0 windowed signal), and a handful of privacy-gated identity-bridge internals (Gnosis App C03 net-drop-vs-collapse split). DDL/`system.parts` inspection was blocked by the read-only MCP keyword guard across all sectors, so a few engine/partition-count facts are inferred-by-construction rather than read from `SHOW CREATE`.
