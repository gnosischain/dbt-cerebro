# Model review: execution/dao_treasury

**Convergence:** converged in 1 round — inspector and context reports are mutually consistent on all headline findings; the sole severity adjustment (alias-shadowing downgraded from high to low after live warehouse verification) was made by the analyst, not disputed between agents.

---

## Scope and inventory

The unit tracks GnosisDAO on-chain holdings and asset allocation on Gnosis Chain across six labelled Safe wallets, combining ERC-20 token balances with Aave V3 and SparkLend supply positions.

| Layer | Count | Notes |
|---|---|---|
| Seeds | 1 | `dao_treasury_wallets.csv` — 6 Safe addresses |
| Intermediate | 1 | `int_dao_treasury_holdings_daily` — daily incremental fact, partitioned by month |
| KPI marts (api_) | 4 | total_holdings, non_gno_holdings, gno_held, lending_total — all with 7-day change_pct |
| Time-series mart | 1 | `api_dao_treasury_holdings_by_class_ts` — daily value by asset class |
| Allocation mart | 1 | `api_dao_treasury_allocation_latest` — latest snapshot with percentage shares |
| Detail mart | 1 | `api_dao_treasury_holdings_detail_latest` — per-wallet, per-token position list |
| Semantic models | 0 | None — full semantic coverage gap |
| dbt tests | 0 | None defined in either schema.yml |

All seven mart models carry tag `dev` (not `production`); the corresponding dashboard sector (`dao-treasury.yml`) is set to `enabled: false`. The unit is not exposed to production API or MCP consumers in its current state.

---

## Business context

This unit answers "what does GnosisDAO hold on Gnosis Chain and how is it deployed?" for the Gnosis analytics team and DAO governance stakeholders. Business questions addressed: (1) total USD value of treasury holdings (wallet + lending) with 7-day change; (2) non-GNO NAV component in USD (diversified treasury health proxy); (3) GNO token holdings in native units across all GNO-derivative forms; (4) USD value of capital deployed into Aave V3 and SparkLend supply positions; (5) daily time-series by asset class (GNO / Stablecoins / RWA / ETH / BTC / Other); (6) latest allocation by token with percentage shares; (7) detailed position list per wallet-token-protocol for governance transparency.

**Canonical definitions:**

- **Treasury wallet universe:** six Safe addresses in `seeds/dao_treasury_wallets.csv` — GNO Main Treasury (`0x458cd345`), Stables & Staking (`0x509ad727`), Gnosis Chain Stables (`0x5be8ab1c`), Gnosis Chain Treasury (`0x10e4597f`), Aave Lending (`0x9065a0f9`), GNO Micro (`0xcdf50be9`). Any wallet not in this seed is out of scope.
- **GNO holdings:** sum of native-unit balance for symbols GNO, sGNO, spGNO, and aGnoGNO (staked, SparkLend supply, and Aave V3 supply forms respectively). Expressed in GNO native units, independent of price.
- **Non-GNO holdings:** USD sum of all positions where `symbol NOT IN ('GNO','sGNO','spGNO','aGnoGNO')`, covering stablecoins, RWAs, ETH, BTC, and other tokens.
- **Lending total:** USD sum of all positions where `position_type = 'lending'`, sourced from `int_execution_lending_aave_user_balances_daily`.
- **Asset class taxonomy:** GNO = {GNO, sGNO, spGNO, aGnoGNO}; Stablecoins = `token_class = 'STABLECOIN'` OR lending position with symbol in {WxDAI, USDC.e, USDC, USDT, EURe, GBPe, BRLA, BRZ, sDAI}; RWA = `token_class = 'RWA'`; ETH = WETH (wallet or lending); BTC = WBTC; Other = everything else. This CASE logic is hardcoded verbatim in two separate mart SQL files.
- **7-day change_pct:** `(current_val - prior_val) / prior_val * 100` where prior_val is the value exactly 7 calendar days before `max(date)`; uses `nullIf` to guard against division by zero.
- **Materialization grain:** `int_dao_treasury_holdings_daily` is a daily incremental table (insert_overwrite, partitioned by month); declared order_by key is `(date, wallet_address, position_type, protocol, symbol)`.

**Contract context:** Aave V3 pool at `0xb50201558B00496A145fE76f7424749556E326D8`; SparkLend pool at `0x2Dae5307c5E3FD1CF5A72Cb6F698f915860607e0`; supply tokens verified in `seeds/atoken_reserve_mapping.csv` and `seeds/lending_market_mapping.csv`. The six treasury wallet addresses are not cross-referenced in `contracts_whitelist.csv` (which covers protocol contracts, not DAO Safes) — ownership verification against an on-chain Safe registry is outstanding.

---

## Implementation assessment

### Critical

**max(date) anchor unguarded against partial upstream refresh — all six marts currently serve $0 KPIs**

`int_dao_treasury_holdings_daily` is a UNION ALL of `int_execution_tokens_balances_daily` (wallet ERC-20 rows) and `int_execution_lending_aave_user_balances_daily` (lending rows). When the lending source increments a day ahead of the wallet source, `max(date)` resolves to a lending-only date. Verified: date 2026-06-09 has 13 lending rows summing to $0 balance_usd and zero wallet rows, versus date 2026-06-07 which has 60 rows across all asset classes totalling ~$118M USD. Every mart — `api_dao_treasury_kpi_total_holdings`, `api_dao_treasury_kpi_non_gno_holdings`, `api_dao_treasury_kpi_gno_held`, `api_dao_treasury_kpi_lending_total`, `api_dao_treasury_holdings_detail_latest`, `api_dao_treasury_allocation_latest` — uses `WHERE date = (SELECT max(date) ...)` with no guard. All six currently serve ~$0 total holdings and ~-100% change_pct. Fix: anchor the mart `max(date)` to the latest date where wallet rows exist (e.g. `WHERE position_type = 'wallet'` subquery), or where both position_types are present.

Affected models: `models/execution/dao_treasury/intermediate/int_dao_treasury_holdings_daily.sql`, all six mart files under `models/execution/dao_treasury/marts/`.

### High

**No dbt tests on declared grain or any critical column**

Neither `models/execution/dao_treasury/intermediate/schema.yml` nor `models/execution/dao_treasury/marts/schema.yml` defines a single `not_null`, `unique`, or `accepted_values` test. The declared order_by grain `(date, wallet_address, position_type, protocol, symbol)` is currently clean (0 duplicate rows confirmed over 44,192 rows), but that integrity is unprotected. An upstream dedup failure in `int_execution_tokens_balances_daily` or a freshness regression in either source would be completely invisible. At minimum: add `dbt_utils.unique_combination_of_columns` on the grain, `accepted_values` on `position_type`, and a freshness or minimum-row-count test before promotion.

**wstETH (and SAFE, COW) fall to "Other", understating ETH allocation**

The ETH branch in both `api_dao_treasury_allocation_latest` and `api_dao_treasury_holdings_by_class_ts` matches only `WETH`. wstETH (wrapped staked ETH, an ETH derivative) has no explicit branch and falls to "Other" for both wallet and lending positions. Confirmed on 2026-06-07: wstETH $719K, SAFE $1.4M, COW $707K all in "Other" (~$2.8M aggregate). wstETH peaked at ~$2.8M historically. This understates ETH allocation and inflates "Other" in both the time-series and allocation pie marts. wstETH should be added to the ETH branch; explicit bucket decisions for SAFE and COW should be documented.

Affected models: `models/execution/dao_treasury/marts/api_dao_treasury_allocation_latest.sql`, `models/execution/dao_treasury/marts/api_dao_treasury_holdings_by_class_ts.sql`.

### Medium

**All marts tagged `dev` — CI api-tag guard bypassed**

`check_api_tags.py` enforces tag conventions only on `production`-tagged models. All seven marts carry `dev`, so missing `window:7d` tags on the four 7-day-change KPI models and missing `data_type` on all mart schema columns are not caught by CI. These must be fixed before the `dev` -> `production` promotion or the guard will fail.

Affected models: all four KPI mart SQL files and `models/execution/dao_treasury/marts/schema.yml`.

**605 rows (1.4%) have NULL balance_usd — silently dropped from all USD sums**

bCSPX (573 rows) and GBPe (32 rows) have NULL `balance_usd` across their missing-price spans. `sum(balance_usd)` excludes these rows without any warning, understating total holdings on affected days (bCSPX peak ~$1.4M). Either backfill a price feed for these tokens, or surface an "unpriced positions" count so API consumers can see when the total is incomplete.

Affected model: `models/execution/dao_treasury/intermediate/int_dao_treasury_holdings_daily.sql`.

### Low

**Output alias `token_class` shares the source column name (latent shadow risk)**

Both `api_dao_treasury_allocation_latest` and `api_dao_treasury_holdings_by_class_ts` name the CASE output `token_class`, the same as the source column referenced in the WHEN branches. Project memory explicitly flags this pattern as a known ClickHouse pitfall where aliases can shadow source columns in same-level CASE conditions. Verified against the live warehouse: classification is currently correct (bCSPX/bIB01/bIBTA -> RWA, stables -> Stablecoins). Downgraded from the inspector's initial "high" severity on the basis of this verification. Renaming the output to `asset_class` removes the latent risk and eliminates a GROUP-BY-on-alias ambiguity.

**change_pct undocumented NULL behaviour**

If 7-days-prior data is missing or the prior value is exactly zero, `nullIf` yields NULL and `change_pct` is NULL rather than an explicit sentinel. This is technically correct but `schema.yml` documents the column only as "Percentage change vs 7 days ago" with no mention of the NULL case, which can surprise API consumers on new deployments or data gaps.

Affected models: all four KPI mart SQL files.

**`by_class_ts` HAVING `value > 0` creates gaps on fully-unpriced dates**

`sum(balance_usd)` over all-NULL rows returns NULL, which fails `HAVING value > 0`, so a class on a fully-unpriced date vanishes from the time series rather than showing zero. This affects RWA/stablecoin continuity on days where the price feed is completely absent for a given class.

Affected model: `models/execution/dao_treasury/marts/api_dao_treasury_holdings_by_class_ts.sql`.

---

## Business-logic assessment

### High

**wstETH classification gap (also listed under implementation — dual impact)**

Covered above. Beyond the implementation defect, this is a business-logic accuracy issue: a substantial ETH-correlated position (peak ~$2.8M, current $719K) is reported as "Other" in governance-facing dashboards. The fix requires a business decision on whether wstETH belongs in the ETH bucket or a separate "ETH Derivatives" bucket, and whether SAFE and COW tokens warrant named buckets.

### Medium

**GNO-held KPI sums LSD derivatives as 1:1 GNO units**

`api_dao_treasury_kpi_gno_held` sums native `balance` for GNO, sGNO, spGNO, and aGnoGNO. sGNO and spGNO are liquid-staking derivatives whose exchange rate to GNO drifts from 1:1 over time (though spGNO is absent in current data). Summing native units across all four symbols produces an economically imprecise "GNO held" figure. Either compute a GNO-equivalent via `balance_usd / GNO_price`, or caveat the metric explicitly as "GNO + derivatives, native units (not exchange-rate-adjusted)" in `schema.yml`.

Affected model: `models/execution/dao_treasury/marts/api_dao_treasury_kpi_gno_held.sql`.

**No semantic-layer coverage for any mart**

None of the seven `api_*` marts have `semantic_models.yml` entries. The existing semantic registry covers only revenue-related models. MCP `query_metrics` cannot serve any treasury endpoint, and there are no governed metric definitions behind the KPIs. Semantic coverage should be planned as part of the production-promotion checklist.

Affected: `models/execution/dao_treasury/marts/schema.yml` and all mart SQL files.

### Low

**Treasury wallet ownership not cross-verified against any on-chain registry**

The six Safe addresses are assigned solely in `dao_treasury_wallets.csv` with team-assigned labels. None are cross-referenced in `contracts_whitelist.csv` or an on-chain Safe registry. For external/governance reporting, confirming each address via signer-set/threshold verification makes the in-scope boundary auditable.

**Yield-bearing stablecoin treatment is a definitional choice worth stating**

sDAI and aGnosDAI are included in the non-GNO KPI and the Stablecoins asset class. This is a defensible NAV treatment but is implicit; `schema.yml` should state it explicitly so external consumers understand that yield-bearing stablecoin positions are bucketed as stablecoins rather than a separate "yield" or "protocol-deployed" class.

---

## Data findings

Queries run: 8 against `int_dao_treasury_holdings_daily`.

| Query | Result |
|---|---|
| Freshness + row count | max_date = 2026-06-09, min_date = 2021-09-05 (epoch 18870), 44,192 rows, 1,744 distinct dates |
| Today vs max_date | max_date is 2 days behind today (2026-06-11) |
| max_date diagnostic | 2026-06-09: 13 lending rows, sum(balance_usd) = $0, 0 wallet rows |
| Previous complete date | 2026-06-07: 60 rows, ~$118M total balance_usd across OTHERS/STABLECOIN/LENDING/RWA |
| NULL balance_usd | 605 rows (1.4%): bCSPX 573, GBPe 32 |
| Grain uniqueness | 0 duplicate combinations on the declared order_by key |
| Symbol / token_class audit | sGNO present; spGNO, aGnoGNO absent in current data; GNO appears in LENDING and OTHERS |
| "Other" class audit | wstETH ~$719K (peak ~$2.8M), SAFE ~$1.4M, COW ~$707K on 2026-06-07 |

---

## Pros / Cons

**Pros**

- Clear, well-scoped business purpose with a coherent KPI + allocation + detail mart set covering the full "what does GnosisDAO hold" question.
- Wallet universe is explicitly seed-bounded (six labelled Safes in `dao_treasury_wallets.csv`), giving an auditable in-scope definition.
- Intermediate grain is clean: 0 duplicate rows confirmed over 44,192 rows; insert_overwrite strategy correctly enforces the order_by key.
- Unifies wallet ERC-20 balances and Aave V3 / SparkLend supply positions into one daily holdings fact, enabling consistent total / lending / non-GNO cuts.
- Currently tagged `dev` and dashboard sector disabled, so the broken $0 numbers are not yet exposed to production API or MCP consumers.
- Asset-class CASE logic, when exercised against the live warehouse, classifies RWA and stablecoin tokens correctly; the taxonomy core is sound apart from the named wstETH and SAFE/COW gaps.
- Known limitations (Gnosis-Chain-only scope, nullable `balance_usd`, sub-threshold filtering) are documented in `schema.yml` descriptions.

**Cons**

- No guard against partial upstream refresh: `max(date)` resolves to a lending-only / $0 day and all six marts serve $0 and ~-100% change with no sentinel.
- No dbt tests anywhere; upstream dedup or freshness regressions are completely invisible.
- Asset-class taxonomy duplicated verbatim across two marts with no shared macro or intermediate column, creating drift risk.
- wstETH (ETH derivative, up to ~$2.8M peak; $719K current) falls to "Other", understating ETH and inflating "Other" in governance-facing allocation views.
- GNO-held KPI sums native units of LSD derivatives as 1:1 with GNO, which is economically imprecise.
- NULL `balance_usd` for bCSPX/GBPe (605 rows) silently understates totals on affected days with no indicator.
- All marts tagged `dev`, bypassing the CI api-tag guard; missing `window:7d` tags and `data_type` columns on mart schema will not be enforced until promotion.
- No semantic-layer coverage for any of the seven `api_*` marts.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Re-anchor all six mart `max(date)` subqueries to the latest date where wallet rows exist (or where both position_types are present), to stop serving $0 KPIs on partial-refresh days. Add a freshness or minimum-row-count alert on `int_dao_treasury_holdings_daily`. | All 6 mart SQL files + intermediate |
| P1 | Add dbt tests before any production promotion: `dbt_utils.unique_combination_of_columns` on `(date, wallet_address, position_type, protocol, symbol)`, `accepted_values` on `position_type`, and a freshness/recency test. | Both `schema.yml` files |
| P1 | Add wstETH to the ETH asset-class branch (wallet and lending) in both asset-class marts. Decide and document explicit handling for SAFE and COW (dedicated buckets or labelled "Other"). | `api_dao_treasury_allocation_latest.sql`, `api_dao_treasury_holdings_by_class_ts.sql` |
| P2 | Extract the asset-class CASE into a shared macro or an `asset_class` column on the intermediate model so the two mart files cannot drift. | `api_dao_treasury_allocation_latest.sql`, `api_dao_treasury_holdings_by_class_ts.sql`, `int_dao_treasury_holdings_daily.sql` |
| P2 | Add `window:7d` tags to the four 7-day-change KPI models and `data_type` to all mart schema columns before promoting `dev` -> `production`, so the CI api-tag guard passes. | All 4 KPI mart SQL files, `marts/schema.yml` |
| P2 | Document or resolve the NULL `balance_usd` gap for bCSPX (573 days) and GBPe (32 days); consider surfacing an "unpriced positions" count in the KPI marts so totals are visibly incomplete rather than silently understated. | `int_dao_treasury_holdings_daily.sql`, mart `schema.yml` |
| P3 | Rename the CASE output alias from `token_class` to `asset_class` in both asset-class marts to remove the latent source-column shadow and GROUP-BY-on-alias ambiguity. | `api_dao_treasury_allocation_latest.sql`, `api_dao_treasury_holdings_by_class_ts.sql` |
| P3 | Caveat the GNO-held KPI as "native-unit (not exchange-rate-adjusted) sum of GNO + derivatives" in `schema.yml`, or switch to a GNO-equivalent calculation via `balance_usd / GNO_price`. | `api_dao_treasury_kpi_gno_held.sql`, `marts/schema.yml` |
| P3 | Plan semantic-layer coverage for the seven marts as part of the production-promotion checklist so MCP `query_metrics` and governed metric definitions exist. | All 7 mart SQL files |
| P4 | Cross-verify the six treasury Safe addresses against an on-chain Safe registry and document the signer set / threshold for each so the in-scope boundary is auditable for external/governance reporting. | `dao_treasury_wallets.csv` |

---

## Open disagreements

None — the review converged in one round. The only inter-agent discrepancy was the alias-shadowing severity (inspector rated high; analyst downgraded to low after live warehouse verification confirmed correct classification). No unresolved contradictions remain.

---

## Review log

| Round | Agent | Challenge / action | Outcome |
|---|---|---|---|
| 1 | Analyst | Verified critical claim: queried `int_dao_treasury_holdings_daily` for max(date) row composition | Confirmed: 2026-06-09 has 13 lending rows at $0, 0 wallet rows — critical finding upheld |
| 1 | Analyst | Tested alias-shadowing claim from inspector by running CASE expression with production aliasing against live warehouse | Finding downgraded from high to low — classification is currently correct; latent risk documented |
| 1 | Analyst | Verified wstETH / SAFE / COW "Other" classification | Confirmed: wstETH $719K, SAFE $1.4M, COW $707K all in Other on 2026-06-07 |
| 1 | Analyst | Confirmed dev tag and no semantic coverage | Both confirmed; no production exposure currently |
