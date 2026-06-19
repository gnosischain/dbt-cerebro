# Model Review — Resume Checkpoint

Three-agent iterative model review (Model Inspector + Context Gatherer + Senior Analyst,
looping to convergence, max 3 rounds/unit). Paused mid-run to conserve credits.

## State at pause (2026-06-10)

- **Completed units:** 0 (Opus run had just started; nothing finished, nothing lost).
- **Reports on disk:** none yet in `docs/model_review/`.
- **Model config:** Inspector + Context Gatherer + Reporter on Sonnet (`model: 'sonnet'`
  overrides); Senior Analyst on Opus (inherits the session model). Cost-optimized split
  chosen 2026-06-11 — workers on Sonnet, judgment role on Opus.
- **Working tree:** untouched except this new `docs/` dir. Review is READ-ONLY on models + warehouse.

## Workflow script

Persisted at:
`/Users/hugser/.claude/projects/-Users-hugser-Documents-Gnosis-repos-dbt-cerebro/4c72adba-eb07-4875-82dd-7455214c16cd/workflows/scripts/three-agent-model-review-wf_32c179bf-4fc.js`

It already contains: the `args` JSON.parse guard, the per-unit 3-round convergence loop,
all four role prompts, and `model: 'opus'` overrides. Invoke with:
`Workflow({scriptPath: "<path above>", args: {today: "<date>", units: [ ... ]}})`

If that script path is gone (new machine / cleaned session dir), the full plan is at
`/Users/hugser/.claude/plans/deploy-an-agent-to-effervescent-hartmanis.md` — rebuild from there.

## Resume procedure (works across sessions)

1. `ls docs/model_review/*.md` → note which `<unit-key>.md` files already exist.
2. For each batch below, drop any unit whose `key` already has a report on disk.
3. Run remaining units: `Workflow({scriptPath, args: {today, units: <remaining>}})`.
4. Run batches sequentially (1 → 2 → 3); review headlines between batches.
5. After all units done: synthesis step — an agent reads every `docs/model_review/*.md`
   and writes `docs/model_review/REPORT.md` (severity-ranked findings + convergence table).

Per-unit cost note: dial the Reporter role back to `'sonnet'` (pure formatting) to cut
cost with no quality loss, if credits are tight.

---

## BATCH 1 — execution subsectors (23 units) — args.units

```json
[
  {"key": "execution-circles", "title": "execution/Circles", "desc": "Circles v1/v2 protocol analytics: avatars/humans/groups, trust graph, CRC token flows, group economies, cashback", "model_paths": ["models/execution/Circles"], "semantic_paths": ["semantic/authoring/execution/Circles"], "shards": [
    {"label": "intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/Circles/intermediate — all 47 .sql files"},
    {"label": "marts-1", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/Circles/marts — sort all 82 .sql filenames alphabetically and cover ONLY the FIRST half (rows 1-41)"},
    {"label": "marts-2", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/Circles/marts — sort all 82 .sql filenames alphabetically and cover ONLY the SECOND half (rows 42-82)"}]},
  {"key": "execution-gnosis_app", "title": "execution/gnosis_app", "desc": "Gnosis App product analytics: users, actions, funnels, retention, in-app activity (verify actual purpose from schema docs)", "model_paths": ["models/execution/gnosis_app"], "semantic_paths": ["semantic/authoring/execution/gnosis_app"], "shards": [
    {"label": "intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/gnosis_app/intermediate — all 25 .sql files"},
    {"label": "marts-1", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/gnosis_app/marts — sort all 102 .sql filenames alphabetically and cover ONLY the FIRST half (rows 1-51)"},
    {"label": "marts-2", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/gnosis_app/marts — sort all 102 .sql filenames alphabetically and cover ONLY the SECOND half (rows 52-102)"}]},
  {"key": "execution-gpay", "title": "execution/gpay", "desc": "Gnosis Pay analytics: card payments, wallets, spending, rewards/cashback, user activity", "model_paths": ["models/execution/gpay"], "semantic_paths": ["semantic/authoring/execution/gpay"], "shards": [
    {"label": "intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/gpay/intermediate — all 19 .sql files"},
    {"label": "marts-1", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/gpay/marts — sort all 93 .sql filenames alphabetically and cover ONLY the FIRST half (rows 1-46)"},
    {"label": "marts-2", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/gpay/marts — sort all 93 .sql filenames alphabetically and cover ONLY the SECOND half (rows 47-93)"}]},
  {"key": "execution-pools", "title": "execution/pools", "desc": "DEX liquidity pool analytics across AMMs: TVL, volumes, fees, LP positions", "model_paths": ["models/execution/pools"], "semantic_paths": ["semantic/authoring/execution/pools"], "shards": [
    {"label": "staging-intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/pools/staging and /Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/pools/intermediate — all 32 .sql files"},
    {"label": "marts", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/execution/pools/marts — all 31 .sql files"}]},
  {"key": "execution-transactions", "title": "execution/transactions", "desc": "Transaction-level chain analytics: counts, gas, success rates, contract interactions", "model_paths": ["models/execution/transactions"], "semantic_paths": ["semantic/authoring/execution/transactions"]},
  {"key": "execution-tokens", "title": "execution/tokens", "desc": "Token transfer/holder analytics: transfers, balances, supply", "model_paths": ["models/execution/tokens"], "semantic_paths": ["semantic/authoring/execution/tokens"]},
  {"key": "execution-accounts", "title": "execution/accounts", "desc": "Account activity: active addresses, new accounts, EOA/contract breakdowns", "model_paths": ["models/execution/accounts"], "semantic_paths": []},
  {"key": "execution-yields", "title": "execution/yields", "desc": "Yield/vault metrics across protocols", "model_paths": ["models/execution/yields"], "semantic_paths": ["semantic/authoring/execution/yields"]},
  {"key": "execution-cow", "title": "execution/cow", "desc": "CoW Protocol on Gnosis: trades, solvers, volumes", "model_paths": ["models/execution/cow"], "semantic_paths": ["semantic/authoring/execution/cow"]},
  {"key": "execution-lending", "title": "execution/lending", "desc": "Lending protocols (Aave/Agave/Spark): deposits, borrows, liquidations, reserves", "model_paths": ["models/execution/lending"], "semantic_paths": ["semantic/authoring/execution/lending"]},
  {"key": "execution-ubo", "title": "execution/ubo", "desc": "UBO models (verify actual purpose from schema docs)", "model_paths": ["models/execution/ubo"], "semantic_paths": []},
  {"key": "execution-safe", "title": "execution/safe", "desc": "Safe smart accounts: deployments, transactions, modules", "model_paths": ["models/execution/safe"], "semantic_paths": ["semantic/authoring/execution/safe"]},
  {"key": "execution-live", "title": "execution/live", "desc": "Live/low-latency streaming marts (45s refresh loop, feat/live-trades work)", "model_paths": ["models/execution/live"], "semantic_paths": []},
  {"key": "execution-blocks", "title": "execution/blocks", "desc": "Block-level metrics: production, gas, base fee", "model_paths": ["models/execution/blocks"], "semantic_paths": ["semantic/authoring/execution/blocks"]},
  {"key": "execution-dao_treasury", "title": "execution/dao_treasury", "desc": "GnosisDAO treasury tracking: balances, flows", "model_paths": ["models/execution/dao_treasury"], "semantic_paths": []},
  {"key": "execution-mmm", "title": "execution/mmm", "desc": "Marketing mix modeling inputs/outputs", "model_paths": ["models/execution/mmm"], "semantic_paths": []},
  {"key": "execution-state", "title": "execution/state", "desc": "Chain state snapshots (verify actual purpose from schema docs)", "model_paths": ["models/execution/state"], "semantic_paths": ["semantic/authoring/execution/state"]},
  {"key": "execution-prices", "title": "execution/prices", "desc": "Native token price feeds (Dune-sourced today; native Chainlink decode planned per docs/native_token_prices_build_plan.md)", "model_paths": ["models/execution/prices"], "semantic_paths": ["semantic/authoring/execution/prices"]},
  {"key": "execution-zodiac", "title": "execution/zodiac", "desc": "Zodiac module interactions on Safes", "model_paths": ["models/execution/zodiac"], "semantic_paths": []},
  {"key": "execution-transfers", "title": "execution/transfers", "desc": "Low-level transfer event models", "model_paths": ["models/execution/transfers"], "semantic_paths": ["semantic/authoring/execution/transfers"]},
  {"key": "execution-rwa", "title": "execution/rwa", "desc": "Real-world asset tokens on Gnosis", "model_paths": ["models/execution/rwa"], "semantic_paths": ["semantic/authoring/execution/rwa"]},
  {"key": "execution-gbcdeposit", "title": "execution/GBCDeposit", "desc": "Gnosis Beacon Chain deposit contract tracking", "model_paths": ["models/execution/GBCDeposit"], "semantic_paths": ["semantic/authoring/execution/GBCDeposit"]},
  {"key": "execution-shared", "title": "execution/shared", "desc": "Shared execution-layer utility mart", "model_paths": ["models/execution/shared"], "semantic_paths": ["semantic/authoring/execution/shared"]}
]
```

## BATCH 2 — top-level sectors (10 units) — args.units

```json
[
  {"key": "consensus", "title": "consensus", "desc": "Beacon-chain validator analytics: APY, deposits, withdrawals, attestations, block proposals, validator explorer", "model_paths": ["models/consensus"], "semantic_paths": ["semantic/authoring/consensus"], "shards": [
    {"label": "staging-intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/consensus/staging and /Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/consensus/intermediate — all .sql files (recursive)"},
    {"label": "marts-1", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/consensus/marts — glob all .sql, sort filenames alphabetically, cover ONLY the FIRST half"},
    {"label": "marts-2", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/consensus/marts — glob all .sql, sort filenames alphabetically, cover ONLY the SECOND half"}]},
  {"key": "revenue", "title": "revenue", "desc": "Economic/revenue modeling for Gnosis Chain (fees, MEV, protocol revenue). See docs/economic_concepts.md", "model_paths": ["models/revenue"], "semantic_paths": ["semantic/authoring/revenue"], "shards": [
    {"label": "intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/revenue/intermediate — all .sql files (recursive)"},
    {"label": "marts-1", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/revenue/marts — glob all .sql, sort filenames alphabetically, cover ONLY the FIRST half"},
    {"label": "marts-2", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/revenue/marts — glob all .sql, sort filenames alphabetically, cover ONLY the SECOND half"}]},
  {"key": "mixpanel_ga", "title": "mixpanel_ga", "desc": "App analytics from Mixpanel/Google Analytics; PRIVACY-TIERED (privacy:mixpanel_ga, excluded from API/MCP). Many warehouse queries will be blocked — note and continue code-only.", "model_paths": ["models/mixpanel_ga"], "semantic_paths": ["semantic/authoring/mixpanel_ga"], "shards": [
    {"label": "staging-intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/mixpanel_ga/staging and /Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/mixpanel_ga/intermediate — all .sql files (recursive)"},
    {"label": "marts", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/mixpanel_ga/marts — all .sql files"}]},
  {"key": "p2p", "title": "p2p", "desc": "P2P network analytics: peer topology, client distribution, crawl results", "model_paths": ["models/p2p"], "semantic_paths": ["semantic/authoring/p2p"], "shards": [
    {"label": "staging-intermediate", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/p2p/staging and /Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/p2p/intermediate — all .sql files (recursive)"},
    {"label": "marts", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/p2p/marts — all .sql files"}]},
  {"key": "bridges", "title": "bridges", "desc": "Cross-chain bridge analytics: inflow/outflow by token, netflow, Sankey, KPIs", "model_paths": ["models/bridges"], "semantic_paths": ["semantic/authoring/bridges"]},
  {"key": "ESG", "title": "ESG", "desc": "Environmental metrics: power/energy consumption, carbon emissions, node-class sustainability", "model_paths": ["models/ESG"], "semantic_paths": ["semantic/authoring/ESG"]},
  {"key": "crawlers_data", "title": "crawlers_data", "desc": "External crawler/Dune-sourced datasets: labels, prices, project/sector totals, GNO supply", "model_paths": ["models/crawlers_data"], "semantic_paths": ["semantic/authoring/crawlers_data"]},
  {"key": "probelab", "title": "probelab", "desc": "ProbeLab network measurements: client versions, cloud distribution, QUIC support", "model_paths": ["models/probelab"], "semantic_paths": ["semantic/authoring/probelab"]},
  {"key": "quarterly_data", "title": "quarterly_data", "desc": "Quarterly reporting snapshots across 6 subsectors: circles, esg, gnosis_app, gnosis_chain, gnosis_pay, stablecoins. Cover all 6 subdirs.", "model_paths": ["models/quarterly_data"], "semantic_paths": []},
  {"key": "shared", "title": "shared", "desc": "Cross-sector shared utility marts (e.g. date spine, address dims)", "model_paths": ["models/shared"], "semantic_paths": ["semantic/authoring/shared"]}
]
```

## BATCH 3 — contracts protocol families (4 units) — args.units

```json
[
  {"key": "contracts-circles", "title": "contracts/Circles", "desc": "Decoded Circles v1/v2 contract events+calls (Hub, group registries, factories). Feeds execution/Circles. Verify addresses vs seeds/contracts_circles_registry_static.csv and dbt_project.yml circles vars.", "model_paths": ["models/contracts/Circles"], "semantic_paths": [], "shards": [
    {"label": "alpha-1", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/contracts/Circles — glob all 47 .sql, sort filenames alphabetically, cover ONLY the FIRST half"},
    {"label": "alpha-2", "scope": "/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/models/contracts/Circles — glob all 47 .sql, sort filenames alphabetically, cover ONLY the SECOND half"}]},
  {"key": "contracts-amm-dex", "title": "contracts/AMM-DEX", "desc": "Decoded AMM/DEX contract events+calls: BalancerV2, BalancerV3, Curve, Swapr, UniswapV3, CowProtocol. Verify pool/router addresses vs seeds.", "model_paths": ["models/contracts/BalancerV2", "models/contracts/BalancerV3", "models/contracts/Curve", "models/contracts/Swapr", "models/contracts/UniswapV3", "models/contracts/CowProtocol"], "semantic_paths": []},
  {"key": "contracts-lending-tokens", "title": "contracts/lending-tokens-oracles", "desc": "Decoded lending, token, and oracle contracts: aave, agave, spark, backedfi, chainlink, tokens, GBCDeposit. Verify vs lending_market_mapping.csv, atoken_reserve_mapping.csv, tokens_whitelist.csv.", "model_paths": ["models/contracts/aave", "models/contracts/agave", "models/contracts/spark", "models/contracts/backedfi", "models/contracts/chainlink", "models/contracts/tokens", "models/contracts/GBCDeposit"], "semantic_paths": []},
  {"key": "contracts-prediction-markets", "title": "contracts/prediction-markets", "desc": "Decoded prediction-market contracts: ConditionalTokens, FPMMDeterministicFactory, OmenAgentResultMapping, AgentResultMapping, Realitio_v2_1, SeerPM. Verify factory/oracle addresses vs seeds.", "model_paths": ["models/contracts/ConditionalTokens", "models/contracts/FPMMDeterministicFactory", "models/contracts/OmenAgentResultMapping", "models/contracts/AgentResultMapping", "models/contracts/Realitio_v2_1", "models/contracts/SeerPM"], "semantic_paths": []}
]
```
