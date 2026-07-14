# Entity hubs — backlog & findings

Scoping done 2026-07-13 while investigating the cerebro-mcp benchmark's
`pct_models_with_entities` coverage metric (44.4%). Two conclusions:

## 1. The coverage % is near its practical ceiling — don't chase it

`generate_entities.py` annotates a model only when one of its columns is a
verbatim entity key from `entity_dictionary.yml`. Of the ~722 unannotated
models:

- **~591 (82%) have no bindable key at all** — they are aggregate / KPI / rollup
  models (`*_new_accounts`, `*_cumulative_accounts`, `*_validator_count`, scalar
  KPIs like `api_bridges_kpi_netflow_7d`). They aggregated their address/token/
  etc. keys away, so there is nothing to bind. These are legitimately
  unannotatable.
- Only ~131 (18%) have a plausible key not yet in the dictionary, and most of
  *those* are false positives (count columns). The genuine dictionary-alias
  gap is a handful of keys (`token_id`, `token_class`, `peer_id`, `avatar_type`,
  `wallet_label`) plus the 26 `suggest_hand_edit` hand-authored models.

Net: annotating everything realistically addressable lifts coverage only to
~50%. `pct_models_with_entities` is partly a measure of model architecture, not
dictionary completeness — it is not a high-value target.

## 2. The real value is graph EDGES, via the 13 NULL hubs

Today only **16 approved relationship edges** exist (283 dormant candidates).
Thirteen entities have `hub_model: null`, so they produce annotations but **zero
edges** — and they cover the most models:

| entity | models | entity | models |
|---|---|---|---|
| `transaction` | 164 | `contract` | 83 |
| `token_symbol` | 137 | `protocol` | 65 |
| `token` | 93 | `client` | 18 |
| `circles_group` | 14 | `sector` | 14 |
| `project` | 14 | `bridge` | 6 |
| `cycle` | 5 | `gateway` | 4 |
| `safe_module` | 4 | | |

Filling a `hub_model` unlocks many-to-one edges from every model that binds the
entity → the hub (`spoke.<key> = hub.<key>`). But **the blocker is that no
canonical dimension model exists** to point these hubs at.

### Why you can't shortcut it with an existing model (`token` worked example)

- No unified token dimension exists. `stg_pools__tokens_meta` has
  `token_address` + `symbol` but is **ERC20/pool tokens only** (no circles
  tokens), so circles-token spokes wouldn't resolve.
- It is also `docs_only` status. `generate_entities.py` marks an edge
  `approved` **only when both endpoints are approved** — a `docs_only` hub makes
  *every* edge `candidate`, which is runtime-invisible. So even the ERC20 edges
  would be dormant.

### What each hub actually needs

A single **approved** dimension model, one row per entity key. For `token` /
`token_symbol` that means a new `dim_tokens` that unions:

- **ERC20 arm:** `tokens_whitelist` seed / `stg_pools__tokens_meta`
  → `token_address`, `symbol`, `decimals`
- **Circles arm:** circles personal / group / wrapped tokens (keyed by `avatar`;
  the `avatar ↔ token_address ↔ symbol` mapping is spread across
  `api_execution_circles_v2_avatar_balances_latest` (has `token_address` +
  `avatar`) and `crc20_prices_daily` (has `symbol`))
- one row per `token_address`, tagged `semantic_status: approved`.

The other null-hub entities (`contract`, `protocol`, `transaction`, `sector`,
…) likewise need a dimension model authored before their hub is meaningful.

### Once a dimension exists, the dictionary edit is trivial

```yaml
- entity: token
  hub_model: dim_tokens        # <- add this line
  columns: [token_address, token_bought_address, ...]
- entity: token_symbol
  hub_model: dim_tokens        # <- add this line
  columns: [symbol, token_symbol, token]
```

Then re-run `python scripts/semantic/generate_entities.py` (writes the two
`*_generated.yml`), rebuild the registry, and the ~93 + 137 token/token_symbol
spokes gain live approved edges. Verify with the report's
`relationships: N (M approved, ...)` line.

## Recommended order

1. `dim_tokens` (ERC20 ∪ circles, approved) → hub for `token` + `token_symbol`
   (biggest, and both sources already exist). Highest ROI.
2. Case-by-case dimension models for the other null hubs where a natural
   dimension exists; skip the ones that don't (e.g. `transaction` is rarely a
   useful enrichment hub).
3. Do **not** invest further in raising `pct_models_with_entities` — it is near
   ceiling.
