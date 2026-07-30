# Generate dbt schema.yml

Generate or update schema.yml entries for dbt models. Input: a folder, model
name(s), or the current working context.

## Workflow

1. Find all .sql models in scope (skip `_*.sql` and `*_tmp.sql`).
2. For each model, parse the **final SELECT columns** (outermost SELECT only;
   trace `SELECT *` back to its CTE; respect aliases like `symbol AS token`).
3. Compare against the directory's schema.yml:
   - **Missing**: no entry → generate
   - **Stale**: columns in schema don't match SQL → regenerate
   - **Up to date**: skip (unless asked to regenerate)
4. Print a summary of what needs work.
5. For each model that needs generation:
   a. Read `ref()` models' schema.yml entries for upstream context
   b. Read `source()` tables from `*_sources.yml` files
   c. Search for downstream models that `ref()` this model
   d. Generate the entry following conventions below
6. Merge into the directory's schema.yml — preserve untouched model entries
   and all existing `meta:` blocks (especially `full_refresh` and `agent` —
   `meta.agent` carries the model's engineering contract: grain, invariants,
   hazards, ground_truth, validation, reprocess_runbook; see
   `agent_context/profiles.yml` for the schema).

## Format

```yaml
version: 2
models:

- name: model_name
  description: 1-3 sentences — what it computes, key upstream refs, downstream purpose.
  columns:
  - name: column_name
    description: Business meaning in one sentence, capitalized.
    data_type: ClickHouseType
  meta:
    authoritative: false
```

## Rules

- **Columns**: only from the final SELECT. Every column gets `name`, `description`, `data_type`.
- **Types**: prefer the ACTUAL warehouse type from `target/catalog.json` when the model is
  catalogued there; fall back to inference from SQL casts. ClickHouse types — `Date`,
  `DateTime64(0, 'UTC')`, `String`, `Float64`, `UInt64`, `Int256`, `Nullable(String)`,
  `AggregateFunction(groupBitmap, UInt64)`, etc.
- **Never invent semantics**: a description may only assert what is provable from the model
  SQL, upstream schema.yml descriptions, macros, or seeds. If a column's business meaning is
  not provable, LEAVE ITS DESCRIPTION EMPTY and report the column under
  "uncertain — needs human input" at the end of the run. A wrong description poisons the
  semantic search corpus; an empty one is an honest, measurable gap.
- **Meta**: `authoritative: false` only. Do NOT emit generator bookkeeping
  (`generated_by`, `_generated_at`, `_generated_fields` — banned by
  `scripts/checks/check_meta_keys.py`; report generated fields in your run summary
  instead). Do NOT emit `owner` — module-tree defaults in dbt_project.yml supply it
  (`+meta: owner: analytics_team`); only preserve an existing model-level owner if
  one is already present.
- **No tests**: do not generate `tests:` blocks.
- **No full_refresh**: do not generate — only preserve existing or add when the user
  explicitly asks.
- **No agent contracts**: do not generate `meta.agent` — only preserve existing blocks
  verbatim. Contracts assert invariants; inventing one is worse than omitting it.
- **Preserve**: keep untouched models' entries and all manually written meta intact. Never
  overwrite a non-empty human-written description.

## Description style

- **stg_**: what source it normalizes, who depends on it
- **int_**: business logic (joins, aggregations, gap-filling), upstream inputs, downstream consumers
- **fct_**: business metric, grain, what it combines
- **api_**: dashboard use case and source model (keep short)

Column descriptions: business meaning, not SQL. Mention related models for join keys.
List known values for enum columns. Explain formulas only when non-obvious.

## Domain terms

TVL = Total Value Locked · APY = Annual Percentage Yield · Reserve = Aave V3 lending asset ·
Scaled balance = Aave balance before index multiplication · RAY = 1e27 ·
Bitmap state = ClickHouse AggregateFunction for distinct counting ·
Safe / Pay wallet = Gnosis Safe smart contract wallet

## Examples

**Staging**
```yaml
- name: stg_gpay__wallets
  description: Staging model that extracts distinct Gnosis Pay wallet addresses from the crawlers data labels, filtering for the gpay project.
  columns:
  - name: address
    description: The blockchain address of a Gnosis Pay wallet.
    data_type: String
  meta:
    authoritative: false
```

**Intermediate**
```yaml
- name: int_execution_gpay_balances_daily
  description: Incremental model that tracks daily token balances for each Gnosis Pay wallet address, with both native and USD values.
  columns:
  - name: date
    description: The calendar date of the balance snapshot.
    data_type: Date
  - name: address
    description: The blockchain address of the Gnosis Pay wallet.
    data_type: String
  - name: symbol
    description: The token symbol for the balance.
    data_type: String
  - name: balance
    description: The token balance in native units.
    data_type: Float64
  - name: balance_usd
    description: The token balance converted to USD.
    data_type: Float64
  meta:
    authoritative: false
```

**API view**
```yaml
- name: api_execution_gpay_total_balance
  description: API view exposing total Gnosis Pay balance from fct_execution_gpay_snapshots.
  columns:
  - name: value
    description: The total balance value across all Gnosis Pay wallets.
    data_type: Float64
  meta:
    authoritative: false
```
