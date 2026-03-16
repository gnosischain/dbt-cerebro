# Generate dbt schema.yml

Generate or update schema.yml entries for dbt models.

## Usage

```
/project:generate-schema <target>
```

Target is a folder, model name(s), or omit to use current context.

## Workflow

1. Find all .sql models in scope (skip `_*.sql` and `*_tmp.sql`).
2. For each model, parse the **final SELECT columns** (outermost SELECT only;
   trace `SELECT *` back to its CTE; respect aliases like `symbol AS token`).
3. Compare against the directory's schema.yml:
   - **Missing**: no entry → generate
   - **Stale**: columns in schema don't match SQL → regenerate
   - **Up to date**: skip (unless user asks to regenerate)
4. Print a summary of what needs work.
5. For each model that needs generation:
   a. Read `ref()` models' schema.yml entries for upstream context
   b. Read `source()` tables from `*_sources.yml` files
   c. Search for downstream models that `ref()` this model
   d. Generate the entry following conventions below
6. Merge into the directory's schema.yml — preserve untouched model entries
   and all existing `meta:` blocks (especially `full_refresh`).

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
    owner: analytics-team
    authoritative: false
    generated_by: dbt-schema-gen
    _generated_at: '<ISO timestamp>'
    _generated_fields:
    - description
    - columns.<col>.description
    - columns.<col>.data_type
```

## Rules

- **Columns**: only from the final SELECT. Every column gets `name`, `description`, `data_type`.
- **Types**: ClickHouse — `Date`, `DateTime64(0, 'UTC')`, `String`, `Float64`, `UInt64`,
  `Int256`, `Nullable(String)`, `AggregateFunction(groupBitmap, UInt64)`, etc.
- **Meta**: always `owner: analytics-team`, `authoritative: false`. Include `generated_by`,
  `_generated_at`, `_generated_fields` (list only fields you generated, not preserved ones).
- **No tests**: do not generate `tests:` blocks.
- **No full_refresh**: do not generate — only preserve existing or add when user explicitly asks.
- **Preserve**: keep untouched models' entries and all manually written meta intact.

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
    owner: analytics-team
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
    owner: analytics-team
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
    owner: analytics-team
    authoritative: false
```
