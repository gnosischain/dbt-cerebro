# dbt Full Refresh Orchestrator

A lightweight wrapper that enables batched full refreshes of large dbt models while leveraging dbt's native features (meta, tags, selectors, manifest).

## Overview

This orchestrator solves the problem of refreshing large dbt models that cannot complete in a single execution due to:

- **Data volume**: Models spanning years of daily data
- **Memory constraints**: ClickHouse query memory limits  
- **Timeout issues**: Long-running queries getting terminated
- **Multi-dimensional batching**: Some models need batching by time AND by field (e.g., symbol)

## Quick Start

```bash
# Dry run to see execution plan
python scripts/full_refresh/refresh.py --select tag:production --dry-run

# Execute full refresh
python scripts/full_refresh/refresh.py --select tag:production

# Resume after failure
python scripts/full_refresh/refresh.py --select tag:production --resume
```

## How It Works

1. **Get Models**: Uses `dbt ls` to get models in dependency order
2. **Parse Manifest**: Reads `target/manifest.json` to extract `meta.full_refresh` config
3. **Execute**: For each model:
   - If it has `full_refresh` config: runs batched (by time and/or stages)
   - Otherwise: runs normally with `--full-refresh` flag
4. **Track Progress**: Saves state to `.refresh_state.json` for resume capability

## Configuration

Add `meta.full_refresh` to models in their `schema.yml` files:

### Pattern 1: Time Batching Only

For models that just need time-based chunking:

```yaml
models:
  - name: int_execution_transactions_by_project_daily
    description: "Daily transactions aggregated by project"
    meta:
      full_refresh:
        start_date: "2018-08-01"    # When data begins
        batch_months: 2              # Process 2 months at a time
    config:
      tags: ['production', 'execution', 'transactions']
```

### Pattern 2: Time + Multi-Stage (Field Filtering)

For models where certain field values create much larger datasets:

```yaml
models:
  - name: int_execution_tokens_balances_daily
    description: "Daily token balances per address"
    meta:
      full_refresh:
        start_date: "2020-07-01"   # Default for all stages
        batch_months: 1
        stages:
          # Stage 1: All tokens EXCEPT the large ones
          - name: small_tokens
            vars:
              symbol_exclude: "WxDAI,GNO"
          
          # Stage 2: WxDAI alone (very large)
          - name: wxdai
            vars:
              symbol: "WxDAI"
          
          # Stage 3: GNO alone (very large)
          - name: gno
            vars:
              symbol: "GNO"
    config:
      tags: ['production', 'execution', 'tokens']
```

### Pattern 2b: Multi-Stage with Different Start Dates

For models where different token classes have different chain history (newer tokens don't need to start from genesis):

```yaml
models:
  - name: int_execution_tokens_balances_daily
    description: "Daily token balances per address"
    meta:
      full_refresh:
        batch_months: 1            # No model-level start_date needed
        stages:
          # Legacy tokens from chain genesis
          - name: legacy_tokens
            start_date: "2020-07-01"
            vars:
              symbol_exclude: "USDC,sDAI,EURe"
          
          # USDC deployed later
          - name: usdc
            start_date: "2022-03-01"
            vars:
              symbol: "USDC"
          
          # sDAI deployed much later
          - name: sdai
            start_date: "2023-10-01"
            vars:
              symbol: "sDAI"
              
          # EURe deployed later
          - name: eure
            start_date: "2022-06-01"
            vars:
              symbol: "EURe"
    config:
      tags: ['production', 'execution', 'tokens']
```

This approach saves significant time by not running empty batches for tokens that didn't exist yet.

### Pattern 2c: Multi-Stage with Different Start Dates AND Batch Sizes

For maximum optimization - tokens with low activity can use larger batches:

```yaml
models:
  - name: int_execution_tokens_balances_daily
    description: "Daily token balances per address"
    meta:
      full_refresh:
        batch_months: 1            # Default for high-volume tokens
        stages:
          # Legacy tokens - high volume, need monthly batches
          - name: legacy_tokens
            start_date: "2020-07-01"
            batch_months: 1        # Monthly (default)
            vars:
              symbol_exclude: "USDC,sDAI,EURe,bC3M"
          
          # USDC - high volume
          - name: usdc
            start_date: "2022-03-01"
            batch_months: 1
            vars:
              symbol: "USDC"
          
          # sDAI - medium volume, can do 2 months
          - name: sdai
            start_date: "2023-10-01"
            batch_months: 2
            vars:
              symbol: "sDAI"
              
          # EURe - lower volume, can do 3 months
          - name: eure
            start_date: "2022-06-01"
            batch_months: 3
            vars:
              symbol: "EURe"
              
          # bC3M - very low volume RWA token, can do 6 months
          - name: bc3m
            start_date: "2023-06-01"
            batch_months: 6
            vars:
              symbol: "bC3M"
    config:
      tags: ['production', 'execution', 'tokens']
```

This optimizes both by skipping non-existent periods AND reducing batch count for low-volume tokens.

### Pattern 3: Time + Numeric Range Batching

For models with numeric range filtering (e.g., validator indices):

```yaml
models:
  - name: int_consensus_validators_per_index_apy_daily
    description: "Daily APY per validator index"
    meta:
      full_refresh:
        start_date: "2021-12-01"
        batch_months: 1
        stages:
          - name: validators_0_100k
            vars:
              validator_index_start: 0
              validator_index_end: 100000
          - name: validators_100k_200k
            vars:
              validator_index_start: 100000
              validator_index_end: 200000
    config:
      tags: ['production', 'consensus']
```

### Configuration Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start_date` | string (YYYY-MM-DD) | No* | - | Default start date for all stages |
| `batch_months` | integer | No | 1 | Default months per batch for all stages |
| `stages` | array | No | null | List of stage configurations |
| `stages[].name` | string | Yes | - | Human-readable stage identifier |
| `stages[].start_date` | string (YYYY-MM-DD) | No* | Model's start_date | Per-stage start date |
| `stages[].batch_months` | integer | No | Model's batch_months | Per-stage batch size in months |
| `stages[].vars` | object | Yes | - | Variables to pass to dbt |

*Either model-level `start_date` OR per-stage `start_date` is required for each stage.

## Usage Examples

### Basic Commands

```bash
# Full refresh single model
python scripts/full_refresh/refresh.py --select int_execution_tokens_balances_daily

# Full refresh by tag
python scripts/full_refresh/refresh.py --select tag:production

# Dry run (preview execution plan)
python scripts/full_refresh/refresh.py --select tag:production --dry-run

# Resume after failure
python scripts/full_refresh/refresh.py --select tag:production --resume

# Multiple selectors (use dbt union syntax)
python scripts/full_refresh/refresh.py --select "tag:tokens tag:transactions"
```

### Adding a New Token (Incremental Update)

When you add a new token to the whitelist and want to backfill its data without destroying existing data:

```bash
# 1. First, add the new stage to your schema.yml:
#    - name: new_token
#      start_date: "2024-06-01"
#      batch_months: 3
#      vars:
#        symbol: "NEW_TOKEN"

# 2. Run ONLY the new stage, without --full-refresh (append only)
python scripts/full_refresh/refresh.py \
    --select int_execution_tokens_balances_daily \
    --stage new_token \
    --incremental-only

# Preview what would run:
python scripts/full_refresh/refresh.py \
    --select int_execution_tokens_balances_daily \
    --stage new_token \
    --incremental-only \
    --dry-run
```

Output:
```
============================================================
Model: int_execution_tokens_balances_daily
  Mode: INCREMENTAL (append only)
  Stage filter: ['new_token'] (1/6 stages)
  Stages: 1
  Total runs: 8
============================================================

  Stage: new_token (8 batches, 2024-06-01 → now, 3mo batches)
    [1/8] 2024-06-01 → 2024-08-01 | {'symbol': 'NEW_TOKEN'}
    [2/8] 2024-09-01 → 2024-11-01 | {'symbol': 'NEW_TOKEN'}
    ...
```

### Running Multiple Specific Stages

```bash
# Run only usdc and sdai stages (e.g., after fixing price data)
python scripts/full_refresh/refresh.py \
    --select int_execution_tokens_balances_daily \
    --stage usdc,sdai \
    --incremental-only
```

### Understanding Dry Run Output

```
Getting models for: tag:tokens
Found 8 models: ['int_execution_transfers_whitelisted_daily', ...]

============================================================
DRY RUN - Execution Plan
============================================================

  int_execution_transfers_whitelisted_daily (standard run) --full-refresh

============================================================
Model: int_execution_tokens_address_diffs_daily
  Time batches: 55 (2020-07-01 → now)
  Stages: 3
  Total runs: 165
============================================================

  Stage: small_tokens
    [1/165] 2020-07-01 → 2020-07-01 | {'symbol_exclude': 'WxDAI,GNO'} --full-refresh
    [2/165] 2020-08-01 → 2020-08-01 | {'symbol_exclude': 'WxDAI,GNO'}
    ...
```

### Resuming After Failure

If execution fails midway, the state is automatically saved. Resume with:

```bash
python scripts/full_refresh/refresh.py --select tag:tokens --resume
```

The script will skip completed models and batches, resuming from where it left off.

## Model Requirements

Models must support the vars being passed. Ensure your models use the vars:

```sql
-- Example: int_execution_tokens_balances_daily.sql
{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
{% set symbol = var('symbol', none) %}
{% set symbol_exclude = var('symbol_exclude', none) %}

-- Use vars in WHERE clause
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% endif %}
  {% if symbol is not none %}
    AND symbol = '{{ symbol }}'
  {% endif %}
  {% if symbol_exclude is not none %}
    AND symbol NOT IN (
      {% for s in symbol_exclude.split(',') %}
        '{{ s }}'{% if not loop.last %}, {% endif %}
      {% endfor %}
    )
  {% endif %}
```

## State File

Progress is tracked in `.refresh_state.json`:

```json
{
  "completed_models": [
    "int_execution_transfers_whitelisted_daily",
    "int_execution_tokens_address_diffs_daily"
  ],
  "current_model": "int_execution_tokens_balances_daily",
  "current_batch": 47
}
```

This file is automatically:
- Created during execution
- Updated after each successful batch
- Deleted on successful completion
- Should be added to `.gitignore`

## Troubleshooting

### Error: "Manifest not found"

```bash
# Solution: Compile dbt project first
dbt compile
```

### Error: "No models found for selector"

```bash
# Check selector syntax
dbt ls --select tag:your_tag --resource-type model

# Valid selectors:
# - model_name
# - tag:tag_name
# - path:models/execution
# - +model_name (with upstream)
# - model_name+ (with downstream)
```

### Model fails during batch execution

1. Check the specific error in dbt output
2. Fix the underlying issue
3. Resume: `python refresh.py --select <selector> --resume`
