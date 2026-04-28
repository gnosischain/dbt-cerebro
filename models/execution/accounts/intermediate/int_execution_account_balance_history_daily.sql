{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    partition_by='toStartOfMonth(date)',
    order_by='(address, date)',
    unique_key='(address, date)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=[
      "SET max_threads = 1",
      "SET max_block_size = 8192",
      "SET max_memory_usage = 10000000000",
      "SET max_bytes_before_external_group_by = 100000000",
      "SET max_bytes_before_external_sort = 100000000",
      "SET group_by_two_level_threshold = 10000",
      "SET group_by_two_level_threshold_bytes = 10000000"
    ],
    post_hook=[
      "SET max_threads = 0",
      "SET max_block_size = 65505",
      "SET max_memory_usage = 0",
      "SET max_bytes_before_external_group_by = 0",
      "SET max_bytes_before_external_sort = 0"
    ],
    tags=['production', 'execution', 'accounts', 'portfolio', 'balances', 'granularity:daily', 'intermediate', 'refill_append']
  )
}}

-- Heavy address × token × date aggregate, checkpointed into its own
-- monthly-partitioned int_ table so the parent fct_ stays a thin pass-through
-- and full-refresh runs in monthly chunks instead of materializing the entire
-- aggregating transform in RAM.

WITH balances AS (
  SELECT
    lower(address) AS address,
    date,
    symbol,
    balance,
    ifNull(balance_usd, 0) AS balance_usd
  FROM {{ ref('int_execution_tokens_balances_daily') }}
  WHERE address IS NOT NULL
    AND address != ''
    AND date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
    {% endif %}
)

SELECT
  address,
  date,
  sum(balance_usd) AS total_balance_usd,
  countIf(balance > 0) AS tokens_held,
  maxIf(balance, upper(symbol) IN ('XDAI', 'WXDAI')) AS native_or_wrapped_xdai_balance,
  sumIf(balance_usd, balance_usd > 0) AS priced_balance_usd,
  countIf(balance > 0 AND balance_usd > 0) AS priced_tokens_held
FROM balances
GROUP BY address, date
