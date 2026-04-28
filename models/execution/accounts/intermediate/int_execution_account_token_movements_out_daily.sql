{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    partition_by='toStartOfMonth(date)',
    order_by='(address, date, counterparty, token_address)',
    unique_key='(date, address, counterparty, token_address)',
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
    tags=['production', 'execution', 'accounts', 'portfolio', 'movements', 'granularity:daily', 'intermediate']
  )
}}

-- Outbound leg of token movements (from_address = address, to_address = counterparty).
-- Split out from the daily fct so the aggregating transform doesn't have both
-- legs of a UNION-then-GROUP plan resident at the same time.

SELECT
  date,
  lower(token_address) AS token_address,
  symbol,
  lower("from") AS address,
  lower("to") AS counterparty,
  'out' AS direction,
  -sum(amount_raw) AS net_amount_raw,
  sum(abs(amount_raw)) AS gross_amount_raw,
  sum(transfer_count) AS transfer_count
FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
WHERE date < today()
  AND "from" IS NOT NULL
  AND "from" != ''
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
  {% endif %}
GROUP BY date, token_address, symbol, address, counterparty
