{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    partition_by='toStartOfMonth(date)',
    order_by='(address, date, counterparty, token_address, direction)',
    unique_key='(date, address, counterparty, token_address, direction)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production', 'execution', 'accounts', 'portfolio', 'movements', 'granularity:daily']
  )
}}

-- Thin UNION ALL over the monthly-partitioned in/out int_ legs. The heavy
-- group-by + counterparty cardinality blow-up lives upstream so refreshing
-- this fct is cheap.

WITH all_legs AS (
  SELECT
    date,
    token_address,
    symbol,
    address,
    counterparty,
    direction,
    net_amount_raw,
    gross_amount_raw,
    transfer_count
  FROM {{ ref('int_execution_account_token_movements_out_daily') }}

  UNION ALL

  SELECT
    date,
    token_address,
    symbol,
    address,
    counterparty,
    direction,
    net_amount_raw,
    gross_amount_raw,
    transfer_count
  FROM {{ ref('int_execution_account_token_movements_in_daily') }}
)

SELECT
  date,
  token_address,
  symbol,
  'WHITELISTED' AS token_class,
  address,
  counterparty,
  direction,
  net_amount_raw,
  gross_amount_raw,
  transfer_count
FROM all_legs
WHERE address != '0x0000000000000000000000000000000000000000'
  AND counterparty IS NOT NULL
  AND counterparty != ''
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
  {% endif %}
