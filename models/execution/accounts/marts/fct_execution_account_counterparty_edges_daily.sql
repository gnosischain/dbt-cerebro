{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    partition_by='toStartOfMonth(date)',
    order_by='(source, date, target, edge_type)',
    unique_key='(date, source, target, edge_type)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production', 'execution', 'accounts', 'portfolio', 'graph', 'granularity:daily']
  )
}}

WITH token_edges AS (
  SELECT
    date,
    address AS source,
    counterparty AS target,
    'token_transfer' AS edge_type,
    sum(transfer_count) AS weight,
    sum(gross_amount_raw) AS raw_volume,
    max(date) AS last_seen_date
  FROM {{ ref('fct_execution_account_token_movements_daily') }}
  WHERE date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('date', 'date', true, lookback_days=2) }}
    {% endif %}
  GROUP BY date, source, target, edge_type
),

gpay_edges AS (
  SELECT
    toDate(block_timestamp) AS date,
    lower(wallet_address) AS source,
    lower(counterparty) AS target,
    'gpay_activity' AS edge_type,
    count() AS weight,
    sum(abs(value_raw)) AS raw_volume,
    max(toDate(block_timestamp)) AS last_seen_date
  FROM {{ ref('int_execution_gpay_activity') }}
  WHERE counterparty IS NOT NULL
    AND counterparty != ''
    {% if start_month and end_month %}
      AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
      AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', true, lookback_days=2) }}
    {% endif %}
  GROUP BY date, source, target, edge_type
)

SELECT * FROM token_edges
UNION ALL
SELECT * FROM gpay_edges
