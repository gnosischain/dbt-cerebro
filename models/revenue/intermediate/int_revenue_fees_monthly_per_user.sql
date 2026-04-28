{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(month, stream_type, symbol, user)',
    partition_by='toStartOfYear(month)',
    unique_key='(month, stream_type, symbol, user)',
    settings={'allow_nullable_key': 1},
    tags=['production','revenue','revenue_cross']
  )
}}

WITH daily AS (
    SELECT 'holdings' AS stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_holdings_fees_daily') }}
    UNION ALL
    SELECT 'sdai'     AS stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_sdai_fees_daily') }}
    UNION ALL
    SELECT 'gpay'     AS stream_type, date, user, symbol, fees
    FROM {{ ref('int_revenue_gpay_fees_daily') }}
)

SELECT
    toStartOfMonth(date) AS month,
    stream_type,
    user,
    symbol,
    round(sum(fees), 8) AS month_fees
FROM daily
WHERE toStartOfMonth(date) < toStartOfMonth(today())
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'month', true, lookback_days=2, lookback_res='month') }}
  {% endif %}
GROUP BY month, stream_type, user, symbol
