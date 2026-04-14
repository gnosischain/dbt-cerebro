{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, provider, pool_address)',
        unique_key='(date, provider, pool_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'user_portfolio', 'marts']
    )
}}

{#-
  Daily fee collection amounts per user per pool, from V3 Collect events.
  Feeds the fee income chart in the user portfolio dashboard.
-#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(block_timestamp)    AS date,
    provider,
    pool_address,
    protocol,
    sum(amount_usd)            AS fees_usd
FROM {{ ref('int_execution_pools_dex_liquidity_events') }}
WHERE event_type = 'collect'
  AND amount_usd > 0
  AND block_timestamp < today()
  {% if not (start_month and end_month) %}
    {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
  {% endif %}
GROUP BY date, provider, pool_address, protocol
