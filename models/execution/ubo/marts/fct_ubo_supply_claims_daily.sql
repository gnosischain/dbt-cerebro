{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, container_address, ubo_address, token_address)',
        unique_key='(date, protocol, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','ubo','claims','supply_claims']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_aave_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_balancer_v2_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_uniswap_v3_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_swapr_v3_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_curve_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_sdai_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}
