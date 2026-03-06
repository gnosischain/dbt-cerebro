{{ 
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address, address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address, address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','address_deltas']
  ) 
}}

-- depends_on: {{ ref('int_execution_transfers_whitelisted_daily') }}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}
{% set symbol = var('symbol', none) %}
{% set symbol_exclude = var('symbol_exclude', none) %}

{% set symbol_sql %}
  {{ symbol_filter('symbol', symbol, 'include') }}
  {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
{% endset %}

WITH base AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        symbol,
        lower("from")        AS from_address,
        lower("to")          AS to_address,
        amount_raw               AS amount_raw
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', true, filters_sql=symbol_sql) }}
      {% endif %}
      {{ symbol_filter('symbol', symbol, 'include') }}
      {{ symbol_filter('symbol', symbol_exclude, 'exclude') }}
),

with_class AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        coalesce(w.token_class, 'OTHER') AS token_class,
        b.from_address,
        b.to_address,
        b.amount_raw
    FROM base b
    INNER JOIN {{ ref('tokens_whitelist') }} w
      ON lower(w.address) = b.token_address
     AND b.date >= toDate(w.date_start)
     AND (w.date_end IS NULL OR b.date < toDate(w.date_end))
),

deltas AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        from_address AS address,
        -amount_raw      AS delta_raw
    FROM with_class

    UNION ALL

    SELECT
        date,
        token_address,
        symbol,
        token_class,
        to_address   AS address,
        amount_raw       AS delta_raw
    FROM with_class
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        lower(address) AS address,
        sum(delta_raw)     AS net_delta_raw
    FROM deltas
    GROUP BY date, token_address, symbol, token_class, address
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    address,
    net_delta_raw
FROM agg
WHERE net_delta_raw != 0