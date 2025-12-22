{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['dev','execution','tokens','value_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

supply_holders AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        supply,
        holders
    FROM {{ ref('int_execution_tokens_supply_holders_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
),

transfers AS (
    SELECT
        t.date,
        t.token_address,
        t.symbol,
        t.token_class,
        t.volume_token,
        t.transfer_count,
        t.ua_bitmap_state,
        t.active_senders,
        t.unique_receivers
    FROM {{ ref('int_execution_tokens_transfers_daily') }} t
    WHERE t.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(t.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(t.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('t.date', 'date', 'true') }}
      {% endif %}
),

prices AS (
    SELECT
        date,
        symbol,
        price AS price_usd
    FROM {{ ref('int_execution_token_prices_daily') }}
    WHERE date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
      {% endif %}
),

joined AS (
    SELECT
        coalesce(sh.date, t.date) AS date,
        coalesce(sh.token_address, t.token_address) AS token_address,
        coalesce(sh.symbol, t.symbol) AS symbol,
        coalesce(sh.token_class, t.token_class) AS token_class,

        sh.supply,
        sh.holders,

        t.volume_token,
        t.volume_token * COALESCE(p.price_usd, 0) AS volume_usd,
        t.transfer_count,
        t.ua_bitmap_state,
        t.active_senders,
        t.unique_receivers
    FROM supply_holders sh
    FULL OUTER JOIN transfers t
      ON sh.date = t.date
     AND sh.token_address = t.token_address

    LEFT JOIN prices p
      ON p.date = coalesce(sh.date, t.date)
     AND upper(p.symbol) = upper(coalesce(sh.symbol, t.symbol))
)

SELECT
    date,
    token_address,
    symbol,
    token_class,

    supply,
    holders,

    volume_token,
    volume_usd,
    transfer_count,
    ua_bitmap_state,
    active_senders,
    unique_receivers
FROM joined
WHERE date < today()
ORDER BY date, token_address

