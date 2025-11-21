{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, token_address)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, token_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','value_daily']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH balances_agg AS (
    SELECT
        b.date,
        b.token_address,
        any(b.symbol)      AS symbol,
        any(b.token_class) AS token_class,

        sumIf(
            b.balance,
            lower(b.address) != '0x0000000000000000000000000000000000000000'
        ) AS supply,

        countDistinctIf(
            b.address,
            b.balance > 0
            AND lower(b.address) != '0x0000000000000000000000000000000000000000'
        ) AS holders
    FROM {{ ref('fct_execution_tokens_balances_daily') }} b
    WHERE b.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
      {% endif %}
    GROUP BY b.date, b.token_address
),

flows AS (
    SELECT
        t.date,
        t.token_address,
        t.symbol,
        t.token_class,
        t.volume_token,
        t.volume_usd,
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

joined AS (
    SELECT
        coalesce(b.date, f.date) AS date,
        coalesce(b.token_address, f.token_address) AS token_address,
        coalesce(b.symbol, f.symbol) AS symbol,
        coalesce(b.token_class, f.token_class) AS token_class,

        b.supply,
        b.holders,

        f.volume_token,
        f.volume_usd,
        f.transfer_count,
        f.ua_bitmap_state,
        f.active_senders,
        f.unique_receivers,

        p.price AS price_usd
    FROM balances_agg b
    FULL OUTER JOIN flows f
      ON b.date = f.date
     AND b.token_address = f.token_address

    LEFT JOIN {{ ref('int_execution_token_prices_daily') }} p
      ON p.date = coalesce(b.date, f.date)
     AND p.symbol = upper(coalesce(b.symbol, f.symbol))
)

SELECT
    date,
    token_address,
    symbol,
    token_class,

    supply,
    holders,
    price_usd,
    supply * price_usd AS value_usd,

    volume_token,
    volume_usd,
    transfer_count,
    ua_bitmap_state,
    active_senders,
    unique_receivers
FROM joined
WHERE date < today()
ORDER BY date, token_address