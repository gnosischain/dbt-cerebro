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

WITH

sparse_supply AS (
    SELECT
        b.date,
        b.token_address,
        any(b.symbol)      AS symbol,
        any(b.token_class) AS token_class,

        sumIf(
            b.balance,
            lower(b.address) != '0x0000000000000000000000000000000000000000'
        ) AS supply,

        toUInt64(
          countDistinctIf(
              b.address,
              b.balance > 0
              AND lower(b.address) != '0x0000000000000000000000000000000000000000'
          )
        ) AS holders
    FROM {{ ref('int_execution_tokens_balances_daily') }} b
    WHERE b.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(b.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(b.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('b.date', 'date', 'true') }}
      {% endif %}
    GROUP BY b.date, b.token_address
),

bounds AS (
    SELECT
        min(date) AS min_date,
        max(date) AS max_date
    FROM sparse_supply
),

token_list AS (
    SELECT
        token_address,
        any(symbol)      AS symbol,
        any(token_class) AS token_class
    FROM sparse_supply
    GROUP BY token_address
),

prev_supply AS (
    {% if is_incremental() %}
    SELECT
        token_address,
        any(symbol)      AS symbol,
        any(token_class) AS token_class,
        argMax(supply, date)          AS supply,
        toUInt64(argMax(holders, date)) AS holders
    FROM {{ this }}
    WHERE date < (SELECT min_date FROM bounds)
    GROUP BY token_address
    {% else %}
    SELECT
        cast('' AS String)  AS token_address,
        cast('' AS String)  AS symbol,
        cast('' AS String)  AS token_class,
        cast(0  AS Float64) AS supply,
        cast(0  AS UInt64)  AS holders
    WHERE 0
    {% endif %}
),

prev_supply_min AS (
    SELECT
        (SELECT min_date FROM bounds) AS date,
        p.token_address,
        p.symbol,
        p.token_class,
        p.supply,
        p.holders
    FROM prev_supply p
    LEFT JOIN sparse_supply s
      ON s.token_address = p.token_address
     AND s.date = (SELECT min_date FROM bounds)
    WHERE s.token_address IS NULL
),

supply_seed AS (
    SELECT * FROM sparse_supply
    UNION ALL
    SELECT * FROM prev_supply_min
),

calendar AS (
    SELECT
        toDate(arrayJoin(
            range(
                toUInt32((SELECT min_date FROM bounds)),
                toUInt32((SELECT max_date FROM bounds)) + 1
            )
        )) AS date
),

token_calendar AS (
    SELECT
        c.date,
        t.token_address,
        t.symbol,
        t.token_class
    FROM calendar c
    CROSS JOIN token_list t
),

dense_supply AS (
    SELECT
        tc.date,
        tc.token_address,
        tc.symbol,
        tc.token_class,

        last_value(s.supply) IGNORE NULLS
          OVER (PARTITION BY tc.token_address ORDER BY tc.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS supply,

        last_value(s.holders) IGNORE NULLS
          OVER (PARTITION BY tc.token_address ORDER BY tc.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS holders
    FROM token_calendar tc
    LEFT JOIN supply_seed s
      ON s.token_address = tc.token_address
     AND s.date = tc.date
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
    FROM dense_supply b
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