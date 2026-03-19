{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, token_address)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','tokens','metrics_daily']
  )
}}

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
),

keys AS (
    SELECT
        date,
        token_address,
        any(symbol) AS symbol,
        any(token_class) AS token_class
    FROM (
        SELECT
            date,
            token_address,
            symbol,
            token_class
        FROM supply_holders

        UNION ALL

        SELECT
            date,
            token_address,
            symbol,
            token_class
        FROM transfers
    )
    GROUP BY
        date,
        token_address
),

price_keys AS (
    SELECT DISTINCT
        date,
        upper(symbol) AS symbol_upper
    FROM keys
    WHERE symbol IS NOT NULL
),

prices AS (
    SELECT
        date,
        upper(symbol) AS symbol_upper,
        price AS price_usd
    FROM {{ ref('int_execution_token_prices_daily') }}
    WHERE date < today()
      AND (date, upper(symbol)) IN (
          SELECT
              date,
              symbol_upper
          FROM price_keys
      )
),

joined AS (
    SELECT
        k.date AS date,
        k.token_address AS token_address,
        coalesce(sh.symbol, t.symbol, k.symbol) AS symbol,
        coalesce(sh.token_class, t.token_class, k.token_class) AS token_class,

        sh.supply,
        sh.supply * coalesce(p.price_usd, 0) AS supply_usd,
        sh.holders,

        t.volume_token,
        t.volume_token * coalesce(p.price_usd, 0) AS volume_usd,
        t.transfer_count,
        t.ua_bitmap_state,
        t.active_senders,
        t.unique_receivers
    FROM keys k
    LEFT JOIN supply_holders sh
      ON sh.date = k.date
     AND sh.token_address = k.token_address
    LEFT JOIN transfers t
      ON t.date = k.date
     AND t.token_address = k.token_address
    LEFT JOIN prices p
      ON p.date = k.date
     AND p.symbol_upper = upper(coalesce(sh.symbol, t.symbol, k.symbol))
)

SELECT
    date,
    token_address,
    symbol,
    token_class,

    supply,
    supply_usd,
    holders,

    volume_token,
    volume_usd,
    transfer_count,
    ua_bitmap_state,
    active_senders,
    unique_receivers
FROM joined
WHERE date < today()
