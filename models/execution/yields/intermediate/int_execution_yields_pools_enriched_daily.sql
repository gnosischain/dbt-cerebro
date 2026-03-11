{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'pools', 'daily']
    )
}}

{#-
  Central token-level balance enrichment for all LP pools.
  Joins raw balances with token metadata and prices (ASOF join for gap safety)
  to produce TVL components. Single source of truth for TVL across all
  downstream pool models.
-#}

WITH

token_meta AS (
    SELECT
        lower(address) AS token_address,
        nullIf(upper(trimBoth(symbol)), '') AS token,
        decimals,
        date_start,
        date_end
    FROM {{ ref('tokens_whitelist') }}
),

prices AS (
    SELECT
        toDate(date) AS date,
        nullIf(upper(trimBoth(symbol)), '') AS token,
        toFloat64(price) AS price_usd
    FROM {{ ref('int_execution_token_prices_daily') }}
    WHERE date < today()
)

SELECT
    toDate(b.date) AS date,
    b.protocol AS protocol,
    multiIf(
        startsWith(lower(b.pool_address), '0x'),
        lower(b.pool_address),
        concat('0x', lower(b.pool_address))
    ) AS pool_address,
    replaceAll(
        multiIf(
            startsWith(lower(b.pool_address), '0x'),
            lower(b.pool_address),
            concat('0x', lower(b.pool_address))
        ),
        '0x',
        ''
    ) AS pool_address_no0x,
    lower(b.token_address) AS token_address,
    tm.token AS token,
    b.token_amount AS token_amount,
    p.price_usd AS price_usd,
    b.token_amount * p.price_usd AS tvl_component_usd
FROM {{ ref('int_execution_pools_balances_daily') }} b
LEFT JOIN token_meta tm
  ON tm.token_address = lower(b.token_address)
 AND toDate(b.date) >= toDate(tm.date_start)
 AND (tm.date_end IS NULL OR toDate(b.date) < toDate(tm.date_end))
ASOF LEFT JOIN (
    SELECT * FROM prices ORDER BY token, date
) p
  ON p.token = tm.token
 AND toDate(b.date) >= p.date
WHERE b.date < today()
