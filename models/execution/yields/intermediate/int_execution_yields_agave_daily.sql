{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, token_address)',
        unique_key='(date, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','yields','agave']
    )
}}

WITH

agave_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS token_address,
        block_timestamp,
        toUInt256OrNull(decoded_params['liquidityRate']) AS liquidity_rate_ray
    FROM {{ ref('contracts_agave_LendingPool_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityRate'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

latest_rates AS (
    SELECT
        date,
        token_address,
        argMax(liquidity_rate_ray, block_timestamp) AS liquidity_rate_ray
    FROM agave_events
    GROUP BY date, token_address
),

with_symbols AS (
    SELECT
        lr.date,
        lr.token_address,
        w.symbol,
        'Agave' AS protocol,
        -- Convert RAY to APY: liquidityRate / 1e27 is already the APY as a decimal
        CASE 
            WHEN lr.liquidity_rate_ray = 0 OR lr.liquidity_rate_ray IS NULL THEN 0
            ELSE floor(
                (toFloat64(lr.liquidity_rate_ray) / 1e27) * 100,
                4
            )
        END AS apy_daily
    FROM latest_rates lr
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = lr.token_address
    WHERE lr.liquidity_rate_ray IS NOT NULL
)

SELECT
    date,
    token_address,
    symbol,
    protocol,
    apy_daily
FROM with_symbols
ORDER BY date, token_address

