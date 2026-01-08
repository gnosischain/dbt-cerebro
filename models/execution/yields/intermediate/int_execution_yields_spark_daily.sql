{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, token_address)',
        unique_key='(date, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','yields','spark']
    )
}}

WITH

spark_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS token_address,
        block_timestamp,
        toUInt256OrNull(decoded_params['liquidityRate']) AS liquidity_rate_ray
    FROM {{ ref('contracts_spark_Pool_events') }}
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
    FROM spark_events
    GROUP BY date, token_address
),

with_symbols AS (
    SELECT
        lr.date,
        lr.token_address,
        w.symbol,
        'Spark' AS protocol,
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
),

-- Get date range and unique token combinations
date_range AS (
    SELECT 
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM with_symbols
),

token_combinations AS (
    SELECT DISTINCT
        token_address,
        symbol
    FROM with_symbols
),

-- Create calendar: all dates for each token combination
calendar AS (
    SELECT
        tc.token_address,
        tc.symbol,
        addDays(dr.min_date, offset) AS date
    FROM token_combinations tc
    CROSS JOIN date_range dr
    ARRAY JOIN range(toUInt64(dateDiff('day', dr.min_date, dr.max_date) + 1)) AS offset
),

-- Forward fill: use last known value for missing days
filled_yields AS (
    SELECT
        c.date,
        c.token_address,
        c.symbol,
        'Spark' AS protocol,
        -- Forward fill: get the last known apy_daily value up to this date
        last_value(ws.apy_daily) IGNORE NULLS OVER (
            PARTITION BY c.token_address 
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS apy_daily
    FROM calendar c
    LEFT JOIN with_symbols ws
        ON ws.token_address = c.token_address
        AND ws.date = c.date
)

SELECT
    date,
    token_address,
    symbol,
    protocol,
    apy_daily
FROM filled_yields
WHERE apy_daily IS NOT NULL
ORDER BY date, token_address

