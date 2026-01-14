{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, token_address)',
        unique_key='(date, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','yields','aave']
    )
}}

WITH

-- Extract APY rates from ReserveDataUpdated events
aave_rate_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS token_address,
        block_timestamp,
        toUInt256OrNull(decoded_params['liquidityRate']) AS liquidity_rate_ray,
        toUInt256OrNull(decoded_params['variableBorrowRate']) AS variable_borrow_rate_ray
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityRate'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

-- Extract activity events (Supply, Borrow) for volumes and user counts
aave_activity_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS token_address,
        lower(decoded_params['user']) AS user_address,
        event_name AS event_type,
        toUInt256OrNull(decoded_params['amount']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name IN ('Supply', 'Borrow')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['user'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

latest_rates AS (
    SELECT
        date,
        token_address,
        argMax(liquidity_rate_ray, block_timestamp) AS liquidity_rate_ray,
        argMax(variable_borrow_rate_ray, block_timestamp) AS variable_borrow_rate_ray
    FROM aave_rate_events
    GROUP BY date, token_address
),

-- Aggregate activity metrics by date and token
activity_agg AS (
    SELECT
        date,
        token_address,
        -- Bitmap states for unique user tracking
        groupBitmapState(cityHash64(user_address)) FILTER (WHERE event_type = 'Supply') AS lenders_bitmap_state,
        groupBitmapState(cityHash64(user_address)) FILTER (WHERE event_type = 'Borrow') AS borrowers_bitmap_state,
        -- Daily counts
        uniqExact(user_address) FILTER (WHERE event_type = 'Supply') AS lenders_count_daily,
        uniqExact(user_address) FILTER (WHERE event_type = 'Borrow') AS borrowers_count_daily,
        -- Volumes (will be converted from wei later)
        sum(amount_raw) FILTER (WHERE event_type = 'Supply') AS deposits_volume_raw,
        sum(amount_raw) FILTER (WHERE event_type = 'Borrow') AS borrows_volume_raw
    FROM aave_activity_events
    GROUP BY date, token_address
),

with_symbols AS (
    SELECT
        lr.date,
        lr.token_address,
        w.symbol,
        w.token_class,
        w.decimals,
        'Aave V3' AS protocol,
        -- Convert RAY to APY: liquidityRate / 1e27 is already the APY as a decimal
        CASE 
            WHEN lr.liquidity_rate_ray = 0 OR lr.liquidity_rate_ray IS NULL THEN 0
            ELSE floor(
                (toFloat64(lr.liquidity_rate_ray) / 1e27) * 100,
                4
            )
        END AS apy_daily,
        -- Convert RAY to APY for borrow rate
        CASE 
            WHEN lr.variable_borrow_rate_ray = 0 OR lr.variable_borrow_rate_ray IS NULL THEN NULL
            ELSE floor(
                (toFloat64(lr.variable_borrow_rate_ray) / 1e27) * 100,
                4
            )
        END AS borrow_apy_variable_daily
    FROM latest_rates lr
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = lr.token_address
    WHERE lr.liquidity_rate_ray IS NOT NULL
),

-- Join yields with activity metrics
yields_with_activity AS (
    SELECT
        ws.date,
        ws.token_address,
        ws.symbol,
        ws.token_class,
        ws.protocol,
        ws.apy_daily,
        ws.borrow_apy_variable_daily,
        -- Activity metrics (may be NULL if no activity that day)
        aa.lenders_bitmap_state,
        aa.borrowers_bitmap_state,
        COALESCE(aa.lenders_count_daily, 0) AS lenders_count_daily,
        COALESCE(aa.borrowers_count_daily, 0) AS borrowers_count_daily,
        -- Convert volumes from wei to token units
        COALESCE(aa.deposits_volume_raw / POWER(10, COALESCE(ws.decimals, 18)), 0) AS deposits_volume_daily,
        COALESCE(aa.borrows_volume_raw / POWER(10, COALESCE(ws.decimals, 18)), 0) AS borrows_volume_daily
    FROM with_symbols ws
    LEFT JOIN activity_agg aa
        ON ws.date = aa.date
        AND ws.token_address = aa.token_address
),

-- Get date range and unique token combinations
date_range AS (
    SELECT 
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM yields_with_activity
),

token_combinations AS (
    SELECT DISTINCT
        token_address,
        symbol,
        token_class
    FROM yields_with_activity
),

-- Create calendar: all dates for each token combination
calendar AS (
    SELECT
        tc.token_address,
        tc.symbol,
        tc.token_class,
        addDays(dr.min_date, offset) AS date
    FROM token_combinations tc
    CROSS JOIN date_range dr
    ARRAY JOIN range(toUInt64(dateDiff('day', dr.min_date, dr.max_date) + 1)) AS offset
),

-- Forward fill: use last known value for missing days (only for APY rates)
-- Activity metrics don't need forward-filling (show only days with activity)
filled_yields AS (
    SELECT
        c.date,
        c.token_address,
        c.symbol,
        c.token_class,
        'Aave V3' AS protocol,
        -- Forward fill: get the last known apy_daily value up to this date
        last_value(ywa.apy_daily) IGNORE NULLS OVER (
            PARTITION BY c.token_address 
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS apy_daily,
        -- Forward fill: get the last known borrow_apy_variable_daily value up to this date
        last_value(ywa.borrow_apy_variable_daily) IGNORE NULLS OVER (
            PARTITION BY c.token_address 
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS borrow_apy_variable_daily,
        -- Activity metrics: use actual values (no forward fill)
        ywa.lenders_bitmap_state,
        ywa.borrowers_bitmap_state,
        COALESCE(ywa.lenders_count_daily, 0) AS lenders_count_daily,
        COALESCE(ywa.borrowers_count_daily, 0) AS borrowers_count_daily,
        COALESCE(ywa.deposits_volume_daily, 0) AS deposits_volume_daily,
        COALESCE(ywa.borrows_volume_daily, 0) AS borrows_volume_daily
    FROM calendar c
    LEFT JOIN yields_with_activity ywa
        ON ywa.token_address = c.token_address
        AND ywa.date = c.date
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    protocol,
    apy_daily,
    borrow_apy_variable_daily,
    -- Calculate spread: borrow APY - lend APY
    CASE 
        WHEN borrow_apy_variable_daily IS NOT NULL AND apy_daily IS NOT NULL
        THEN ROUND(borrow_apy_variable_daily - apy_daily, 2)
        ELSE NULL
    END AS spread_variable,
    lenders_bitmap_state,
    borrowers_bitmap_state,
    lenders_count_daily,
    borrowers_count_daily,
    deposits_volume_daily,
    borrows_volume_daily
FROM filled_yields
WHERE apy_daily IS NOT NULL
ORDER BY date, token_address

