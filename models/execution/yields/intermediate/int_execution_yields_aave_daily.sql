{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, token_address)',
        unique_key='(date, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','yields','aave']
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
        toUInt256OrNull(decoded_params['variableBorrowRate']) AS variable_borrow_rate_ray,
        toFloat64(toUInt256OrNull(decoded_params['liquidityIndex'])) AS liquidity_index
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityRate'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

-- Extract activity events (Supply, Borrow, Withdraw, Repay, LiquidationCall) for volumes and user counts
aave_activity_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS token_address,
        lower(decoded_params['user']) AS user_address,
        event_name AS event_type,
        toUInt256OrNull(decoded_params['amount']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name IN ('Supply', 'Borrow', 'Withdraw', 'Repay')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['user'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}

    UNION ALL

    SELECT
        toStartOfDay(block_timestamp) AS date,
        lower(decoded_params['collateralAsset']) AS token_address,
        lower(decoded_params['user']) AS user_address,
        'LiquidationWithdraw' AS event_type,
        toUInt256OrNull(decoded_params['liquidatedCollateralAmount']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['collateralAsset'] IS NOT NULL
      AND decoded_params['user'] IS NOT NULL
      AND decoded_params['liquidatedCollateralAmount'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

latest_rates AS (
    SELECT
        date,
        token_address,
        argMax(liquidity_rate_ray, block_timestamp) AS liquidity_rate_ray,
        argMax(variable_borrow_rate_ray, block_timestamp) AS variable_borrow_rate_ray,
        argMax(liquidity_index, block_timestamp) AS liquidity_index
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
        sum(amount_raw) FILTER (WHERE event_type = 'Borrow') AS borrows_volume_raw,
        sum(amount_raw) FILTER (WHERE event_type = 'Withdraw') AS withdrawals_volume_raw,
        sum(amount_raw) FILTER (WHERE event_type = 'Repay') AS repays_volume_raw,
        sum(amount_raw) FILTER (WHERE event_type = 'LiquidationWithdraw') AS liquidated_supply_raw
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
        lr.liquidity_index,
        CASE 
            WHEN lr.liquidity_rate_ray = 0 OR lr.liquidity_rate_ray IS NULL THEN 0
            ELSE floor(
                (toFloat64(lr.liquidity_rate_ray) / 1e27) * 100,
                4
            )
        END AS apy_daily,
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
       AND lr.date >= toDate(w.date_start)
       AND (w.date_end IS NULL OR lr.date < toDate(w.date_end))
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
        ws.liquidity_index,
        aa.lenders_bitmap_state,
        aa.borrowers_bitmap_state,
        COALESCE(aa.lenders_count_daily, 0) AS lenders_count_daily,
        COALESCE(aa.borrowers_count_daily, 0) AS borrowers_count_daily,
        COALESCE(aa.deposits_volume_raw / POWER(10, COALESCE(ws.decimals, 18)), 0) AS deposits_volume_daily,
        COALESCE(aa.borrows_volume_raw / POWER(10, COALESCE(ws.decimals, 18)), 0) AS borrows_volume_daily,
        COALESCE(aa.withdrawals_volume_raw / POWER(10, COALESCE(ws.decimals, 18)), 0) AS withdrawals_volume_daily,
        COALESCE(aa.repays_volume_raw / POWER(10, COALESCE(ws.decimals, 18)), 0) AS repays_volume_daily,
        (
            COALESCE(toFloat64(aa.deposits_volume_raw), 0)
            - COALESCE(toFloat64(aa.withdrawals_volume_raw), 0)
            - COALESCE(toFloat64(aa.liquidated_supply_raw), 0)
        ) / POWER(10, COALESCE(ws.decimals, 18)) AS net_supply_change_daily
    FROM with_symbols ws
    LEFT JOIN activity_agg aa
        ON ws.date = aa.date
        AND ws.token_address = aa.token_address
),

{% if is_incremental() %}
last_known_apy AS (
    SELECT
        token_address,
        argMax(apy_daily, date) AS last_apy,
        argMax(borrow_apy_variable_daily, date) AS last_borrow_apy,
        argMax(liquidity_index, date) AS last_liquidity_index
    FROM {{ this }}
    WHERE apy_daily IS NOT NULL
    GROUP BY token_address
),
{% endif %}

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

filled_yields AS (
    SELECT
        c.date,
        c.token_address,
        c.symbol,
        c.token_class,
        'Aave V3' AS protocol,
        COALESCE(
            last_value(ywa.apy_daily) IGNORE NULLS OVER (
                PARTITION BY c.token_address 
                ORDER BY c.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            {% if is_incremental() %}
            lka.last_apy
            {% else %}
            NULL
            {% endif %}
        ) AS apy_daily,
        COALESCE(
            last_value(ywa.borrow_apy_variable_daily) IGNORE NULLS OVER (
                PARTITION BY c.token_address 
                ORDER BY c.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            {% if is_incremental() %}
            lka.last_borrow_apy
            {% else %}
            NULL
            {% endif %}
        ) AS borrow_apy_variable_daily,
        COALESCE(
            last_value(ywa.liquidity_index) IGNORE NULLS OVER (
                PARTITION BY c.token_address 
                ORDER BY c.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            {% if is_incremental() %}
            lka.last_liquidity_index
            {% else %}
            NULL
            {% endif %}
        ) AS liquidity_index,
        ywa.lenders_bitmap_state,
        ywa.borrowers_bitmap_state,
        COALESCE(ywa.lenders_count_daily, 0) AS lenders_count_daily,
        COALESCE(ywa.borrowers_count_daily, 0) AS borrowers_count_daily,
        COALESCE(ywa.deposits_volume_daily, 0) AS deposits_volume_daily,
        COALESCE(ywa.borrows_volume_daily, 0) AS borrows_volume_daily,
        COALESCE(ywa.withdrawals_volume_daily, 0) AS withdrawals_volume_daily,
        COALESCE(ywa.repays_volume_daily, 0) AS repays_volume_daily,
        COALESCE(ywa.net_supply_change_daily, 0) AS net_supply_change_daily
    FROM calendar c
    LEFT JOIN yields_with_activity ywa
        ON ywa.token_address = c.token_address
        AND ywa.date = c.date
    {% if is_incremental() %}
    LEFT JOIN last_known_apy lka
        ON lka.token_address = c.token_address
    {% endif %}
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    protocol,
    apy_daily,
    borrow_apy_variable_daily,
    CASE 
        WHEN borrow_apy_variable_daily IS NOT NULL AND apy_daily IS NOT NULL
        THEN ROUND(borrow_apy_variable_daily - apy_daily, 2)
        ELSE NULL
    END AS spread_variable,
    liquidity_index,
    lenders_bitmap_state,
    borrowers_bitmap_state,
    lenders_count_daily,
    borrowers_count_daily,
    deposits_volume_daily,
    borrows_volume_daily,
    withdrawals_volume_daily,
    repays_volume_daily,
    net_supply_change_daily
FROM filled_yields
WHERE apy_daily IS NOT NULL
ORDER BY date, token_address

