




WITH

aave_rate_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS event_date,
        lower(decoded_params['reserve']) AS token_address,
        block_timestamp,
        toUInt256OrNull(decoded_params['liquidityRate']) AS liquidity_rate_ray,
        toUInt256OrNull(decoded_params['variableBorrowRate']) AS variable_borrow_rate_ray,
        toFloat64(toUInt256OrNull(decoded_params['liquidityIndex'])) AS liquidity_index,
        toFloat64(toUInt256OrNull(decoded_params['variableBorrowIndex'])) AS variable_borrow_index
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityRate'] IS NOT NULL
      AND block_timestamp < today()
      
        AND toStartOfMonth(toDate(block_timestamp)) >= (
          SELECT toStartOfMonth(max(`date`)) FROM `dbt`.`int_execution_lending_aave_daily`
        )
        AND toDate(block_timestamp) >= (
          SELECT max(`date`) FROM `dbt`.`int_execution_lending_aave_daily`
        )
      
),

aave_activity_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS event_date,
        lower(decoded_params['reserve']) AS token_address,
        lower(decoded_params['user']) AS user_address,
        event_name AS event_type,
        toUInt256OrNull(decoded_params['amount']) AS amount_raw
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name IN ('Supply', 'Borrow', 'Withdraw', 'Repay')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['user'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      
        AND toStartOfMonth(toDate(block_timestamp)) >= (
          SELECT toStartOfMonth(max(`date`)) FROM `dbt`.`int_execution_lending_aave_daily`
        )
        AND toDate(block_timestamp) >= (
          SELECT max(`date`) FROM `dbt`.`int_execution_lending_aave_daily`
        )
      

    UNION ALL

    SELECT
        toStartOfDay(block_timestamp) AS event_date,
        lower(decoded_params['collateralAsset']) AS token_address,
        lower(decoded_params['user']) AS user_address,
        'LiquidationWithdraw' AS event_type,
        toUInt256OrNull(decoded_params['liquidatedCollateralAmount']) AS amount_raw
    FROM `dbt`.`contracts_aaveV3_PoolInstance_events`
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['collateralAsset'] IS NOT NULL
      AND decoded_params['user'] IS NOT NULL
      AND decoded_params['liquidatedCollateralAmount'] IS NOT NULL
      AND block_timestamp < today()
      
        AND toStartOfMonth(toDate(block_timestamp)) >= (
          SELECT toStartOfMonth(max(`date`)) FROM `dbt`.`int_execution_lending_aave_daily`
        )
        AND toDate(block_timestamp) >= (
          SELECT max(`date`) FROM `dbt`.`int_execution_lending_aave_daily`
        )
      
),

latest_rates AS (
    SELECT
        event_date,
        token_address,
        argMax(liquidity_rate_ray, block_timestamp) AS liquidity_rate_ray,
        argMax(variable_borrow_rate_ray, block_timestamp) AS variable_borrow_rate_ray,
        argMax(liquidity_index, block_timestamp) AS liquidity_index,
        argMax(variable_borrow_index, block_timestamp) AS variable_borrow_index
    FROM aave_rate_events
    GROUP BY event_date, token_address
),

activity_agg AS (
    SELECT
        event_date,
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
    GROUP BY event_date, token_address
),

with_symbols AS (
    SELECT
        lr.event_date AS metric_date,
        lr.token_address,
        rm.reserve_symbol AS symbol,
        rm.token_class,
        rm.decimals,
        'Aave V3' AS protocol,
        lr.liquidity_index,
        lr.variable_borrow_index,
        CASE
            WHEN lr.liquidity_rate_ray = 0 OR lr.liquidity_rate_ray IS NULL THEN 0
            ELSE floor(
                (pow(1 + toFloat64(lr.liquidity_rate_ray) / 1e27 / 31536000, 31536000) - 1) * 100,
                4
            )
        END AS apy_daily,
        CASE
            WHEN lr.variable_borrow_rate_ray = 0 OR lr.variable_borrow_rate_ray IS NULL THEN NULL
            ELSE floor(
                (pow(1 + toFloat64(lr.variable_borrow_rate_ray) / 1e27 / 31536000, 31536000) - 1) * 100,
                4
            )
        END AS borrow_apy_variable_daily
    FROM latest_rates lr
    INNER JOIN `dbt`.`atoken_reserve_mapping` rm
        ON lower(rm.reserve_address) = lr.token_address
    WHERE lr.liquidity_rate_ray IS NOT NULL
),

-- Join yields with activity metrics
yields_with_activity AS (
    SELECT
        ws.metric_date,
        ws.token_address,
        ws.symbol,
        ws.token_class,
        ws.protocol,
        ws.apy_daily,
        ws.borrow_apy_variable_daily,
        ws.liquidity_index,
        ws.variable_borrow_index,
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
        ON ws.metric_date = aa.event_date
        AND ws.token_address = aa.token_address
),


date_range AS (
    SELECT 
        MIN(metric_date) AS min_date,
        MAX(metric_date) AS max_date
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
        addDays(dr.min_date, offset) AS metric_date
    FROM token_combinations tc
    CROSS JOIN date_range dr
    ARRAY JOIN range(toUInt64(dateDiff('day', dr.min_date, dr.max_date) + 1)) AS offset
),

calendar_with_data AS (
    SELECT
        c.metric_date,
        c.token_address,
        c.symbol,
        c.token_class,
        'Aave V3' AS protocol,
        last_value(ywa.apy_daily) IGNORE NULLS OVER (
            PARTITION BY c.token_address 
            ORDER BY c.metric_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS apy_daily,
        last_value(ywa.borrow_apy_variable_daily) IGNORE NULLS OVER (
            PARTITION BY c.token_address 
            ORDER BY c.metric_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS borrow_apy_variable_daily,
        last_value(ywa.liquidity_index) IGNORE NULLS OVER (
            PARTITION BY c.token_address 
            ORDER BY c.metric_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS liquidity_index,
        last_value(ywa.variable_borrow_index) IGNORE NULLS OVER (
            PARTITION BY c.token_address 
            ORDER BY c.metric_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS variable_borrow_index,
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
        AND ywa.metric_date = c.metric_date
)

SELECT
    f.metric_date AS date,
    f.token_address,
    f.symbol,
    f.token_class,
    f.protocol,
    
    COALESCE(f.apy_daily, lka.last_apy) AS apy_daily,
    COALESCE(f.borrow_apy_variable_daily, lka.last_borrow_apy) AS borrow_apy_variable_daily,
    
    CASE 
        WHEN COALESCE(f.borrow_apy_variable_daily, lka.last_borrow_apy) IS NOT NULL
         AND COALESCE(f.apy_daily, lka.last_apy) IS NOT NULL
        THEN ROUND(
            COALESCE(f.borrow_apy_variable_daily, lka.last_borrow_apy)
            - COALESCE(f.apy_daily, lka.last_apy), 2)
        ELSE NULL
    END AS spread_variable,
    
    COALESCE(f.liquidity_index, lka.last_liquidity_index) AS liquidity_index,
    COALESCE(f.variable_borrow_index, lka.last_variable_borrow_index) AS variable_borrow_index,
    
    f.lenders_bitmap_state,
    f.borrowers_bitmap_state,
    f.lenders_count_daily,
    f.borrowers_count_daily,
    f.deposits_volume_daily,
    f.borrows_volume_daily,
    f.withdrawals_volume_daily,
    f.repays_volume_daily,
    f.net_supply_change_daily
FROM calendar_with_data f

LEFT JOIN (
    SELECT
        token_address,
        argMax(apy_daily, `date`) AS last_apy,
        argMax(borrow_apy_variable_daily, `date`) AS last_borrow_apy,
        argMax(liquidity_index, `date`) AS last_liquidity_index,
        argMax(variable_borrow_index, `date`) AS last_variable_borrow_index
    FROM `dbt`.`int_execution_lending_aave_daily`
    WHERE apy_daily IS NOT NULL
    GROUP BY token_address
) lka ON lka.token_address = f.token_address

WHERE COALESCE(f.apy_daily, lka.last_apy) IS NOT NULL
ORDER BY f.metric_date, f.token_address