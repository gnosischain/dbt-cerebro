{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, token_address)',
        unique_key='(date, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','lending','aave','utilization']
    )
}}

-- NOTE: cumulative_scaled_supply and cumulative_scaled_borrow are Int256 for exact
-- WadRayMath. Run with --full-refresh when migrating from the previous Float64 schema.

WITH

-- ReserveDataUpdated snapshots ordered within (tx, reserve) so each pool action can be
-- paired with the RDU that fired at the same position — matches Aave's on-chain behaviour
-- when a single tx produces multiple state updates on the same reserve.
reserve_index_by_tx AS (
    SELECT
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        log_index,
        toUInt256OrZero(decoded_params['liquidityIndex'])       AS liquidity_index,
        toUInt256OrZero(decoded_params['variableBorrowIndex'])  AS variable_borrow_index,
        row_number() OVER (
            PARTITION BY transaction_hash, lower(decoded_params['reserve'])
            ORDER BY log_index
        ) AS event_order
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

supply_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrZero(decoded_params['amount']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name IN ('Supply', 'Withdraw')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}

    UNION ALL

    -- All LiquidationCall rows count here (no receiveAToken filter): utilization tracks
    -- only Pool events, so there is no BalanceTransfer path that could double-count.
    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['collateralAsset']) AS token_address,
        'LiquidationWithdraw' AS event_type,
        toUInt256OrZero(decoded_params['liquidatedCollateralAmount']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['collateralAsset'] IS NOT NULL
      AND decoded_params['liquidatedCollateralAmount'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

borrow_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrZero(decoded_params['amount']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name IN ('Borrow', 'Repay')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}

    UNION ALL

    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        log_index,
        lower(decoded_params['debtAsset']) AS token_address,
        'LiquidationRepay' AS event_type,
        toUInt256OrZero(decoded_params['debtToCover']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['debtAsset'] IS NOT NULL
      AND decoded_params['debtToCover'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

supply_events_ordered AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.transaction_hash, s.token_address
            ORDER BY s.log_index
        ) AS event_order
    FROM supply_events s
),

borrow_events_ordered AS (
    SELECT
        b.*,
        row_number() OVER (
            PARTITION BY b.transaction_hash, b.token_address
            ORDER BY b.log_index
        ) AS event_order
    FROM borrow_events b
),

-- Exact scaled-delta math: rayDivFloor on inflows, rayDivCeil on outflows, matching Aave.
supply_scaled AS (
    SELECT
        s.date,
        s.token_address,
        CASE
            WHEN s.event_type = 'Supply' THEN
                toInt256(
                    intDiv(
                        s.amount_raw * toUInt256OrZero('1000000000000000000000000000'),
                        r.liquidity_index
                    )
                )
            WHEN s.event_type IN ('Withdraw', 'LiquidationWithdraw') THEN
                -toInt256(
                    intDiv(
                        s.amount_raw * toUInt256OrZero('1000000000000000000000000000')
                            + r.liquidity_index - toUInt256OrZero('1'),
                        r.liquidity_index
                    )
                )
            ELSE toInt256(0)
        END AS scaled_delta
    FROM supply_events_ordered s
    INNER JOIN reserve_index_by_tx r
        ON r.transaction_hash = s.transaction_hash
       AND r.token_address    = s.token_address
       AND r.event_order      = s.event_order
    WHERE r.liquidity_index > toUInt256OrZero('0')
),

borrow_scaled AS (
    SELECT
        b.date,
        b.token_address,
        CASE
            WHEN b.event_type = 'Borrow' THEN
                toInt256(
                    intDiv(
                        b.amount_raw * toUInt256OrZero('1000000000000000000000000000'),
                        r.variable_borrow_index
                    )
                )
            WHEN b.event_type IN ('Repay', 'LiquidationRepay') THEN
                -toInt256(
                    intDiv(
                        b.amount_raw * toUInt256OrZero('1000000000000000000000000000')
                            + r.variable_borrow_index - toUInt256OrZero('1'),
                        r.variable_borrow_index
                    )
                )
            ELSE toInt256(0)
        END AS scaled_delta
    FROM borrow_events_ordered b
    INNER JOIN reserve_index_by_tx r
        ON r.transaction_hash = b.transaction_hash
       AND r.token_address    = b.token_address
       AND r.event_order      = b.event_order
    WHERE r.variable_borrow_index > toUInt256OrZero('0')
),

supply_daily AS (
    SELECT date, token_address, sum(scaled_delta) AS delta_supply
    FROM supply_scaled
    GROUP BY date, token_address
),

borrow_daily AS (
    SELECT date, token_address, sum(scaled_delta) AS delta_borrow
    FROM borrow_scaled
    GROUP BY date, token_address
),

deltas AS (
    SELECT
        coalesce(s.date, b.date) AS date,
        coalesce(s.token_address, b.token_address) AS token_address,
        coalesce(s.delta_supply, toInt256(0)) AS delta_supply,
        coalesce(b.delta_borrow, toInt256(0)) AS delta_borrow
    FROM supply_daily s
    FULL OUTER JOIN borrow_daily b
        ON b.date = s.date
       AND b.token_address = s.token_address
),

{% if is_incremental() %}
prev_cumulative AS (
    SELECT
        token_address,
        argMax(cumulative_scaled_supply, date) AS prev_supply,
        argMax(cumulative_scaled_borrow, date) AS prev_borrow
    FROM {{ this }}
    GROUP BY token_address
),
{% endif %}

with_cumulative AS (
    SELECT
        d.date,
        d.token_address,
        sum(d.delta_supply) OVER (
            PARTITION BY d.token_address ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.prev_supply, toInt256(0))
        {% endif %}
        AS cumulative_scaled_supply,
        sum(d.delta_borrow) OVER (
            PARTITION BY d.token_address ORDER BY d.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        {% if is_incremental() %}
            + coalesce(p.prev_borrow, toInt256(0))
        {% endif %}
        AS cumulative_scaled_borrow
    FROM deltas d
    {% if is_incremental() %}
    LEFT JOIN prev_cumulative p
        ON p.token_address = d.token_address
    {% endif %}
),

-- End-of-day indices sourced directly from raw events in UInt256 so the final utilization
-- ratio stays in exact arithmetic end-to-end (bypasses the Float64 storage in
-- int_execution_lending_aave_daily).
daily_index AS (
    SELECT
        toDate(block_timestamp) AS date,
        lower(decoded_params['reserve']) AS token_address,
        argMax(
            toUInt256OrZero(decoded_params['liquidityIndex']),
            (block_timestamp, log_index)
        ) AS liquidity_index_eod,
        argMax(
            toUInt256OrZero(decoded_params['variableBorrowIndex']),
            (block_timestamp, log_index)
        ) AS variable_borrow_index_eod
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityIndex']      IS NOT NULL
      AND decoded_params['variableBorrowIndex'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
    GROUP BY date, token_address
)

SELECT
    c.date,
    c.token_address,
    c.cumulative_scaled_supply,
    c.cumulative_scaled_borrow,
    CASE
        WHEN c.cumulative_scaled_supply > toInt256(0)
             AND i.liquidity_index_eod        > toUInt256OrZero('0')
             AND i.variable_borrow_index_eod  > toUInt256OrZero('0')
        THEN
            -- utilization = (borrow_scaled * borrow_index) / (supply_scaled * liquidity_index) * 100
            -- Both numerator and denominator fit comfortably in UInt256 (max ~1e56 vs 1e77).
            toFloat64(
                toUInt256(c.cumulative_scaled_borrow) * i.variable_borrow_index_eod
            )
            / toFloat64(
                toUInt256(c.cumulative_scaled_supply) * i.liquidity_index_eod
            ) * 100
        ELSE NULL
    END AS utilization_rate
FROM with_cumulative c
LEFT JOIN daily_index i
    ON i.token_address = c.token_address
   AND i.date = c.date
ORDER BY c.date, c.token_address
