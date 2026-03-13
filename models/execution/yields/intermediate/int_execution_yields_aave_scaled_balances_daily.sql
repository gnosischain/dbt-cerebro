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

reserve_index_by_tx AS (
    SELECT
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        any(toFloat64(toUInt256OrNull(decoded_params['liquidityIndex']))) AS liquidity_index,
        any(toFloat64(toUInt256OrNull(decoded_params['variableBorrowIndex']))) AS variable_borrow_index
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
    GROUP BY transaction_hash, lower(decoded_params['reserve'])
),

supply_events AS (
    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrNull(decoded_params['amount']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name IN ('Supply', 'Withdraw')
      AND decoded_params['reserve'] IS NOT NULL
      AND decoded_params['amount'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}

    UNION ALL

    SELECT
        toStartOfDay(block_timestamp) AS date,
        transaction_hash,
        lower(decoded_params['collateralAsset']) AS token_address,
        'LiquidationWithdraw' AS event_type,
        toUInt256OrNull(decoded_params['liquidatedCollateralAmount']) AS amount_raw
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
        lower(decoded_params['reserve']) AS token_address,
        event_name AS event_type,
        toUInt256OrNull(decoded_params['amount']) AS amount_raw
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
        lower(decoded_params['debtAsset']) AS token_address,
        'LiquidationRepay' AS event_type,
        toUInt256OrNull(decoded_params['debtToCover']) AS amount_raw
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'LiquidationCall'
      AND decoded_params['debtAsset'] IS NOT NULL
      AND decoded_params['debtToCover'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
),

supply_scaled AS (
    SELECT
        s.date,
        s.token_address,
        CASE
            WHEN s.event_type = 'Supply' THEN toFloat64(s.amount_raw) * 1e27 / r.liquidity_index
            WHEN s.event_type IN ('Withdraw', 'LiquidationWithdraw') THEN -toFloat64(s.amount_raw) * 1e27 / r.liquidity_index
            ELSE 0
        END AS scaled_delta
    FROM supply_events s
    INNER JOIN reserve_index_by_tx r
        ON r.transaction_hash = s.transaction_hash
       AND r.token_address = s.token_address
    WHERE r.liquidity_index IS NOT NULL
      AND r.liquidity_index > 0
),

borrow_scaled AS (
    SELECT
        b.date,
        b.token_address,
        CASE
            WHEN b.event_type = 'Borrow' THEN toFloat64(b.amount_raw) * 1e27 / r.variable_borrow_index
            WHEN b.event_type IN ('Repay', 'LiquidationRepay') THEN -toFloat64(b.amount_raw) * 1e27 / r.variable_borrow_index
            ELSE 0
        END AS scaled_delta
    FROM borrow_events b
    INNER JOIN reserve_index_by_tx r
        ON r.transaction_hash = b.transaction_hash
       AND r.token_address = b.token_address
    WHERE r.variable_borrow_index IS NOT NULL
      AND r.variable_borrow_index > 0
),

supply_daily AS (
    SELECT date, token_address, sum(scaled_delta) AS net_scaled_supply_change_daily
    FROM supply_scaled
    GROUP BY date, token_address
),

borrow_daily AS (
    SELECT date, token_address, sum(scaled_delta) AS net_scaled_borrow_change_daily
    FROM borrow_scaled
    GROUP BY date, token_address
)

SELECT
    coalesce(s.date, b.date) AS date,
    coalesce(s.token_address, b.token_address) AS token_address,
    coalesce(s.net_scaled_supply_change_daily, 0) AS net_scaled_supply_change_daily,
    coalesce(b.net_scaled_borrow_change_daily, 0) AS net_scaled_borrow_change_daily
FROM supply_daily s
FULL OUTER JOIN borrow_daily b
    ON b.date = s.date
   AND b.token_address = s.token_address
ORDER BY date, token_address
