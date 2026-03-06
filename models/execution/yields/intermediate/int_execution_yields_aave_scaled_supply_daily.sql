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

liquidity_index_by_tx AS (
    SELECT
        transaction_hash,
        lower(decoded_params['reserve']) AS token_address,
        any(toFloat64(toUInt256OrNull(decoded_params['liquidityIndex']))) AS liquidity_index
    FROM {{ ref('contracts_aaveV3_PoolInstance_events') }}
    WHERE event_name = 'ReserveDataUpdated'
      AND decoded_params['liquidityIndex'] IS NOT NULL
      AND block_timestamp < today()
      {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
    GROUP BY transaction_hash, lower(decoded_params['reserve'])
),

activity_scaled AS (
    SELECT
        s.date,
        s.token_address,
        s.event_type,
        toFloat64(s.amount_raw) * 1e27 / r.liquidity_index AS scaled_amount
    FROM supply_events s
    INNER JOIN liquidity_index_by_tx r
        ON r.transaction_hash = s.transaction_hash
       AND r.token_address = s.token_address
    WHERE r.liquidity_index IS NOT NULL
      AND r.liquidity_index > 0
)

SELECT
    date,
    token_address,
    sum(CASE
        WHEN event_type = 'Supply' THEN scaled_amount
        WHEN event_type IN ('Withdraw', 'LiquidationWithdraw') THEN -scaled_amount
        ELSE 0
    END) AS net_scaled_supply_change_daily
FROM activity_scaled
GROUP BY date, token_address
ORDER BY date, token_address
