{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'balancer_v2', 'staging']
    )
}}

WITH vault_events AS (
    SELECT
        block_timestamp,
        transaction_hash,
        log_index,
        event_name,
        decoded_params
    FROM {{ ref('contracts_BalancerV2_Vault_events') }}
    WHERE event_name IN ('PoolBalanceChanged', 'PoolBalanceManaged', 'Swap', 'FlashLoan')
      AND block_timestamp < today()
),

pool_balance_changed AS (
    SELECT
        lower(decoded_params['poolId']) AS pool_id,
        block_timestamp,
        transaction_hash,
        log_index,
        'PoolBalanceChanged' AS event_type,
        lower(JSONExtractString(token_val, '')) AS token_address,
        toInt256OrNull(JSONExtractString(delta_val, '')) AS delta_amount_raw
    FROM vault_events
    ARRAY JOIN 
        JSONExtractArrayRaw(ifNull(decoded_params['tokens'], '[]')) AS token_val,
        JSONExtractArrayRaw(ifNull(decoded_params['deltas'], '[]')) AS delta_val
    WHERE event_name = 'PoolBalanceChanged'
      AND decoded_params['poolId'] IS NOT NULL
      AND decoded_params['tokens'] IS NOT NULL
      AND decoded_params['deltas'] IS NOT NULL
),

pool_balance_managed AS (
    SELECT
        lower(decoded_params['poolId']) AS pool_id,
        block_timestamp,
        transaction_hash,
        log_index,
        'PoolBalanceManaged' AS event_type,
        lower(decoded_params['token']) AS token_address,
        -- cashDelta + managedDelta = net change
        toInt256OrNull(decoded_params['cashDelta']) + toInt256OrNull(decoded_params['managedDelta']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'PoolBalanceManaged'
      AND decoded_params['poolId'] IS NOT NULL
      AND decoded_params['token'] IS NOT NULL
      AND (decoded_params['cashDelta'] IS NOT NULL OR decoded_params['managedDelta'] IS NOT NULL)
),

swap_events AS (
    SELECT
        lower(decoded_params['poolId']) AS pool_id,
        block_timestamp,
        transaction_hash,
        log_index,
        'Swap' AS event_type,
        lower(decoded_params['tokenIn']) AS token_address,
        toInt256OrNull(decoded_params['amountIn']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'Swap'
      AND decoded_params['poolId'] IS NOT NULL
      AND decoded_params['tokenIn'] IS NOT NULL
      AND decoded_params['amountIn'] IS NOT NULL

    UNION ALL

    SELECT
        lower(decoded_params['poolId']) AS pool_id,
        block_timestamp,
        transaction_hash,
        log_index,
        'Swap' AS event_type,
        lower(decoded_params['tokenOut']) AS token_address,
        -toInt256OrNull(decoded_params['amountOut']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'Swap'
      AND decoded_params['poolId'] IS NOT NULL
      AND decoded_params['tokenOut'] IS NOT NULL
      AND decoded_params['amountOut'] IS NOT NULL
)

SELECT
    pool_id,
    block_timestamp,
    transaction_hash,
    log_index,
    event_type,
    token_address,
    delta_amount_raw
FROM (
    SELECT * FROM pool_balance_changed
    UNION ALL
    SELECT * FROM pool_balance_managed
    UNION ALL
    SELECT * FROM swap_events
)
WHERE delta_amount_raw IS NOT NULL
  AND token_address IS NOT NULL
  AND pool_id IS NOT NULL
ORDER BY pool_id, block_timestamp, transaction_hash, log_index
