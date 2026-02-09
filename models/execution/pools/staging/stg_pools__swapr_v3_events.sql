{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'swapr_v3', 'staging']
    )
}}

WITH pool_events AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        event_name,
        decoded_params
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
    WHERE event_name IN ('Mint', 'Burn', 'Swap', 'Collect', 'Flash')
      AND block_timestamp < today()
)

SELECT
    pool_address,
    block_timestamp,
    transaction_hash,
    log_index,
    event_type,
    token_position,
    delta_amount_raw
FROM (
    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Mint' AS event_type,
        'token0' AS token_position,
        toInt256OrNull(decoded_params['amount0']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Mint'
      AND decoded_params['amount0'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Mint' AS event_type,
        'token1' AS token_position,
        toInt256OrNull(decoded_params['amount1']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Mint'
      AND decoded_params['amount1'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Burn' AS event_type,
        'token0' AS token_position,
        -toInt256OrNull(decoded_params['amount0']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Burn'
      AND decoded_params['amount0'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Burn' AS event_type,
        'token1' AS token_position,
        -toInt256OrNull(decoded_params['amount1']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Burn'
      AND decoded_params['amount1'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Swap' AS event_type,
        'token0' AS token_position,
        toInt256OrNull(decoded_params['amount0']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Swap'
      AND decoded_params['amount0'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Swap' AS event_type,
        'token1' AS token_position,
        toInt256OrNull(decoded_params['amount1']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Swap'
      AND decoded_params['amount1'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Collect' AS event_type,
        'token0' AS token_position,
        -toInt256OrNull(decoded_params['amount0']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Collect'
      AND decoded_params['amount0'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Collect' AS event_type,
        'token1' AS token_position,
        -toInt256OrNull(decoded_params['amount1']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Collect'
      AND decoded_params['amount1'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Flash' AS event_type,
        'token0' AS token_position,
        toInt256OrNull(decoded_params['paid0']) - toInt256OrNull(decoded_params['amount0']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Flash'
      AND decoded_params['paid0'] IS NOT NULL
      AND decoded_params['amount0'] IS NOT NULL

    UNION ALL

    SELECT
        pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Flash' AS event_type,
        'token1' AS token_position,
        toInt256OrNull(decoded_params['paid1']) - toInt256OrNull(decoded_params['amount1']) AS delta_amount_raw
    FROM pool_events
    WHERE event_name = 'Flash'
      AND decoded_params['paid1'] IS NOT NULL
      AND decoded_params['amount1'] IS NOT NULL
)
WHERE delta_amount_raw IS NOT NULL
ORDER BY pool_address, block_timestamp, transaction_hash, log_index
