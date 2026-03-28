

WITH constants AS (
    SELECT
        toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967') AS max_int256,
        toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639936') AS two_256
),

pool_events AS (
    SELECT
        replaceAll(lower(contract_address), '0x', '') AS pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        event_name,
        decoded_params
    FROM `dbt`.`contracts_Swapr_v3_AlgebraPool_events`
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
    delta_amount_raw,
    multiIf(
        event_type IN ('Mint', 'Burn'), 'liquidity',
        event_type = 'Swap' AND delta_amount_raw > toInt256(0), 'swap_in',
        event_type = 'Swap' AND delta_amount_raw <= toInt256(0), 'swap_out',
        event_type = 'Collect', 'fee_collection',
        event_type = 'Flash', 'flash_fee',
        'other'
    ) AS delta_category
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
        if(
            toUInt256OrNull(decoded_params['amount0']) > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - toUInt256OrNull(decoded_params['amount0'])),
            toInt256(toUInt256OrNull(decoded_params['amount0']))
        ) AS delta_amount_raw
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
        if(
            toUInt256OrNull(decoded_params['amount1']) > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - toUInt256OrNull(decoded_params['amount1'])),
            toInt256(toUInt256OrNull(decoded_params['amount1']))
        ) AS delta_amount_raw
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