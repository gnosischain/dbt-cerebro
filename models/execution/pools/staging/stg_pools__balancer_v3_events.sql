{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'balancer_v3', 'staging']
    )
}}

WITH vault_events AS (
    SELECT
        block_timestamp,
        transaction_hash,
        log_index,
        event_name,
        decoded_params
    FROM {{ ref('contracts_BalancerV3_Vault_events') }}
    WHERE event_name IN ('LiquidityAdded', 'LiquidityRemoved', 'Swap', 'Wrap', 'Unwrap', 'LiquidityAddedToBuffer')
      AND block_timestamp < today()
),

liquidity_added AS (
    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'LiquidityAdded' AS event_type,
        toString(arrayJoin(arrayEnumerate(JSONExtractArrayRaw(ifNull(decoded_params['amountsAddedRaw'], '[]')))) - 1) AS token_index,
        toInt256OrNull(JSONExtractString(amount_val, '')) AS delta_amount_raw
    FROM vault_events
    ARRAY JOIN 
        JSONExtractArrayRaw(ifNull(decoded_params['amountsAddedRaw'], '[]')) AS amount_val
    WHERE event_name = 'LiquidityAdded'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['amountsAddedRaw'] IS NOT NULL
),

liquidity_removed AS (
    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'LiquidityRemoved' AS event_type,
        toString(arrayJoin(arrayEnumerate(JSONExtractArrayRaw(ifNull(decoded_params['amountsRemovedRaw'], '[]')))) - 1) AS token_index,
        -toInt256OrNull(JSONExtractString(amount_val, '')) AS delta_amount_raw
    FROM vault_events
    ARRAY JOIN 
        JSONExtractArrayRaw(ifNull(decoded_params['amountsRemovedRaw'], '[]')) AS amount_val
    WHERE event_name = 'LiquidityRemoved'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['amountsRemovedRaw'] IS NOT NULL
),

swap_events AS (
    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Swap' AS event_type,
        lower(decoded_params['tokenIn']) AS token_address,
        toInt256OrNull(decoded_params['amountIn']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'Swap'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['tokenIn'] IS NOT NULL
      AND decoded_params['amountIn'] IS NOT NULL

    UNION ALL

    SELECT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Swap' AS event_type,
        lower(decoded_params['tokenOut']) AS token_address,
        -toInt256OrNull(decoded_params['amountOut']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'Swap'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['tokenOut'] IS NOT NULL
      AND decoded_params['amountOut'] IS NOT NULL
),

wrap_events AS (
    SELECT
        lower(decoded_params['wrappedToken']) AS wrapped_token_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Wrap' AS event_type,
        NULL AS pool_address,
        toInt256OrNull(decoded_params['depositedUnderlying']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'Wrap'
      AND decoded_params['wrappedToken'] IS NOT NULL
      AND decoded_params['depositedUnderlying'] IS NOT NULL
),

unwrap_events AS (
    SELECT
        lower(decoded_params['wrappedToken']) AS wrapped_token_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'Unwrap' AS event_type,
        NULL AS pool_address,
        -toInt256OrNull(decoded_params['withdrawnUnderlying']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'Unwrap'
      AND decoded_params['wrappedToken'] IS NOT NULL
      AND decoded_params['withdrawnUnderlying'] IS NOT NULL
),

liquidity_added_to_buffer AS (
    SELECT
        lower(decoded_params['wrappedToken']) AS wrapped_token_address,
        block_timestamp,
        transaction_hash,
        log_index,
        'LiquidityAddedToBuffer' AS event_type,
        NULL AS pool_address,
        toInt256OrNull(decoded_params['amountUnderlying']) AS delta_amount_raw
    FROM vault_events
    WHERE event_name = 'LiquidityAddedToBuffer'
      AND decoded_params['wrappedToken'] IS NOT NULL
      AND decoded_params['amountUnderlying'] IS NOT NULL
)

SELECT
    pool_address,
    block_timestamp,
    transaction_hash,
    log_index,
    event_type,
    token_index,
    NULL AS wrapped_token_address,
    NULL AS token_address,
    delta_amount_raw
FROM (
    SELECT * FROM liquidity_added
    UNION ALL
    SELECT * FROM liquidity_removed
)
WHERE delta_amount_raw IS NOT NULL
  AND pool_address IS NOT NULL

UNION ALL

SELECT
    pool_address,
    block_timestamp,
    transaction_hash,
    log_index,
    event_type,
    NULL AS token_index,
    NULL AS wrapped_token_address,
    token_address,
    delta_amount_raw
FROM swap_events
WHERE delta_amount_raw IS NOT NULL
  AND pool_address IS NOT NULL
  AND token_address IS NOT NULL

UNION ALL

SELECT
    pool_address,
    block_timestamp,
    transaction_hash,
    log_index,
    event_type,
    NULL AS token_index,
    wrapped_token_address,
    NULL AS token_address,
    delta_amount_raw
FROM (
    SELECT * FROM wrap_events
    UNION ALL
    SELECT * FROM unwrap_events
    UNION ALL
    SELECT * FROM liquidity_added_to_buffer
)
WHERE delta_amount_raw IS NOT NULL
  AND wrapped_token_address IS NOT NULL

ORDER BY pool_address, block_timestamp, transaction_hash, log_index
