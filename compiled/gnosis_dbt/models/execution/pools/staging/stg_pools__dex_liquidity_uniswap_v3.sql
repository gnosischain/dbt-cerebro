




SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Uniswap V3'                                    AS protocol,
    r.pool_address                                  AS pool_address,
    lower(decoded_params['owner'])                  AS provider,
    multiIf(
        e.event_name = 'Mint', 'mint',
        e.event_name = 'Burn', 'burn',
        'collect'
    )                                               AS event_type,
    r.token0_address                                AS token_address,
    abs(toInt256OrNull(decoded_params['amount0']))  AS amount_raw,
    toInt32OrNull(decoded_params['tickLower'])       AS tick_lower,
    toInt32OrNull(decoded_params['tickUpper'])       AS tick_upper,
    multiIf(
        e.event_name = 'Mint',  toInt256OrNull(decoded_params['amount']),
        e.event_name = 'Burn', -toInt256OrNull(decoded_params['amount']),
        NULL
    )                                               AS liquidity_delta
FROM `dbt`.`contracts_UniswapV3_Pool_events` e
INNER JOIN (
    SELECT pool_address, pool_address_no0x, token0_address, token1_address
    FROM `dbt`.`stg_pools__v3_pool_registry`
    WHERE protocol = 'Uniswap V3'
) r ON r.pool_address_no0x = e.contract_address
WHERE e.event_name IN ('Mint', 'Burn', 'Collect')
  AND e.block_timestamp < today()
  AND decoded_params['amount0'] IS NOT NULL
  

UNION ALL

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Uniswap V3'                                    AS protocol,
    r.pool_address                                  AS pool_address,
    lower(decoded_params['owner'])                  AS provider,
    multiIf(
        e.event_name = 'Mint', 'mint',
        e.event_name = 'Burn', 'burn',
        'collect'
    )                                               AS event_type,
    r.token1_address                                AS token_address,
    abs(toInt256OrNull(decoded_params['amount1']))  AS amount_raw,
    toInt32OrNull(decoded_params['tickLower'])       AS tick_lower,
    toInt32OrNull(decoded_params['tickUpper'])       AS tick_upper,
    multiIf(
        e.event_name = 'Mint',  toInt256OrNull(decoded_params['amount']),
        e.event_name = 'Burn', -toInt256OrNull(decoded_params['amount']),
        NULL
    )                                               AS liquidity_delta
FROM `dbt`.`contracts_UniswapV3_Pool_events` e
INNER JOIN (
    SELECT pool_address, pool_address_no0x, token0_address, token1_address
    FROM `dbt`.`stg_pools__v3_pool_registry`
    WHERE protocol = 'Uniswap V3'
) r ON r.pool_address_no0x = e.contract_address
WHERE e.event_name IN ('Mint', 'Burn', 'Collect')
  AND e.block_timestamp < today()
  AND decoded_params['amount1'] IS NOT NULL
  