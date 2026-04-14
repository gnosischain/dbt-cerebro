




SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Swapr V3'                                                               AS protocol,
    r.pool_address                                                           AS pool_address,
    CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
        THEN r.token0_address ELSE r.token1_address
    END                                                                      AS token_bought_address,
    abs(CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
        THEN toInt256OrNull(decoded_params['amount0'])
        ELSE toInt256OrNull(decoded_params['amount1'])
    END)                                                                     AS amount_bought_raw,
    CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
        THEN r.token1_address ELSE r.token0_address
    END                                                                      AS token_sold_address,
    abs(CASE WHEN toInt256OrNull(decoded_params['amount0']) < 0
        THEN toInt256OrNull(decoded_params['amount1'])
        ELSE toInt256OrNull(decoded_params['amount0'])
    END)                                                                     AS amount_sold_raw,
    lower(decoded_params['recipient'])                                       AS taker
FROM `dbt`.`contracts_Swapr_v3_AlgebraPool_events` e
INNER JOIN (
    SELECT pool_address, pool_address_no0x, token0_address, token1_address
    FROM `dbt`.`stg_pools__v3_pool_registry`
    WHERE protocol = 'Swapr V3'
) r ON r.pool_address_no0x = e.contract_address
WHERE e.event_name = 'Swap'
  AND e.block_timestamp < today()
  AND decoded_params['amount0'] IS NOT NULL
  AND decoded_params['amount1'] IS NOT NULL
  