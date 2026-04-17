{{ config(materialized='view', tags=['dev', 'live', 'execution', 'pools', 'trades', 'staging']) }}

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Balancer V2'                                                            AS protocol,
    r.pool_address                                                           AS pool_address,
    lower(decoded_params['tokenOut'])                                        AS token_bought_address,
    toUInt256OrNull(decoded_params['amountOut'])                             AS amount_bought_raw,
    lower(decoded_params['tokenIn'])                                         AS token_sold_address,
    toUInt256OrNull(decoded_params['amountIn'])                              AS amount_sold_raw
FROM {{ ref('contracts_BalancerV2_Vault_events_live') }} e
LEFT JOIN {{ ref('stg_pools__balancer_v2_pool_registry') }} r
    ON r.pool_id = lower(decoded_params['poolId'])
WHERE e.event_name = 'Swap'
  AND decoded_params['tokenIn']   IS NOT NULL
  AND decoded_params['tokenOut']  IS NOT NULL
  AND decoded_params['amountIn']  IS NOT NULL
  AND decoded_params['amountOut'] IS NOT NULL
