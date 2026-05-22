{{ config(materialized='view', tags=['live', 'execution', 'pools', 'trades', 'staging']) }}

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Balancer V3'                                                                    AS protocol,
    concat('0x', replaceAll(lower(decoded_params['pool']), '0x', ''))               AS pool_address,
    coalesce(wm_out.underlying_address, lower(decoded_params['tokenOut']))             AS token_bought_address,
    toUInt256OrNull(decoded_params['amountOut'])                                     AS amount_bought_raw,
    coalesce(wm_in.underlying_address, lower(decoded_params['tokenIn']))             AS token_sold_address,
    toUInt256OrNull(decoded_params['amountIn'])                                      AS amount_sold_raw
FROM {{ ref('contracts_BalancerV3_Vault_events_live') }} e
LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm_out
    ON wm_out.wrapper_address = lower(decoded_params['tokenOut'])
LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm_in
    ON wm_in.wrapper_address = lower(decoded_params['tokenIn'])
WHERE e.event_name = 'Swap'
  AND decoded_params['tokenIn']   IS NOT NULL
  AND decoded_params['tokenOut']  IS NOT NULL
  AND decoded_params['amountIn']  IS NOT NULL
  AND decoded_params['amountOut'] IS NOT NULL
