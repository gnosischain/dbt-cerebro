{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'staging']
    )
}}

SELECT
    concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
    'Uniswap V3'                                                 AS protocol,
    argMax(toInt32OrNull(decoded_params['tick']), block_timestamp) AS current_tick
FROM {{ ref('contracts_UniswapV3_Pool_events') }}
WHERE event_name = 'Swap'
  AND decoded_params['tick'] IS NOT NULL
  AND block_timestamp < today()
GROUP BY pool_address

UNION ALL

SELECT
    concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
    'Swapr V3'                                                   AS protocol,
    argMax(toInt32OrNull(decoded_params['tick']), block_timestamp) AS current_tick
FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
WHERE event_name = 'Swap'
  AND decoded_params['tick'] IS NOT NULL
  AND block_timestamp < today()
GROUP BY pool_address
