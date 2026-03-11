{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'yields', 'pools']
    )
}}

{#-
  Shared pool metadata for Uniswap V3 and Swapr V3 pools on Gnosis Chain.
  Maps each pool address to its protocol and token0/token1 addresses.
  Referenced by multiple yields models to avoid duplicating factory-event queries.
-#}

SELECT DISTINCT
    'Uniswap V3' AS protocol,
    lower(decoded_params['pool']) AS pool_address,
    replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
    lower(decoded_params['token0']) AS token0_address,
    lower(decoded_params['token1']) AS token1_address
FROM {{ ref('contracts_UniswapV3_Factory_events') }}
WHERE event_name = 'PoolCreated'
  AND decoded_params['pool'] IS NOT NULL
  AND decoded_params['token0'] IS NOT NULL
  AND decoded_params['token1'] IS NOT NULL

UNION ALL

SELECT DISTINCT
    'Swapr V3' AS protocol,
    lower(decoded_params['pool']) AS pool_address,
    replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
    lower(decoded_params['token0']) AS token0_address,
    lower(decoded_params['token1']) AS token1_address
FROM {{ ref('contracts_Swapr_v3_AlgebraFactory_events') }}
WHERE event_name = 'Pool'
  AND decoded_params['pool'] IS NOT NULL
  AND decoded_params['token0'] IS NOT NULL
  AND decoded_params['token1'] IS NOT NULL
