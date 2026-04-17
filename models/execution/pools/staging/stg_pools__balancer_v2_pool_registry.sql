{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='pool_id',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'balancer_v2', 'staging']
    )
}}

{#-
    Mapping of Balancer V2 poolId (bytes32) → pool contract address,
    built from PoolRegistered events emitted once per pool at creation.
    Materialized as a small table so live views don't scan the full
    historical Vault events table on every query.
-#}

SELECT DISTINCT
    lower(decoded_params['poolId'])                                          AS pool_id,
    concat('0x', replaceAll(lower(decoded_params['poolAddress']), '0x', '')) AS pool_address
FROM {{ ref('contracts_BalancerV2_Vault_events') }}
WHERE event_name = 'PoolRegistered'
  AND decoded_params['poolId']      IS NOT NULL
  AND decoded_params['poolAddress'] IS NOT NULL
