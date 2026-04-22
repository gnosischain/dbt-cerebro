{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='pool_id',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'balancer_v2', 'staging']
    )
}}

SELECT DISTINCT
    lower(decoded_params['poolId'])                                          AS pool_id,
    concat('0x', replaceAll(lower(decoded_params['poolAddress']), '0x', '')) AS pool_address
FROM {{ ref('contracts_BalancerV2_Vault_events') }}
WHERE event_name = 'PoolRegistered'
  AND decoded_params['poolId']      IS NOT NULL
  AND decoded_params['poolAddress'] IS NOT NULL
