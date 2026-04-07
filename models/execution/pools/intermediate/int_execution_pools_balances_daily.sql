{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'balances', 'intermediate']
    )
}}

{#- Model documentation in schema.yml -#}

SELECT * FROM {{ ref('int_execution_pools_uniswap_v3_daily') }}

UNION ALL

SELECT * FROM {{ ref('int_execution_pools_swapr_v3_daily') }}

UNION ALL

SELECT * FROM {{ ref('int_execution_pools_balancer_v2_daily') }}

UNION ALL

SELECT * FROM {{ ref('int_execution_pools_balancer_v3_daily') }}
