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

{#-
  Combined daily pool token balances across all supported DEX protocols.
  Thin UNION ALL of protocol-level models that each handle their own
  event processing, fee separation, incremental logic, and price enrichment.

  Protocol models:
    - int_execution_pools_uniswap_v3_daily (Uniswap V3)
    - int_execution_pools_swapr_v3_daily   (Swapr V3)
    - int_execution_pools_balancer_v2_daily (Balancer V2)
    - int_execution_pools_balancer_v3_daily (Balancer V3)
-#}

SELECT * FROM {{ ref('int_execution_pools_uniswap_v3_daily') }}

UNION ALL

SELECT * FROM {{ ref('int_execution_pools_swapr_v3_daily') }}

UNION ALL

SELECT * FROM {{ ref('int_execution_pools_balancer_v2_daily') }}

UNION ALL

SELECT * FROM {{ ref('int_execution_pools_balancer_v3_daily') }}
