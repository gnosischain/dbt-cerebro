{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','ubo','claims','supply_claims']
    )
}}


SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_aave_daily') }}
WHERE date < today()

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_balancer_v2_daily') }}
WHERE date < today()

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_uniswap_v3_daily') }}
WHERE date < today()

UNION ALL

SELECT date, protocol, container_address, token_address, symbol, token_class,
       ubo_address, balance_raw, balance, balance_usd
FROM {{ ref('int_ubo_claims_swapr_v3_daily') }}
WHERE date < today()
