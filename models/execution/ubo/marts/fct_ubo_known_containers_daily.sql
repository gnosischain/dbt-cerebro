{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','ubo','known_containers']
    )
}}


SELECT DISTINCT
    date,
    container_address,
    token_address
FROM {{ ref('fct_ubo_supply_claims_daily') }}
WHERE date < today()
