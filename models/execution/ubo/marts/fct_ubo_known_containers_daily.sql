{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET max_memory_usage = 6000000000",
            "SET max_bytes_before_external_group_by = 2000000000",
            "SET max_bytes_before_external_sort = 2000000000"
        ],
        post_hook=[
            "SET max_memory_usage = 0",
            "SET max_bytes_before_external_group_by = 0",
            "SET max_bytes_before_external_sort = 0"
        ],
        tags=['production','execution','ubo','known_containers']
    )
}}


SELECT DISTINCT
    date,
    container_address,
    token_address
FROM {{ ref('fct_ubo_supply_claims_daily') }}
WHERE date < today()
