{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='date',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'supply_daily']
    )
}}

SELECT
    date,
    sum(supply_raw) AS total_supply_raw,
    sum(supply) AS total_supply,
    sum(demurraged_supply) AS total_demurraged_supply,
    count() AS token_count
FROM {{ ref('fct_execution_circles_v2_tokens_supply_daily') }}
GROUP BY date
ORDER BY date
