{{ config(materialized='view') }}

{#- Model documentation in schema.yml -#}

WITH unique_pool_tokens AS (
    SELECT DISTINCT
        pool_address,
        token_address
    FROM {{ ref('stg_pools__balancer_v3_events') }}
    WHERE event_type = 'Swap'
      AND token_address IS NOT NULL
      AND token_address != ''
)

SELECT
    pool_address,
    token_address,
    toUInt64(ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY token_address) - 1) AS token_index
FROM unique_pool_tokens