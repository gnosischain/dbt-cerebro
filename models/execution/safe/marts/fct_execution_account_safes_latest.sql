{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(owner_address, safe_address)',
        unique_key='(owner_address, safe_address)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
          "SET max_threads = 1",
          "SET max_block_size = 8192",
          "SET max_memory_usage = 10000000000",
          "SET join_algorithm = 'grace_hash'",
          "SET grace_hash_join_initial_buckets = 256",
          "SET max_bytes_in_join = 500000000",
          "SET max_bytes_before_external_sort = 100000000"
        ],
        post_hook=[
          "SET max_threads = 0",
          "SET max_block_size = 65505",
          "SET max_memory_usage = 0",
          "SET join_algorithm = 'default'",
          "SET max_bytes_in_join = 0",
          "SET max_bytes_before_external_sort = 0"
        ],
        tags=['production', 'execution', 'safe', 'accounts', 'portfolio', 'fct:account_safes', 'granularity:latest']
    )
}}

WITH owners AS (
    SELECT
        safe_address,
        count() AS current_owner_count,
        any(current_threshold) AS current_threshold
    FROM {{ ref('int_execution_safes_current_owners') }}
    GROUP BY safe_address
)

SELECT
    lower(co.owner) AS owner_address,
    lower(co.safe_address) AS safe_address,
    co.became_owner_at AS became_owner_at,
    o.current_threshold AS current_threshold,
    o.current_owner_count AS current_owner_count,
    s.creation_version AS creation_version,
    s.block_date AS deployment_date
FROM {{ ref('int_execution_safes_current_owners') }} AS co
LEFT JOIN owners AS o ON o.safe_address = co.safe_address
LEFT JOIN {{ ref('int_execution_safes') }} AS s ON s.safe_address = co.safe_address
WHERE co.owner IS NOT NULL
  AND co.safe_address IS NOT NULL
