{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gnosis_app']
  )
}}


SELECT
    address,
    min(block_timestamp)                       AS first_seen_at,
    max(block_timestamp)                       AS last_seen_at,
    count()                                    AS heuristic_hits,
    groupUniqArray(heuristic_kind)             AS heuristic_kinds,
    toUInt8(length(groupUniqArray(heuristic_kind))) AS n_distinct_heuristics
FROM {{ ref('int_execution_gnosis_app_user_events') }}
GROUP BY address
