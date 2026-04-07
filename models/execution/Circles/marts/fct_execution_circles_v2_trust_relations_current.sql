{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(truster, trustee)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'trusts']
    )
}}

WITH exploded AS (
    SELECT
        truster,
        trustee,
        r.1 AS valid_from,
        r.2 AS valid_to
    FROM {{ ref('int_execution_circles_v2_trust_pair_ranges') }}
    ARRAY JOIN arrayZip(valid_from_agg, valid_to_agg) AS r
)

SELECT
    truster,
    trustee,
    valid_from,
    valid_to
FROM exploded
WHERE valid_to > now()
