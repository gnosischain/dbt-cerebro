{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(direction, trust_bucket)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'trusts']
    )
}}

WITH latest AS (
    SELECT max(day) AS max_day
    FROM {{ ref('fct_execution_circles_v2_avatar_trusts_daily') }}
    WHERE day < today()
),
latest_trusts AS (
    SELECT avatar, trusts_given_count, trusts_received_count
    FROM {{ ref('fct_execution_circles_v2_avatar_trusts_daily') }}
    WHERE day = (SELECT max_day FROM latest)
),
given_bucketed AS (
    SELECT
        'given' AS direction,
        multiIf(
            trusts_given_count = 0,   '0',
            trusts_given_count <= 5,  '1-5',
            trusts_given_count <= 10, '6-10',
            trusts_given_count <= 25, '11-25',
            trusts_given_count <= 50, '26-50',
            trusts_given_count <= 100,'51-100',
                                      '100+'
        ) AS trust_bucket,
        count() AS avatar_count
    FROM latest_trusts
    GROUP BY trust_bucket
),
received_bucketed AS (
    SELECT
        'received' AS direction,
        multiIf(
            trusts_received_count = 0,   '0',
            trusts_received_count <= 5,  '1-5',
            trusts_received_count <= 10, '6-10',
            trusts_received_count <= 25, '11-25',
            trusts_received_count <= 50, '26-50',
            trusts_received_count <= 100,'51-100',
                                          '100+'
        ) AS trust_bucket,
        count() AS avatar_count
    FROM latest_trusts
    GROUP BY trust_bucket
)

SELECT * FROM given_bucketed
UNION ALL
SELECT * FROM received_bucketed
ORDER BY direction, trust_bucket
