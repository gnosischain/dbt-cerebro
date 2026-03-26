{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'trusts']
    )
}}

WITH unique_avatars AS (
    SELECT
        avatar,
        block_timestamp AS registered_at
    FROM {{ ref('fct_execution_circles_avatars_current') }}
),
trust_degrees AS (
    SELECT
        avatar,
        sum(in_edge) AS in_degree,
        sum(out_edge) AS out_degree
    FROM (
        SELECT trustee AS avatar, 1 AS in_edge, 0 AS out_edge
        FROM {{ ref('fct_execution_circles_trust_relations_current') }}
        WHERE version = 2

        UNION ALL

        SELECT truster AS avatar, 0 AS in_edge, 1 AS out_edge
        FROM {{ ref('fct_execution_circles_trust_relations_current') }}
        WHERE version = 2
    )
    GROUP BY 1
),
mutual_counts AS (
    SELECT
        t1.truster AS avatar,
        count() AS mutual_count
    FROM {{ ref('fct_execution_circles_trust_relations_current') }} t1
    INNER JOIN {{ ref('fct_execution_circles_trust_relations_current') }} t2
        ON t1.truster = t2.trustee
       AND t1.trustee = t2.truster
    WHERE t1.version = 2
      AND t2.version = 2
    GROUP BY 1
),
avatar_stats AS (
    SELECT
        a.avatar,
        coalesce(d.in_degree, 0) AS in_degree,
        coalesce(d.out_degree, 0) AS out_degree,
        coalesce(m.mutual_count, 0) AS mutual_count,
        greatest(0, intDiv({{ circles_chain_now_ts() }} - toUnixTimestamp(a.registered_at), 86400)) AS age_days
    FROM unique_avatars a
    LEFT JOIN trust_degrees d
        ON a.avatar = d.avatar
    LEFT JOIN mutual_counts m
        ON a.avatar = m.avatar
),
network_avg AS (
    SELECT greatest(avg(toFloat64(in_degree + out_degree)), 1.0) AS avg_degree
    FROM avatar_stats
),
scores AS (
    SELECT
        s.avatar,
        toInt32(least(100.0, greatest(
            0.0,
            least(40.0, ((toFloat64(s.in_degree) * 2.0) + toFloat64(s.out_degree)) / n.avg_degree * 20.0)
            + if(s.in_degree > 0, least(35.0, toFloat64(s.mutual_count) / toFloat64(s.in_degree) * 35.0), 0.0)
            + least(25.0, toFloat64(s.age_days) / 7.2)
        ))) AS trust_score,
        s.in_degree,
        s.out_degree,
        s.mutual_count,
        s.age_days
    FROM avatar_stats s
    CROSS JOIN network_avg n
)

SELECT
    avatar,
    trust_score,
    multiIf(
        trust_score >= 85, 'VERY_HIGH',
        trust_score >= 70, 'HIGH',
        trust_score >= 50, 'MEDIUM',
        trust_score >= 30, 'LOW',
        'VERY_LOW'
    ) AS trust_level,
    multiIf(
        in_degree > 5 AND out_degree > 3, 90,
        in_degree > 2, 60,
        30
    ) AS confidence,
    toUInt64({{ circles_chain_now_ts() }}) AS computed_at,
    in_degree,
    out_degree,
    mutual_count,
    age_days
FROM scores
