{{
  config(
    materialized='view',
    tags=['production','execution','circles_v2','scores','api:circles_v2_group_score_distribution','granularity:latest','tier1']
  )
}}

-- Distribution of members across score buckets, per score-based group.
-- Buckets are ordered via bucket_rank for stable chart display.
SELECT
    today() AS as_of_date,
    group_address,
    multiIf(score < 25,  '0-24',
            score < 50,  '25-49',
            score < 75,  '50-74',
            score < 100, '75-99',
            score < 150, '100-149',
                         '150+')  AS score_bucket,
    multiIf(score < 25,  1,
            score < 50,  2,
            score < 75,  3,
            score < 100, 4,
            score < 150, 5,
                         6)        AS bucket_rank,
    count()                        AS n_members
FROM {{ ref('api_execution_circles_v2_group_member_scores') }}
GROUP BY group_address, score_bucket, bucket_rank
