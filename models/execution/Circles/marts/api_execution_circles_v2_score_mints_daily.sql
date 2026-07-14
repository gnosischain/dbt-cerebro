{{
  config(
    materialized='view',
    tags=['production','execution','circles_v2','scores','api:circles_v2_score_mints','granularity:daily','tier1']
  )
}}

-- Daily score-based mint activity per group: mint count, distinct minters,
-- average member score at mint, and total group tokens minted. Excludes the
-- current incomplete day.
SELECT
    group_address             AS group_address,
    mint_date                 AS date,
    count()                   AS n_mints,
    uniqExact(avatar)         AS n_minters,
    round(avg(score), 2)      AS avg_score,
    sum(amount)               AS amount
FROM {{ ref('int_execution_circles_v2_score_mints') }}
WHERE mint_date < today()
GROUP BY group_address, mint_date
