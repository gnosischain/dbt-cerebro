{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:circles_v2_trusts_distribution','granularity:latest']
  )
}}

-- Distribution histogram of trust degree (given / received) across avatars,
-- bucketed (0 / 1-5 / 6-10 / 11-25 / 26-50 / 51-100 / 100+). Thin passthrough
-- over fct_execution_circles_v2_trusts_distribution.

SELECT
    direction,
    trust_bucket,
    avatar_count
FROM {{ ref('fct_execution_circles_v2_trusts_distribution') }}
ORDER BY direction, trust_bucket
