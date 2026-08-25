{{
  config(
    materialized='view',
    tags=['production','governance','tier2','api:governance_forum_activity','granularity:weekly']
  )
}}

SELECT date, metric, value
FROM {{ ref('int_governance_forum_activity_weekly') }}
ORDER BY date, metric
