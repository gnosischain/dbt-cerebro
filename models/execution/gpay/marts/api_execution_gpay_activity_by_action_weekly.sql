{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_activity_by_action_weekly','granularity:weekly']
  )
}}

SELECT
    week,
    action,
    sum(activity_count)                   AS activity_count,
    round(toFloat64(sum(volume_usd)), 2)  AS volume_usd,
    round(toFloat64(sum(volume)), 6)      AS volume_native
FROM {{ ref('fct_execution_gpay_actions_by_token_weekly') }}
GROUP BY action, week
ORDER BY week, action
