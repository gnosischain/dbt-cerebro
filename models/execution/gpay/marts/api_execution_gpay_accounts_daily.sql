{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_accounts','granularity:daily']
  )
}}

SELECT
    deployed_date AS date,
    toFloat64(sum(daily_deployed) OVER (ORDER BY deployed_date)) AS value
FROM (
    SELECT
        deployed_date,
        count() AS daily_deployed
    FROM {{ ref('int_execution_gpay_accounts_deployed') }}
    GROUP BY deployed_date
)
ORDER BY date
