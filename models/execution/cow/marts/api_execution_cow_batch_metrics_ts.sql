{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_batch_metrics_ts','granularity:daily']
  )
}}

SELECT
    date,
    round(cow_ratio * 100, 2)                                                   AS value
FROM {{ ref('fct_execution_cow_daily') }}
WHERE date < today()
ORDER BY date
