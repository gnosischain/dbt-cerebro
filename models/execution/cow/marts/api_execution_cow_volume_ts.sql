{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_volume_ts','granularity:daily']
  )
}}

SELECT
    date,
    volume_usd AS value
FROM {{ ref('fct_execution_cow_daily') }}
WHERE date < today()
ORDER BY date
