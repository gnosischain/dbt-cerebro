{{
  config(
    materialized='view',
    tags=['production','execution','cow','tier1',
          'api:cow_trades_ts','granularity:daily']
  )
}}

SELECT
    date,
    num_trades AS value
FROM {{ ref('fct_execution_cow_daily') }}
WHERE date < today()
ORDER BY date
