{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_user_lifetime_metrics','granularity:all_time']
  )
}}

SELECT *
FROM {{ ref('fct_execution_gpay_user_lifetime_metrics') }}
