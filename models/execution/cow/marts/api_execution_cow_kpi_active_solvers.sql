{{
  config(
    materialized='view',
    tags=['production','execution','cow','kpi','tier0',
          'api:cow_kpi_active_solvers']
  )
}}

SELECT countIf(is_active) AS value
FROM {{ ref('fct_execution_cow_solvers') }}
