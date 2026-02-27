{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_impact_monthly','granularity:monthly']
  )
}}

SELECT *
FROM {{ ref('fct_execution_gpay_cashback_impact_monthly') }}
