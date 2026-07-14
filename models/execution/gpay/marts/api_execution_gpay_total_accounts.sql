{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier0','api:gpay_total_accounts','granularity:all_time']
  )
}}

SELECT
    toFloat64(count()) AS value,
    (SELECT toDate(max(date)) FROM {{ ref('int_execution_gpay_activity_daily') }}) AS as_of_date
FROM {{ ref('int_execution_gpay_accounts_deployed') }}
