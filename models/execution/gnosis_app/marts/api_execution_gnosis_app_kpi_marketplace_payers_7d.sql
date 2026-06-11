{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_marketplace_payers','granularity:last_7d']
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }}) AS as_of_date
FROM (
SELECT
    countDistinct(payer)                             AS value,
    CAST(NULL AS Nullable(Float64))                  AS change_pct
FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }}
WHERE toDate(block_timestamp) >= today() - INTERVAL 7 DAY
  AND toDate(block_timestamp) < today()
) AS sub
