{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_marketplace_buys_total','granularity:snapshot']
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }}) AS as_of_date
FROM (
SELECT
    sum(total_buys)                                    AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM {{ ref('fct_execution_gnosis_app_marketplace_offers_latest') }}
) AS sub
