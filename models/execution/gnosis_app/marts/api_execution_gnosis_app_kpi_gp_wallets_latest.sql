{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_gp_wallets','granularity:snapshot']
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_safes_module_events') }}) AS as_of_date
FROM (
SELECT
    countIf(is_currently_ga_owned)                    AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
) AS sub
