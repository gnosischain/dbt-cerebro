{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_total_users','granularity:snapshot']
  )
}}

SELECT
    count(*)   AS value,
    CAST(NULL AS Nullable(Float64)) AS change_pct
FROM {{ ref('int_execution_gnosis_app_users_current') }}
