{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_total_users','granularity:snapshot']
  )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_gnosis_app_user_events') }}) AS as_of_date
FROM (
SELECT
    count(*)   AS value,
    CAST(NULL AS Nullable(Float64)) AS change_pct
FROM {{ ref('int_execution_gnosis_app_users_current') }}
) AS sub
