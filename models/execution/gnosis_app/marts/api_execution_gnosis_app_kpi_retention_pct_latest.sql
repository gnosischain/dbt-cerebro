{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_retention_pct','granularity:last_month']
  )
}}

SELECT
    anyIf(retention_pct, months_since = 1
                     AND cohort_month = (
                       SELECT max(cohort_month)
                       FROM {{ ref('fct_execution_gnosis_app_retention_monthly') }}
                       WHERE months_since = 1
                     )
    )                                                  AS value,
    CAST(NULL AS Nullable(Float64))                    AS change_pct
FROM {{ ref('fct_execution_gnosis_app_retention_monthly') }}
