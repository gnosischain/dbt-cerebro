{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_churn_rate','granularity:last_month']
  )
}}

SELECT
    anyIf(churn_rate, month = (
        SELECT max(month)
        FROM {{ ref('fct_execution_gnosis_app_churn_monthly') }}
        WHERE scope = 'Any'
    ) AND scope = 'Any')                               AS value,
    CAST(NULL AS Nullable(Float64))                    AS change_pct
FROM {{ ref('fct_execution_gnosis_app_churn_monthly') }}
