{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_retention_pct','granularity:last_month']
  )
}}

{#
  KPI: M1 retention % for the most recent cohort that has completed at
  least one full activity month. Value = percentage of cohort users who
  were active in their cohort_month + 1.
#}

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
