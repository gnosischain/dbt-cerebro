{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_gp_wallets_onboarded','granularity:snapshot']
  )
}}

SELECT
    max(n_ga_wallets_cumulative)                      AS value,
    CAST(NULL AS Nullable(Float64))                   AS change_pct
FROM {{ ref('fct_execution_gnosis_app_gpay_wallets_daily') }}
WHERE onboarding_class = 'onboarded_via_ga'
  AND date < today()
