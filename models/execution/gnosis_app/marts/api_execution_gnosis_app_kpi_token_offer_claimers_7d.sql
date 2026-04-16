{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_token_offer_claimers','granularity:last_7d']
  )
}}

SELECT
    countDistinct(ga_user)                             AS value,
    CAST(NULL AS Nullable(Float64))                    AS change_pct
FROM {{ ref('int_execution_gnosis_app_token_offer_claims') }}
WHERE toDate(block_timestamp) >= today() - INTERVAL 7 DAY
  AND toDate(block_timestamp) < today()
