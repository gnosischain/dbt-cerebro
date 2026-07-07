{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'tier2', 'api:gnosis_app_gt_wallet_cohort_retention', 'granularity:total'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "sort": [{"column": "cohort_month", "direction": "DESC"}, {"column": "month_index", "direction": "ASC"}]
      }
    }
) }}

-- Public acquisition-cohort retention matrix (retained wallets / cohort size at
-- month_index N). Point-in-time snapshot (as_of_date).
SELECT *, today() AS as_of_date
FROM {{ ref('fct_execution_gnosis_app_gt_wallet_cohort_retention_monthly') }}
ORDER BY cohort_month DESC, month_index ASC
