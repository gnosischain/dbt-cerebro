{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'tier2', 'api:gnosis_app_gt_referrals', 'granularity:total'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "sort": [{"column": "n_edges", "direction": "DESC"}]
      }
    }
) }}

SELECT *, today() AS as_of_date FROM {{ ref('fct_execution_gnosis_app_gt_referrals') }}
