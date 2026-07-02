{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'tier2', 'api:gnosis_app_gt_reconciliation', 'granularity:total'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "sort": [{"column": "registry_containment", "direction": "DESC"}]
      }
    }
) }}

SELECT *, today() AS as_of_date FROM {{ ref('int_execution_gnosis_app_gt_user_reconciliation') }}
