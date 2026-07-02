{{ config(
    materialized='view',
    tags=['production', 'execution', 'gnosis_app_gt', 'tier2', 'api:gnosis_app_gt_swaps', 'granularity:total'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "app_scope", "column": "app_scope", "operator": "=",
           "type": "string", "description": "gnosis_app | metri | third_party | unknown | test"}
        ],
        "sort": [{"column": "n_swaps", "direction": "DESC"}]
      }
    }
) }}

SELECT *, today() AS as_of_date FROM {{ ref('fct_execution_gnosis_app_gt_swaps_summary') }} ORDER BY app_scope, n_swaps DESC
