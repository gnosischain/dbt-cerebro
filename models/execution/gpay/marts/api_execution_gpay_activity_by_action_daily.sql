{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_activity_by_action_daily','granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "action",
            "column": "action",
            "operator": "=",
            "type": "string",
            "description": "Action type (Payment, Cashback, etc.)"
          },
          {
            "name": "start_date",
            "column": "date",
            "operator": ">=",
            "type": "date",
            "description": "Inclusive start date"
          },
          {
            "name": "end_date",
            "column": "date",
            "operator": "<=",
            "type": "date",
            "description": "Inclusive end date"
          }
        ],
        "sort": [
          {"column": "date", "direction": "DESC"}
        ]
      }
    }
  )
}}

SELECT
    date,
    action,
    sum(activity_count)                   AS activity_count,
    round(toFloat64(sum(volume_usd)), 2)  AS volume_usd,
    round(toFloat64(sum(volume)), 6)      AS volume_native
FROM {{ ref('fct_execution_gpay_actions_by_token_daily') }}
GROUP BY action, date
ORDER BY date, action
