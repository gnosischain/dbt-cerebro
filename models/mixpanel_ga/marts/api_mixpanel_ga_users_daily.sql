{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_users_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
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
    user_id_hash,
    event_count,
    distinct_event_types,
    distinct_pages
FROM {{ ref('int_mixpanel_ga_users_daily') }}
ORDER BY date, event_count DESC
