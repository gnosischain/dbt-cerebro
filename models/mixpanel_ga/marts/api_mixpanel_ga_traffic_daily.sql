{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_traffic_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "referrer_domain",
            "column": "referrer_domain",
            "operator": "=",
            "type": "string",
            "description": "Filter by referring domain"
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
    referrer_domain,
    initial_referrer_domain,
    event_count,
    unique_users
FROM {{ ref('int_mixpanel_ga_traffic_daily') }}
ORDER BY date, event_count DESC
