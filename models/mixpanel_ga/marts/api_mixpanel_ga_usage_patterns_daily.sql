{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_usage_patterns_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "hour_of_day",
            "column": "hour_of_day",
            "operator": "=",
            "type": "number",
            "description": "Hour of day (0-23 UTC)"
          },
          {
            "name": "day_of_week",
            "column": "day_of_week",
            "operator": "=",
            "type": "number",
            "description": "ISO day of week (1=Mon, 7=Sun)"
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
    hour_of_day,
    day_of_week,
    event_count,
    unique_users
FROM {{ ref('int_mixpanel_ga_usage_patterns_daily') }}
ORDER BY date, hour_of_day, day_of_week
