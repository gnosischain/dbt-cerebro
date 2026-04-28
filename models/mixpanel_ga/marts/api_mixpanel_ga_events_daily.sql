{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_events_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "event_name",
            "column": "event_name",
            "operator": "=",
            "type": "string",
            "description": "Filter by event name"
          },
          {
            "name": "event_category",
            "column": "event_category",
            "operator": "=",
            "type": "string",
            "description": "Filter by event category (pageview, modal, login, feature, navigation, action, system, other)"
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
    event_name,
    event_category,
    event_count,
    unique_users,
    unique_devices,
    autocapture_ratio
FROM {{ ref('int_mixpanel_ga_events_daily') }}
ORDER BY date, event_name
