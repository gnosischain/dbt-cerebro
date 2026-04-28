{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_tech_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "browser",
            "column": "browser",
            "operator": "=",
            "type": "string",
            "description": "Filter by browser name"
          },
          {
            "name": "os",
            "column": "os",
            "operator": "=",
            "type": "string",
            "description": "Filter by operating system"
          },
          {
            "name": "device_type",
            "column": "device_type",
            "operator": "=",
            "type": "string",
            "description": "Filter by device type"
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
    browser,
    os,
    device_type,
    event_count,
    unique_users
FROM {{ ref('int_mixpanel_ga_tech_daily') }}
ORDER BY date, event_count DESC
