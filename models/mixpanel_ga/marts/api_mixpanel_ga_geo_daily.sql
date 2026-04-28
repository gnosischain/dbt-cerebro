{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_geo_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "country_code",
            "column": "country_code",
            "operator": "=",
            "type": "string",
            "description": "ISO 2-letter country code"
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

-- Rolled up to country level (no region) for privacy
SELECT
    date,
    country_code,
    sum(event_count)                AS event_count,
    sum(unique_users)               AS unique_users,
    sum(unique_devices)             AS unique_devices
FROM {{ ref('int_mixpanel_ga_geo_daily') }}
GROUP BY date, country_code
ORDER BY date, event_count DESC
