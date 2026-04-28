{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_pages_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "page_path",
            "column": "page_path",
            "operator": "=",
            "type": "string",
            "description": "Filter by page path"
          },
          {
            "name": "current_domain",
            "column": "current_domain",
            "operator": "=",
            "type": "string",
            "description": "Filter by domain"
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
    current_domain,
    page_path,
    event_count,
    unique_users
FROM {{ ref('int_mixpanel_ga_pages_daily') }}
ORDER BY date, event_count DESC
