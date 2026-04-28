{{
  config(
    materialized='view',
    tags=['production', 'mixpanel_ga', 'tier3', 'api:mixpanel_modals_daily', 'granularity:daily'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {
            "name": "bottom_sheet",
            "column": "bottom_sheet",
            "operator": "=",
            "type": "string",
            "description": "Filter by modal/bottom-sheet component name (e.g. PasskeyStepsModal, ModalJoin, SelectSwapAsset)"
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

SELECT * FROM {{ ref('int_mixpanel_ga_modals_daily') }}
