{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'gnosis_app', 'tier1', 'api:gnosis_app_user_profile', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["address"],
        "parameters": [
          {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20}
        ],
        "pagination": {"enabled": true, "default_limit": 20, "max_limit": 200, "response": "envelope"}
      }
    }
  )
}}

SELECT
  address,
  first_seen_at,
  last_seen_at,
  heuristic_hits,
  heuristic_kinds,
  n_distinct_heuristics,
  controlled_gpay_wallet,
  is_currently_ga_owned,
  n_ga_owners_current,
  n_total_owners_current,
  onboarding_class
FROM {{ ref('fct_execution_gnosis_app_user_profile_latest') }}
