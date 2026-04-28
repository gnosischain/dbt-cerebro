{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_linked_entities', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["root_address"],
        "parameters": [
          {"name": "root_address", "column": "root_address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 20},
          {"name": "relation", "column": "relation", "operator": "=", "type": "string"},
          {"name": "entity_type", "column": "entity_type", "operator": "=", "type": "string"}
        ],
        "pagination": {"enabled": true, "default_limit": 100, "max_limit": 5000, "response": "envelope"},
        "sort": [{"column": "last_seen_at", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
  root_address,
  entity_type,
  entity_id,
  entity_address,
  relation,
  display_label,
  value_count,
  last_seen_at
FROM {{ ref('fct_execution_account_linked_entities_latest') }}

