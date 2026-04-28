{{
  config(
    materialized='view',
    tags=['production', 'execution', 'accounts', 'portfolio', 'tier1', 'api:account_counterparty_graph', 'granularity:latest'],
    meta={
      "api": {
        "methods": ["GET", "POST"],
        "allow_unfiltered": false,
        "require_any_of": ["source"],
        "parameters": [
          {"name": "source", "column": "source", "operator": "=", "type": "string", "case": "lower"},
          {"name": "edge_type", "column": "edge_type", "operator": "=", "type": "string"}
        ],
        "pagination": {"enabled": true, "default_limit": 60, "max_limit": 250, "response": "envelope"},
        "sort": [{"column": "weight", "direction": "DESC"}]
      }
    }
  )
}}

SELECT
  source,
  target,
  source AS source_name,
  target AS target_name,
  edge_type,
  weight,
  raw_volume,
  last_seen_date
FROM {{ ref('fct_execution_account_counterparty_edges_latest') }}

