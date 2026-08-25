{{
  config(
    materialized='view',
    tags=['production','rpc_state_indexer','tier1','granularity:daily']
  )
}}

SELECT
  label,
  block_date AS date,
  supply
FROM {{ ref('int_rpc_state_indexer_gno_supply_daily') }}
ORDER BY date, label
