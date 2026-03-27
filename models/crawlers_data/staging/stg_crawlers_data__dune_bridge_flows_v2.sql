{{ config(
    materialized = 'view',
    tags = ['dev', 'staging', 'dune', 'bridges', 'v2']
) }}

-- V2: Expects pre-aggregated daily data from Dune (with date, txs columns)
-- Use this version when Dune queries are updated to output daily aggregates

SELECT
  date,
  bridge,
  source_chain,
  dest_chain,
  token,
  toFloat64(amount_token) AS volume_token,   
  toFloat64(amount_usd)   AS volume_usd,
  toFloat64(net_usd)      AS net_usd,
  toUInt64(txs)           AS txs,
  CASE
    WHEN dest_chain   = 'gnosis' THEN 'in'
    WHEN source_chain = 'gnosis' THEN 'out'
    ELSE 'xchain'
  END AS direction
FROM {{ source('crawlers_data', 'dune_bridge_flows') }}
