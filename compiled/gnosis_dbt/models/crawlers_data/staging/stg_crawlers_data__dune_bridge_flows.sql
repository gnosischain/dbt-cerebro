

SELECT
  timestamp,
  bridge,
  source_chain,
  dest_chain,
  token,
  toFloat64(amount_token) AS amount_token,
  toFloat64(amount_usd)   AS amount_usd,
  toFloat64(net_usd)      AS net_usd,         
  CASE
    WHEN dest_chain   = 'gnosis' THEN 'in'
    WHEN source_chain = 'gnosis' THEN 'out'
    ELSE 'xchain'
  END AS direction
FROM `crawlers_data`.`dune_bridge_flows`