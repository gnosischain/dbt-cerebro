

-- V2: Passthrough version - expects pre-aggregated daily data from staging
-- Use this version when Dune queries are updated to output daily aggregates

SELECT
    date,
    bridge,
    source_chain,
    dest_chain,
    token,
    direction,
    volume_token,
    volume_usd,
    net_usd,
    txs
FROM `dbt`.`stg_crawlers_data__dune_bridge_flows_v2`
WHERE date < today()

  
