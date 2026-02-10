

-- Production version: Aggregates transaction-level data to daily

WITH base AS (
    SELECT
        toDate(timestamp) AS date, 
        bridge,
        source_chain,
        dest_chain,
        token,
        direction,
        sum(amount_token) AS volume_token,
        sum(amount_usd) AS volume_usd,
        sum(net_usd) AS net_usd,
        count() AS txs 
    FROM `dbt`.`stg_crawlers_data__dune_bridge_flows`
    WHERE timestamp < today()
    
  
    
    

   AND 
    toStartOfMonth(toDate(timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_bridges_flows_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_bridges_flows_daily` AS x2
      WHERE 1=1 
    )
  
 
    GROUP BY date, bridge, source_chain, dest_chain, token, direction
)

SELECT * FROM base