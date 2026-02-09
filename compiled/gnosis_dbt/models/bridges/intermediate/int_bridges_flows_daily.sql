

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
    toStartOfMonth(toStartOfDay(timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_bridges_flows_daily` AS x1
    )
    AND toStartOfDay(timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_bridges_flows_daily` AS x2
    )
  
 
    GROUP BY date, bridge, source_chain, dest_chain, token, direction
)

SELECT * FROM base