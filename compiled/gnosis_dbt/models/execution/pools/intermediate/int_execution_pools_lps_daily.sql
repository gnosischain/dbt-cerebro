




WITH

uniswap_v3_lp_events AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Uniswap V3' AS protocol,
        concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
        event_name,
        lower(decoded_params['owner']) AS lp_address
    FROM `dbt`.`contracts_UniswapV3_Pool_events`
    WHERE event_name IN ('Mint', 'Burn')
      AND decoded_params['owner'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_pools_lps_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_pools_lps_daily` AS x2
        WHERE 1=1 
      )
    
  

      
),

swapr_v3_lp_events AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Swapr V3' AS protocol,
        concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
        event_name,
        lower(decoded_params['owner']) AS lp_address
    FROM `dbt`.`contracts_Swapr_v3_AlgebraPool_events`
    WHERE event_name IN ('Mint', 'Burn')
      AND decoded_params['owner'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_pools_lps_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_pools_lps_daily` AS x2
        WHERE 1=1 
      )
    
  

      
),

balancer_v3_lp_events_raw AS (
    SELECT
        block_timestamp,
        event_name AS raw_event_name,
        decoded_params['pool'] AS pool_param,
        decoded_params['liquidityProvider'] AS lp_param
    FROM `dbt`.`contracts_BalancerV3_Vault_events`
    WHERE event_name IN ('LiquidityAdded', 'LiquidityRemoved')
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['liquidityProvider'] IS NOT NULL
      AND block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_pools_lps_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_pools_lps_daily` AS x2
        WHERE 1=1 
      )
    
  

      
),

balancer_v3_lp_events AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Balancer V3' AS protocol,
        concat('0x', replaceAll(lower(pool_param), '0x', '')) AS pool_address,
        multiIf(
            raw_event_name = 'LiquidityAdded', 'Mint',
            raw_event_name = 'LiquidityRemoved', 'Burn',
            raw_event_name
        ) AS event_name,
        lower(lp_param) AS lp_address
    FROM balancer_v3_lp_events_raw
),

all_lp_events AS (
    SELECT * FROM uniswap_v3_lp_events
    UNION ALL
    SELECT * FROM swapr_v3_lp_events
    UNION ALL
    SELECT * FROM balancer_v3_lp_events
)

SELECT
    date,
    protocol,
    pool_address,
    countIf(event_name = 'Mint') AS mint_count,
    countIf(event_name = 'Burn') AS burn_count,
    uniqExactIf(lp_address, event_name = 'Mint') AS lps_minting_daily,
    uniqExactIf(lp_address, event_name = 'Burn') AS lps_burning_daily,
    groupBitmapState(cityHash64(lp_address)) AS lps_bitmap_state
FROM all_lp_events
WHERE lp_address IS NOT NULL
  AND lp_address != ''
GROUP BY date, protocol, pool_address