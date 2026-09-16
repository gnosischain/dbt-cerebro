











WITH scalars AS (
    SELECT
        s.snapshot_date AS date,
        s.token_address AS token_address,
        max(toInt256(s.scalar_raw)) AS total_supply_raw
    FROM `dbt`.`stg_rpc_state_indexer__token_scalars_published` AS s
    WHERE s.chain_id = 100
      AND s.job_name = 'daily_curated_balances'
      AND s.scalar_name = 'totalSupply'
      AND s.snapshot_date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(s.snapshot_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_rpc_state_indexer_token_supply_daily` AS x1
        WHERE 1=1 
      )
      
    
  

      
    GROUP BY date, token_address
),

balances AS (
    SELECT
        b.snapshot_date AS date,
        b.token_address AS token_address,
        sumIf(toInt256(b.balance_raw), b.holder_address != '0x0000000000000000000000000000000000000000') AS held_raw,
        sumIf(toInt256(b.balance_raw), b.holder_address = '0x0000000000000000000000000000000000000000') AS burned_raw,
        countIf(b.balance_raw > 0 AND b.holder_address != '0x0000000000000000000000000000000000000000') AS holders
    FROM `dbt`.`stg_rpc_state_indexer__token_balances_published` AS b
    WHERE b.chain_id = 100
      AND b.job_name = 'daily_curated_balances'
      AND b.snapshot_date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(b.snapshot_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_rpc_state_indexer_token_supply_daily` AS x1
        WHERE 1=1 
      )
      
    
  

      
    GROUP BY date, token_address
)

SELECT
    s.date AS date,
    s.token_address AS token_address,
    w.symbol AS symbol,
    w.token_class AS token_class,
    (s.total_supply_raw - coalesce(b.burned_raw, toInt256(0))) / POWER(10, w.decimals) AS supply_total,
    b.held_raw / POWER(10, w.decimals) AS supply_holders,
    b.holders AS holders
FROM scalars AS s
LEFT JOIN balances AS b
    ON b.date = s.date
   AND b.token_address = s.token_address
INNER JOIN `dbt`.`tokens_whitelist` AS w
    ON lower(w.address) = s.token_address
   AND s.date >= toDate(w.date_start)
   AND (w.date_end IS NULL OR s.date < toDate(w.date_end))