











WITH census AS (
    SELECT
        b.snapshot_date AS date,
        b.token_address AS token_address,
        b.holder_address AS address,
        toInt256(b.balance_raw) AS balance_raw
    FROM `dbt`.`stg_rpc_state_indexer__token_balances_published` AS b
    WHERE b.chain_id = 100
      AND b.job_name = 'daily_curated_balances'
      AND b.balance_raw > 0
      AND b.holder_address != '0x0000000000000000000000000000000000000000'
      AND b.snapshot_date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(b.snapshot_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_rpc_state_indexer_token_balances_daily` AS x1
        WHERE 1=1 
      )
      
    
  

      
)

SELECT
    c.date AS date,
    c.token_address AS token_address,
    w.symbol AS symbol,
    w.token_class AS token_class,
    c.address AS address,
    c.balance_raw AS balance_raw,
    c.balance_raw / POWER(10, w.decimals) AS balance
FROM census AS c
INNER JOIN `dbt`.`tokens_whitelist` AS w
    ON lower(w.address) = c.token_address
   AND c.date >= toDate(w.date_start)
   AND (w.date_end IS NULL OR c.date < toDate(w.date_end))