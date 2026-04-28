




WITH token_edges AS (
  SELECT
    date,
    address AS source,
    counterparty AS target,
    'token_transfer' AS edge_type,
    sum(transfer_count) AS weight,
    sum(gross_amount_raw) AS raw_volume,
    max(date) AS last_seen_date
  FROM `dbt`.`fct_execution_account_token_movements_daily`
  WHERE date < today()
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`fct_execution_account_counterparty_edges_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`fct_execution_account_counterparty_edges_daily` AS x2
        WHERE 1=1 
      )
    
  

    
  GROUP BY date, source, target, edge_type
),

gpay_edges AS (
  SELECT
    toDate(block_timestamp) AS date,
    lower(wallet_address) AS source,
    lower(counterparty) AS target,
    'gpay_activity' AS edge_type,
    count() AS weight,
    sum(abs(value_raw)) AS raw_volume,
    max(toDate(block_timestamp)) AS last_seen_date
  FROM `dbt`.`int_execution_gpay_activity`
  WHERE counterparty IS NOT NULL
    AND counterparty != ''
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`fct_execution_account_counterparty_edges_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`fct_execution_account_counterparty_edges_daily` AS x2
        WHERE 1=1 
      )
    
  

    
  GROUP BY date, source, target, edge_type
)

SELECT * FROM token_edges
UNION ALL
SELECT * FROM gpay_edges