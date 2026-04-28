



WITH deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        account,
        token_id,
        token_address,
        sum(delta_raw) AS net_delta_raw
    FROM `dbt`.`int_execution_circles_v1_balance_diffs`
    WHERE toDate(block_timestamp) < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_execution_circles_v1_balances_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -1)
          

        FROM `dbt`.`int_execution_circles_v1_balances_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY 1, 2, 3, 4
),

prev_balances AS (
    SELECT
        account,
        token_id,
        token_address,
        argMax(balance_raw, date) AS balance_raw
    FROM `dbt`.`int_execution_circles_v1_balances_daily`
    GROUP BY 1, 2, 3
),

with_running_totals AS (
    SELECT
        d.date,
        d.account,
        d.token_id,
        d.token_address,
        d.net_delta_raw,
        sum(d.net_delta_raw) OVER (
            PARTITION BY d.account, d.token_id, d.token_address
            ORDER BY d.date
        )
        
            + coalesce(p.balance_raw, toInt256(0))
         AS balance_raw
    FROM deltas d
    
    LEFT JOIN prev_balances p
      ON d.account = p.account
     AND d.token_id = p.token_id
     AND d.token_address = p.token_address
    
)

SELECT
    date,
    account,
    token_id,
    token_address,
    balance_raw
FROM with_running_totals
WHERE balance_raw != 0