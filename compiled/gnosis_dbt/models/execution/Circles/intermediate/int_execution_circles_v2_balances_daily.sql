





    
        
    
        
    
        
    
        
    
        
            
        
    
        
    
        
    
        
    

WITH deltas AS (
    SELECT
        date,
        account,
        token_address,
        argMax(circles_type, last_activity_ts) AS circles_type,
        argMax(delta_raw, last_activity_ts) AS net_delta_raw,
        max(last_activity_ts) AS last_activity_ts_for_day
    FROM `dbt`.`int_execution_circles_v2_balance_diffs_daily`
    WHERE date < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
      FROM `dbt`.`int_execution_circles_v2_balances_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(date) >= (
      SELECT 
        
          addDays(max(toDate(x2.date)), -1)
        

      FROM `dbt`.`int_execution_circles_v2_balances_daily` AS x2
      WHERE 1=1 
    )
  

      
    GROUP BY 1, 2, 3
),
overall_max_date AS (
    SELECT
        
            yesterday()
         AS max_date
),

current_partition AS (
    SELECT
        max(date) AS max_date
    FROM `dbt`.`int_execution_circles_v2_balances_daily`
    WHERE date < yesterday()
),
prev_balances AS (
    SELECT
        account,
        token_address,
        balance_raw,
        
        circles_type,
        
        last_activity_ts
    FROM `dbt`.`int_execution_circles_v2_balances_daily`
    WHERE date = (SELECT max_date FROM current_partition)
),
keys AS (
    SELECT DISTINCT account, token_address
    FROM (
        SELECT account, token_address FROM prev_balances
        UNION ALL
        SELECT account, token_address FROM deltas
    )
),
calendar AS (
    SELECT
        k.account,
        k.token_address,
        addDays(cp.max_date + 1, offset) AS date
    FROM keys k
    CROSS JOIN current_partition cp
    CROSS JOIN overall_max_date o
    ARRAY JOIN range(dateDiff('day', cp.max_date, o.max_date)) AS offset
),

balances AS (
    SELECT
        c.date AS date,
        c.account AS account,
        c.token_address AS token_address,
        sum(coalesce(d.net_delta_raw, toInt256(0))) OVER (
            PARTITION BY c.account, c.token_address
            ORDER BY c.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        
            + coalesce(p.balance_raw, toInt256(0))
         AS balance_raw,
        toUInt8(
            greatest(
                max(coalesce(d.circles_type, toUInt8(0))) OVER (
                    PARTITION BY c.account, c.token_address
                    ORDER BY c.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                
                    coalesce(p.circles_type, toUInt8(0))
                
            )
        ) AS circles_type,
        toUInt64(
            greatest(
                max(coalesce(d.last_activity_ts_for_day, toUInt64(0))) OVER (
                    PARTITION BY c.account, c.token_address
                    ORDER BY c.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                
                    coalesce(p.last_activity_ts, toUInt64(0))
                
            )
        ) AS last_activity_ts
    FROM calendar c
    LEFT JOIN deltas d
      ON c.date = d.date
     AND c.account = d.account
     AND c.token_address = d.token_address
    
    LEFT JOIN prev_balances p
      ON c.account = p.account
     AND c.token_address = p.token_address
    
),
snapshots AS (
    SELECT
        date,
        account,
        token_address,
        balance_raw,
        circles_type,
        last_activity_ts,
        toUInt64(
            if(
                date = today(),
                (
    SELECT coalesce(max(timestamp), toUInt32(toUnixTimestamp(now())))
    FROM `execution`.`blocks`
),
                toUnixTimestamp(addDays(toDateTime(date), 1)) - 1
            )
        ) AS snapshot_ts
    FROM balances
    WHERE balance_raw != 0
)

SELECT
    date,
    account,
    token_address,
    balance_raw,
    circles_type,
    last_activity_ts,
    snapshot_ts,
    if(
        circles_type = toUInt8(1),
        toInt256(
            multiplyDecimal(
                toDecimal256(balance_raw, 0),
                
toDecimal256(
  pow(
    toDecimal256('0.9998013320085989574306481700129226782902039065082930593676448873', 64),
    intDiv(snapshot_ts - 1602720000, 86400)
    - intDiv(1602720000 - 1602720000, 86400)
  ),
  18
),
                0
            )
        ),
        toInt256(
            multiplyDecimal(
                toDecimal256(balance_raw, 0),
                
toDecimal256(
  pow(
    toDecimal256('0.9998013320085989574306481700129226782902039065082930593676448873', 64),
    intDiv(snapshot_ts - 1602720000, 86400)
    - intDiv(last_activity_ts - 1602720000, 86400)
  ),
  18
),
                0
            )
        )
    ) AS demurraged_balance_raw
FROM snapshots