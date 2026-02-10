

-- depends_on: `dbt`.`int_execution_transfers_whitelisted_daily`








WITH base AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        symbol,
        lower("from")        AS from_address,
        lower("to")          AS to_address,
        amount_raw               AS amount_raw
    FROM `dbt`.`int_execution_transfers_whitelisted_daily`
    WHERE date < today()
      
        
  
    
    

   AND 
    toStartOfMonth(toDate(date)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_tokens_address_diffs_daily` AS x1
      WHERE 1=1 
  
  

  
  


    )
    AND toDate(date) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_tokens_address_diffs_daily` AS x2
      WHERE 1=1 
  
  

  
  


    )
  

      
      
  

      
  

),

with_class AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        coalesce(w.token_class, 'OTHER') AS token_class,
        b.from_address,
        b.to_address,
        b.amount_raw
    FROM base b
    INNER JOIN `dbt`.`tokens_whitelist` w
      ON lower(w.address) = b.token_address
     AND b.date >= toDate(w.date_start)
     AND (w.date_end IS NULL OR b.date < toDate(w.date_end))
),

deltas AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        from_address AS address,
        -amount_raw      AS delta_raw
    FROM with_class

    UNION ALL

    SELECT
        date,
        token_address,
        symbol,
        token_class,
        to_address   AS address,
        amount_raw       AS delta_raw
    FROM with_class
),

agg AS (
    SELECT
        date,
        token_address,
        symbol,
        token_class,
        lower(address) AS address,
        sum(delta_raw)     AS net_delta_raw
    FROM deltas
    GROUP BY date, token_address, symbol, token_class, address
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    address,
    net_delta_raw
FROM agg
WHERE net_delta_raw != 0