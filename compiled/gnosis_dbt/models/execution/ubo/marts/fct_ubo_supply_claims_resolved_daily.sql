




WITH

kc AS (
    SELECT DISTINCT date, container_address, token_address
    FROM `dbt`.`fct_ubo_known_containers_daily`
),

-- Direct holders: pass through unchanged, stripping rows where ubo_address
-- is itself a known container for the bridge token.
clean AS (
    SELECT f.*
    FROM `dbt`.`fct_ubo_supply_claims_daily` f
    LEFT JOIN kc
        ON  f.date              = kc.date
        AND f.ubo_address       = kc.container_address
        AND f.container_address = kc.token_address
    WHERE kc.container_address IS NULL
      AND f.date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(f.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_resolved_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(f.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`fct_ubo_supply_claims_resolved_daily` AS x2
        WHERE 1=1 
      )
    
  

      
),

-- Second-level redistribution: stream the full claims table through on the
-- left (probe side) and join the tiny pre-materialized second-level rows on
-- the right (hash-table side). All column references are fully qualified —
-- no bare names in the CTE SELECT scope — which avoids the ClickHouse
-- UNKNOWN_IDENTIFIER error that fires when both join sides share column names
-- and the outer SELECT of a CTE tries to resolve bare references.
redistributed AS (
    SELECT
        sub.date                                                                        AS date,
        s.protocol                                                                      AS protocol,
        s.container_address                                                             AS container_address,
        s.token_address                                                                 AS token_address,
        s.symbol                                                                        AS symbol,
        s.token_class                                                                   AS token_class,
        sub.ubo_address                                                                 AS ubo_address,
        toInt256(round(
            sub.balance
            / nullIf(sum(sub.balance) OVER (PARTITION BY sub.date, sub.container_address, sub.token_address), 0)
            * toFloat64(s.balance_raw)
        ))                                                                              AS balance_raw,
        sub.balance
        / nullIf(sum(sub.balance) OVER (PARTITION BY sub.date, sub.container_address, sub.token_address), 0)
        * s.balance                                                                     AS balance,
        sub.balance
        / nullIf(sum(sub.balance) OVER (PARTITION BY sub.date, sub.container_address, sub.token_address), 0)
        * s.balance_usd                                                                 AS balance_usd
    FROM `dbt`.`fct_ubo_supply_claims_daily` sub
    INNER JOIN `dbt`.`int_ubo_second_level_daily` s
        ON  sub.date              = s.date
        AND sub.container_address = s.ubo_address
        AND sub.token_address     = s.container_address
    WHERE sub.date < today()
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(sub.date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_ubo_supply_claims_resolved_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(sub.date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`fct_ubo_supply_claims_resolved_daily` AS x2
        WHERE 1=1 
      )
    
  

      
)

SELECT
    date,
    protocol,
    container_address,
    token_address,
    any(symbol)                AS symbol,
    any(token_class)           AS token_class,
    ubo_address,
    toInt256(sum(balance_raw)) AS balance_raw,
    sum(balance)               AS balance,
    sum(balance_usd)           AS balance_usd
FROM (
    SELECT * FROM clean
    UNION ALL
    SELECT * FROM redistributed
)
GROUP BY date, protocol, container_address, token_address, ubo_address
SETTINGS max_bytes_before_external_group_by = 3000000000