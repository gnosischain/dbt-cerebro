


SELECT
    date
    ,total_amount
    ,cnt
    ,q_amount[1] AS min
    ,q_amount[2] AS q05
    ,q_amount[3] AS q10
    ,q_amount[4] AS q25
    ,q_amount[5] AS q50
    ,q_amount[6] AS q75
    ,q_amount[7] AS q90
    ,q_amount[8] AS q95
    ,q_amount[9] AS max
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date
        ,SUM(amount/POWER(10,9)) AS total_amount
        ,COUNT(*) AS cnt
        ,quantilesTDigest(
            0.0, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 1
        )(amount/POWER(10,9)) AS q_amount
    FROM `dbt`.`stg_consensus__withdrawals`
    WHERE
        slot_timestamp < today()
        
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(slot_timestamp)) >= (
      SELECT
        max(toStartOfMonth(date))
      FROM `dbt`.`int_consensus_withdrawls_dist_daily`
    )
  

    GROUP BY 1
)