

SELECT
    date,
    q_balance[1] AS q05,
    q_balance[2] AS q10,
    q_balance[3] AS q25,
    q_balance[4] AS q50,
    q_balance[5] AS q75,
    q_balance[6] AS q90,
    q_balance[7] AS q95
FROM (
    SELECT
        toStartOfDay(slot_timestamp) AS date,
       quantilesTDigest(-- quantilesExactExclusive(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(balance/POWER(10,9)) AS q_balance
    FROM `dbt`.`stg_consensus__validators`
    WHERE 
        status = 'active_ongoing'
        AND
        slot_timestamp < today()
        
  

    GROUP BY date
)