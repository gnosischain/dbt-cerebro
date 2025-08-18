SELECT
    date
    ,'withdrawls' AS label
    ,cnt
    ,total_amount
FROM 
    {{ ref('int_consensus_withdrawls_dist_daily') }}

UNION ALL

SELECT
    date
    ,'deposits' AS label
    ,cnt
    ,total_amount
FROM 
    {{ ref('int_consensus_deposits_daily') }}
