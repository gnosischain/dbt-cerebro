SELECT
    date
    ,label
    ,cnt AS value
FROM 
    {{ ref('fct_consensus_deposits_withdrawls_daily') }}
ORDER BY date, label