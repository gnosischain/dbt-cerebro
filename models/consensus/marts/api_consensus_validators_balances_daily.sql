SELECT
    date
    ,label
    ,value
FROM (
    SELECT
        date
        ,'balance' AS label
        ,balance AS value
    FROM {{ ref('int_consensus_validators_balances_daily') }}

    UNION ALL 

    SELECT
        date
        ,'eff. balance' AS label
        ,effective_balance AS value
    FROM {{ ref('int_consensus_validators_balances_daily') }}
)
ORDER BY date, label