

-- int_consensus_validators_balances_daily is already REAL GNO
-- (the mGNO->GNO /32 happens at the int layer) — pass through unscaled.
SELECT
    date
    ,label
    ,value
FROM (
    SELECT
        date
        ,'balance' AS label
        ,balance AS value
    FROM `dbt`.`int_consensus_validators_balances_daily`

    UNION ALL

    SELECT
        date
        ,'eff. balance' AS label
        ,effective_balance AS value
    FROM `dbt`.`int_consensus_validators_balances_daily`
)
ORDER BY date, label