SELECT
    date
    ,effective_balance AS value
FROM `dbt`.`int_consensus_validators_balances_daily`
ORDER BY date