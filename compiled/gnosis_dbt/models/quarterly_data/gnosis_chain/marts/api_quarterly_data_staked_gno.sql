

SELECT
    toStartOfQuarter(date) AS quarter,
    round(argMax(effective_balance, date), 1) AS staked_gno
FROM `dbt`.`int_consensus_validators_balances_daily`
GROUP BY quarter
ORDER BY quarter