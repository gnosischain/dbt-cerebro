

SELECT
    toStartOfQuarter(date) AS quarter,
    sum(n_txs) AS transactions
FROM `dbt`.`int_execution_transactions_info_daily`
WHERE success = 1
GROUP BY quarter
ORDER BY quarter