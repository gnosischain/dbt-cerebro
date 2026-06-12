

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_transactions_info_daily`) AS as_of_date
FROM (
SELECT
    transaction_type
    ,SUM(n_txs) AS value
FROM `dbt`.`int_execution_transactions_info_daily`
WHERE success = 1
GROUP BY transaction_type
ORDER BY transaction_type
) AS sub