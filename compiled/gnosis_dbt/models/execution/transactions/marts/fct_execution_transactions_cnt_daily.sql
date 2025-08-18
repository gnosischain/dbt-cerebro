SELECT
    date
    ,transaction_type
    ,success
    ,n_txs
FROM `dbt`.`int_execution_transactions_info_daily`
WHERE date < today()