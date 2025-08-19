SELECT
    date
    ,transaction_type
    ,gas_used
    ,gas_price_avg
    ,gas_price_median
FROM `dbt`.`int_execution_transactions_info_daily`
WHERE success = 1
ORDER BY date, transaction_type