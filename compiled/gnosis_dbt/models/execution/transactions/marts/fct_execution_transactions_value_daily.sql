SELECT
    date
    ,transaction_type
    ,success
    ,xdai_value 
    ,xdai_value_avg 
    ,xdai_value_median
FROM `dbt`.`int_execution_transactions_info_daily`
WHERE date < today()