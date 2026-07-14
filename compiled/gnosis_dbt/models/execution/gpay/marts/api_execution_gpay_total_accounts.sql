

SELECT
    toFloat64(count()) AS value,
    (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM `dbt`.`int_execution_gpay_accounts_deployed`