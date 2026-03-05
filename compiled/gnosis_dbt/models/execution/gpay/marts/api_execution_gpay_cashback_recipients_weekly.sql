

SELECT
    week AS date,
    recipients AS value
FROM `dbt`.`fct_execution_gpay_cashback_recipients_weekly`
ORDER BY date