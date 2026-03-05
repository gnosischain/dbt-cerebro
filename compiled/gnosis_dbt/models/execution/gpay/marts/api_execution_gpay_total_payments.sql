

SELECT value
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'PaymentCount' AND window = 'All'