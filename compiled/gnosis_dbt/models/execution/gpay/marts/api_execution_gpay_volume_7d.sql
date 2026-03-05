

SELECT value, change_pct
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'PaymentVolume' AND window = '7D'