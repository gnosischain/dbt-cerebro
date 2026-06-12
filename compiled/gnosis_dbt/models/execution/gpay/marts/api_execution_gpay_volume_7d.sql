

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM (
SELECT value, change_pct
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'PaymentVolume' AND window = '7D'
) AS sub