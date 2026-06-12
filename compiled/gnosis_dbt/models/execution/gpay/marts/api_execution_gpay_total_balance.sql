

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM (
SELECT value
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'TotalBalance' AND window = 'All'
) AS sub