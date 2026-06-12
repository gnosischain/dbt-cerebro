

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM (
SELECT 'native' AS unit, value
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'CashbackGNO' AND window = 'All'

UNION ALL

SELECT 'usd' AS unit, value
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'CashbackVolume' AND window = 'All'
) AS sub