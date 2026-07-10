

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_celo_gpay_activity_daily`) AS as_of_date
FROM (
SELECT value
FROM `dbt`.`fct_celo_gpay_snapshots`
WHERE label = 'PaymentUsers' AND window = 'All'
) AS sub