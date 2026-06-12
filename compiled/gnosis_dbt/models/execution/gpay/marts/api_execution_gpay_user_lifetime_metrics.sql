

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gpay_activity_daily`) AS as_of_date
FROM (
SELECT *
FROM `dbt`.`fct_execution_gpay_user_lifetime_metrics`
) AS sub