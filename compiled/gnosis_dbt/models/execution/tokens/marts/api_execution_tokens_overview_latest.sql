

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_tokens_metrics_daily`) AS as_of_date
FROM (
SELECT
    token_class,
    label,
    value,
    change_pct
FROM `dbt`.`fct_execution_tokens_overview_by_class_latest`
ORDER BY token_class, label
) AS sub