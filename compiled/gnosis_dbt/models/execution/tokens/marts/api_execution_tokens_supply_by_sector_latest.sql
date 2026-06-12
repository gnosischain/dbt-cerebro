

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_tokens_metrics_daily`) AS as_of_date
FROM (
SELECT
    token_class,
    sector AS label,
    value,
    value_usd,
    percentage
FROM `dbt`.`fct_execution_tokens_supply_by_sector_latest`
ORDER BY token_class, value_usd DESC
) AS sub