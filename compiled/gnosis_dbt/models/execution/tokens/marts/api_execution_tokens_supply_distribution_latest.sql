

SELECT
    token_class,
    token,
    value,
    value_usd,
    percentage
FROM `dbt`.`fct_execution_tokens_supply_distribution_latest`
ORDER BY token_class, value_usd DESC