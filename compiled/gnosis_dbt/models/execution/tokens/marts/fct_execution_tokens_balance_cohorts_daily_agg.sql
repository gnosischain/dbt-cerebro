

SELECT
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket,
    sum(holders_in_bucket)   AS holders_in_bucket,
    sum(value_usd_in_bucket) AS value_usd_in_bucket
FROM `dbt`.`fct_execution_tokens_balance_cohorts_daily`   -- sharded fact
WHERE date < today()
GROUP BY
    date,
    token_address,
    symbol,
    token_class,
    balance_bucket