SELECT
    toDate(date) AS date,
    nullIf(upper(trimBoth(symbol)), '') AS token,
    toFloat64(price) AS price_usd
FROM `dbt`.`int_execution_token_prices_daily`
WHERE date < today()