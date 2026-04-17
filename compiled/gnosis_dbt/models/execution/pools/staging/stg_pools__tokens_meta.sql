

SELECT
    lower(address) AS token_address,
    nullIf(trimBoth(symbol), '') AS token,
    decimals,
    date_start,
    date_end
FROM `dbt`.`tokens_whitelist`