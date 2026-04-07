


SELECT
    date,
    token_address,
    -balance_raw AS supply_raw,
    -balance_raw/POWER(10,18) AS supply,
    -demurraged_balance_raw/POWER(10,18) AS demurraged_supply
FROM `dbt`.`int_execution_circles_v2_balances_daily`
WHERE account = '0x0000000000000000000000000000000000000000'