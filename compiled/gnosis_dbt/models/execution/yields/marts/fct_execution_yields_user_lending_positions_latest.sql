WITH

latest_date AS (
    SELECT max(date) AS max_date
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
    WHERE date < today()
),

latest_balances AS (
    SELECT
        b.user_address,
        b.reserve_address,
        b.symbol,
        b.balance,
        b.balance_usd
    FROM `dbt`.`int_execution_lending_aave_user_balances_daily` b
    CROSS JOIN latest_date d
    WHERE b.date = d.max_date
      AND b.balance_usd > 0.01
),

lending_apy AS (
    SELECT
        token      AS symbol,
        yield_apy  AS supply_apy
    FROM `dbt`.`fct_execution_yields_opportunities_latest`
    WHERE type = 'Lending'
)

SELECT
    lb.user_address,
    lb.reserve_address,
    lb.symbol,
    round(lb.balance, 6)                         AS balance,
    round(lb.balance_usd, 2)                     AS balance_usd,
    round(la.supply_apy, 4)                      AS supply_apy,
    'Aave V3'                                     AS protocol
FROM latest_balances lb
LEFT JOIN lending_apy la
    ON la.symbol = lb.symbol