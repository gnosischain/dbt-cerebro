

SELECT
    rank,
    protocol,
    symbol,
    user_address,
    label,
    balance,
    balance_usd,
    pct_of_total,
    cumulative_pct,
    change_usd_7d,
    (
        SELECT max(date)
        FROM `dbt`.`int_execution_lending_aave_user_balances_daily`
        WHERE date < today()
    ) AS as_of_date
FROM `dbt`.`fct_execution_lending_top_lenders_latest`
ORDER BY protocol, symbol, rank