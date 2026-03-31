

SELECT
    date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Lending APY' AS apy_type,
    apy_daily AS value
FROM `dbt`.`int_execution_yields_aave_daily`
WHERE apy_daily IS NOT NULL

UNION ALL

SELECT
    date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Borrow APY' AS apy_type,
    borrow_apy_variable_daily AS value
FROM `dbt`.`int_execution_yields_aave_daily`
WHERE borrow_apy_variable_daily IS NOT NULL

ORDER BY date DESC, token, label, apy_type