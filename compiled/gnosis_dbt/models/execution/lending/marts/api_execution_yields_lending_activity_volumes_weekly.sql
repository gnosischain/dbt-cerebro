

SELECT
    week AS date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Deposits' AS volume_type,
    deposits_volume_weekly AS value
FROM `dbt`.`fct_execution_yields_lending_weekly`
WHERE deposits_volume_weekly > 0

UNION ALL

SELECT
    week AS date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Borrows' AS volume_type,
    borrows_volume_weekly AS value
FROM `dbt`.`fct_execution_yields_lending_weekly`
WHERE borrows_volume_weekly > 0

ORDER BY date DESC, token, label, volume_type