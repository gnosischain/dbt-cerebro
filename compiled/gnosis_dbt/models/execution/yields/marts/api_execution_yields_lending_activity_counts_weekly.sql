

SELECT
    week AS date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Lenders' AS activity_type,
    lenders_count_weekly AS value
FROM `dbt`.`fct_execution_yields_lending_weekly`
WHERE lenders_count_weekly > 0

UNION ALL

SELECT
    week AS date,
    symbol AS token,
    token_class,
    protocol AS label,
    'Borrowers' AS activity_type,
    borrowers_count_weekly AS value
FROM `dbt`.`fct_execution_yields_lending_weekly`
WHERE borrowers_count_weekly > 0

ORDER BY date DESC, token, label, activity_type