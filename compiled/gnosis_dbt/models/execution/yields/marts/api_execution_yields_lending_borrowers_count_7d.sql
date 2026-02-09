

SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_yields_lending_latest`
WHERE label = 'Borrowers' AND window = '7D'
ORDER BY token