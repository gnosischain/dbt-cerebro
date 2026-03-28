

SELECT
    token,
    value,
    change_pct
FROM `dbt`.`fct_execution_yields_lending_latest`
WHERE label = 'Lenders' AND window = '7D' AND token = 'ALL'