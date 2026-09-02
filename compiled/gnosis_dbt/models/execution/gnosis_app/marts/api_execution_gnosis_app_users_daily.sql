

SELECT * FROM `dbt`.`fct_execution_gnosis_app_users_daily`
WHERE date < today()
ORDER BY date