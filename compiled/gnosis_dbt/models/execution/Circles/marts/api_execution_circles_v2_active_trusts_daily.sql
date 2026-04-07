

SELECT
    date,
    new_trusts,
    revoked_trusts,
    active_trusts
FROM `dbt`.`fct_execution_circles_v2_active_trusts_daily`
WHERE date < today()
ORDER BY date DESC