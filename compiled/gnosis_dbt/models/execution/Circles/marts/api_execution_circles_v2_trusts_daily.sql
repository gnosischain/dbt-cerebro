

SELECT
    date,
    n_trust_events,
    n_new_trusts,
    n_revoked_trusts,
    n_distinct_trusters,
    n_distinct_trustees
FROM `dbt`.`int_execution_circles_v2_trusts_daily`
WHERE date < today()
ORDER BY date DESC