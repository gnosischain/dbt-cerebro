

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`fct_execution_gnosis_app_users_daily`) AS as_of_date
FROM (
WITH days AS (
    SELECT date, active_users
    FROM `dbt`.`fct_execution_gnosis_app_users_daily`
    WHERE date < today()
),
ranked AS (
    SELECT date, active_users,
           row_number() OVER (ORDER BY date DESC) AS rn
    FROM days
)
SELECT
    anyIf(active_users, rn = 1)                                                  AS value,
    round((anyIf(active_users, rn = 1) - anyIf(active_users, rn = 2))
          / nullIf(anyIf(active_users, rn = 2), 0) * 100, 1)                     AS change_pct
FROM ranked
) AS sub