

-- DAU of the latest day that actually has activity. The upstream fact builds a
-- dense date spine to today() and zero-fills days the activity source has not
-- reached yet (~2-day lag), so "yesterday" is often a 0 placeholder. Ranking over
-- active_users > 0 (and taking as_of_date from that ranked day) reports the real
-- latest day instead of a zero-filled one.
WITH days AS (
    SELECT date, active_users
    FROM `dbt`.`fct_execution_gnosis_app_users_daily`
    WHERE date < today() AND active_users > 0
),
ranked AS (
    SELECT date, active_users,
           row_number() OVER (ORDER BY date DESC) AS rn
    FROM days
)
SELECT
    anyIf(active_users, rn = 1)                                                  AS value,
    round((anyIf(active_users, rn = 1) - anyIf(active_users, rn = 2))
          / nullIf(anyIf(active_users, rn = 2), 0) * 100, 1)                     AS change_pct,
    anyIf(date, rn = 1)                                                          AS as_of_date
FROM ranked