

-- Latest complete week's Weekly Economically Active Users (non-blacklisted only)
-- plus WoW change pct.

WITH weeks AS (
    SELECT week, cnt
    FROM `dbt`.`fct_execution_gnosis_app_weekly_economically_active_users`
    WHERE is_blacklisted = false
      AND week < toStartOfWeek(today(), 1)
),
ranked AS (
    SELECT
        week,
        cnt,
        row_number() OVER (ORDER BY week DESC) AS rn
    FROM weeks
)
SELECT
    anyIf(cnt, rn = 1)                                                   AS value,
    round((anyIf(cnt, rn = 1) - anyIf(cnt, rn = 2))
          / nullIf(anyIf(cnt, rn = 2), 0) * 100, 1)                      AS change_pct
FROM ranked