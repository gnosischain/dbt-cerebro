

WITH

-- ── Scope: Any ─────────────────────────────────────────────────────────

any_months AS (
    SELECT DISTINCT
        address,
        toStartOfMonth(date) AS month
    FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
    WHERE activity_kind != 'onboard'
      AND toStartOfMonth(date) < toStartOfMonth(today())
),

any_first_month AS (
    SELECT address, min(month) AS first_month FROM any_months GROUP BY address
),

any_classified AS (
    SELECT
        wm.address                                                               AS address,
        wm.month                                                                 AS month,
        CASE
            WHEN wm.month = fm.first_month                THEN 'new'
            WHEN prev.address IS NOT NULL                 THEN 'retained'
            ELSE                                                'returning'
        END                                                                      AS segment
    FROM any_months wm
    INNER JOIN any_first_month fm ON fm.address = wm.address
    LEFT JOIN any_months prev
        ON prev.address = wm.address
       AND prev.month   = subtractMonths(wm.month, 1)
),

any_segments AS (
    SELECT
        month,
        countIf(segment = 'new')       AS new_users,
        countIf(segment = 'retained')  AS retained_users,
        countIf(segment = 'returning') AS returning_users,
        count()                        AS total_active
    FROM any_classified
    GROUP BY month
),

any_churned AS (
    SELECT
        curr.month,
        count() AS churned_users
    FROM any_months curr
    LEFT JOIN any_months nxt
        ON nxt.address = curr.address
       AND nxt.month   = addMonths(curr.month, 1)
    WHERE nxt.address IS NULL
      AND curr.month < (SELECT max(month) FROM any_months)
    GROUP BY curr.month
),

any_result AS (
    SELECT
        'Any'                                                                         AS scope,
        s.month                                                                       AS month,
        s.new_users                                                                   AS new_users,
        s.retained_users                                                              AS retained_users,
        s.returning_users                                                             AS returning_users,
        coalesce(c.churned_users, 0)                                                  AS churned_users,
        s.total_active                                                                AS total_active,
        round(coalesce(c.churned_users, 0) / greatest(
            lagInFrame(s.total_active, 1) OVER (ORDER BY s.month), 1
        ) * 100, 1)                                                                   AS churn_rate,
        round(s.retained_users / greatest(
            lagInFrame(s.total_active, 1) OVER (ORDER BY s.month), 1
        ) * 100, 1)                                                                   AS retention_rate
    FROM any_segments s
    LEFT JOIN any_churned c ON c.month = s.month
),

-- ── Scope: Swap ────────────────────────────────────────────────────────

swap_months AS (
    SELECT DISTINCT
        address,
        toStartOfMonth(date) AS month
    FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
    WHERE activity_kind IN ('swap_signed','swap_filled')
      AND toStartOfMonth(date) < toStartOfMonth(today())
),

swap_first_month AS (
    SELECT address, min(month) AS first_month FROM swap_months GROUP BY address
),

swap_classified AS (
    SELECT
        wm.address                                                               AS address,
        wm.month                                                                 AS month,
        CASE
            WHEN wm.month = fm.first_month                THEN 'new'
            WHEN prev.address IS NOT NULL                 THEN 'retained'
            ELSE                                                'returning'
        END                                                                      AS segment
    FROM swap_months wm
    INNER JOIN swap_first_month fm ON fm.address = wm.address
    LEFT JOIN swap_months prev
        ON prev.address = wm.address
       AND prev.month   = subtractMonths(wm.month, 1)
),

swap_segments AS (
    SELECT
        month,
        countIf(segment = 'new')       AS new_users,
        countIf(segment = 'retained')  AS retained_users,
        countIf(segment = 'returning') AS returning_users,
        count()                        AS total_active
    FROM swap_classified
    GROUP BY month
),

swap_churned AS (
    SELECT
        curr.month,
        count() AS churned_users
    FROM swap_months curr
    LEFT JOIN swap_months nxt
        ON nxt.address = curr.address
       AND nxt.month   = addMonths(curr.month, 1)
    WHERE nxt.address IS NULL
      AND curr.month < (SELECT max(month) FROM swap_months)
    GROUP BY curr.month
),

swap_result AS (
    SELECT
        'Swap'                                                                        AS scope,
        s.month                                                                       AS month,
        s.new_users                                                                   AS new_users,
        s.retained_users                                                              AS retained_users,
        s.returning_users                                                             AS returning_users,
        coalesce(c.churned_users, 0)                                                  AS churned_users,
        s.total_active                                                                AS total_active,
        round(coalesce(c.churned_users, 0) / greatest(
            lagInFrame(s.total_active, 1) OVER (ORDER BY s.month), 1
        ) * 100, 1)                                                                   AS churn_rate,
        round(s.retained_users / greatest(
            lagInFrame(s.total_active, 1) OVER (ORDER BY s.month), 1
        ) * 100, 1)                                                                   AS retention_rate
    FROM swap_segments s
    LEFT JOIN swap_churned c ON c.month = s.month
)

SELECT * FROM any_result
UNION ALL
SELECT * FROM swap_result
ORDER BY scope, month