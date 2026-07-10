

-- Monthly new / retained / returning / churned segmentation, for two scopes:
-- Payment (card spenders) and Any (any GP activity). Mirrors
-- fct_execution_gpay_churn_monthly with safe_address as the user grain.

-- ── Scope: Payment ──────────────────────────────────────────────────────
WITH payment_safe_months AS (
    SELECT DISTINCT
        safe_address,
        toStartOfMonth(date) AS month
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action = 'Payment'
      AND toStartOfMonth(date) < toStartOfMonth(today())
),

payment_first_month AS (
    SELECT safe_address, min(month) AS first_month
    FROM payment_safe_months
    GROUP BY safe_address
),

payment_classified AS (
    SELECT
        wm.safe_address AS safe_address,
        wm.month        AS month,
        CASE
            WHEN wm.month = fm.first_month      THEN 'new'
            WHEN prev.safe_address IS NOT NULL  THEN 'retained'
            ELSE                                     'returning'
        END AS segment
    FROM payment_safe_months wm
    INNER JOIN payment_first_month fm ON fm.safe_address = wm.safe_address
    LEFT JOIN payment_safe_months prev
        ON prev.safe_address = wm.safe_address
       AND prev.month = subtractMonths(wm.month, 1)
),

payment_segments AS (
    SELECT
        month,
        countIf(segment = 'new')       AS new_users,
        countIf(segment = 'retained')  AS retained_users,
        countIf(segment = 'returning') AS returning_users,
        count()                        AS total_active
    FROM payment_classified
    GROUP BY month
),

payment_churned AS (
    SELECT
        curr.month,
        count() AS churned_users
    FROM payment_safe_months curr
    LEFT JOIN payment_safe_months nxt
        ON nxt.safe_address = curr.safe_address
       AND nxt.month = addMonths(curr.month, 1)
    WHERE nxt.safe_address IS NULL
      AND curr.month < (SELECT max(month) FROM payment_safe_months)
    GROUP BY curr.month
),

payment_result AS (
    SELECT
        'Payment' AS scope,
        s.month,
        s.new_users,
        s.retained_users,
        s.returning_users,
        coalesce(c.churned_users, 0)                                               AS churned_users,
        s.total_active,
        round(coalesce(c.churned_users, 0) / greatest(s.total_active, 1) * 100, 1) AS churn_rate,
        round(s.retained_users / greatest(
            lagInFrame(s.total_active, 1) OVER (ORDER BY s.month), 1
        ) * 100, 1)                                                                AS retention_rate
    FROM payment_segments s
    LEFT JOIN payment_churned c ON c.month = s.month
),

-- ── Scope: Any ────────────────────────────────────────────────────────────
any_safe_months AS (
    SELECT DISTINCT
        safe_address,
        toStartOfMonth(date) AS month
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
),

any_first_month AS (
    SELECT safe_address, min(month) AS first_month
    FROM any_safe_months
    GROUP BY safe_address
),

any_classified AS (
    SELECT
        wm.safe_address AS safe_address,
        wm.month        AS month,
        CASE
            WHEN wm.month = fm.first_month      THEN 'new'
            WHEN prev.safe_address IS NOT NULL  THEN 'retained'
            ELSE                                     'returning'
        END AS segment
    FROM any_safe_months wm
    INNER JOIN any_first_month fm ON fm.safe_address = wm.safe_address
    LEFT JOIN any_safe_months prev
        ON prev.safe_address = wm.safe_address
       AND prev.month = subtractMonths(wm.month, 1)
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
    FROM any_safe_months curr
    LEFT JOIN any_safe_months nxt
        ON nxt.safe_address = curr.safe_address
       AND nxt.month = addMonths(curr.month, 1)
    WHERE nxt.safe_address IS NULL
      AND curr.month < (SELECT max(month) FROM any_safe_months)
    GROUP BY curr.month
),

any_result AS (
    SELECT
        'Any' AS scope,
        s.month,
        s.new_users,
        s.retained_users,
        s.returning_users,
        coalesce(c.churned_users, 0)                                               AS churned_users,
        s.total_active,
        round(coalesce(c.churned_users, 0) / greatest(s.total_active, 1) * 100, 1) AS churn_rate,
        round(s.retained_users / greatest(
            lagInFrame(s.total_active, 1) OVER (ORDER BY s.month), 1
        ) * 100, 1)                                                                AS retention_rate
    FROM any_segments s
    LEFT JOIN any_churned c ON c.month = s.month
)

SELECT * FROM payment_result
UNION ALL
SELECT * FROM any_result
ORDER BY scope, month