

WITH base AS (
    SELECT
        toDate(first_ga_owner_at) AS date,
        onboarding_class,
        pay_wallet
    FROM `dbt`.`int_execution_gnosis_app_gpay_wallets`
    WHERE first_ga_owner_at IS NOT NULL
),

daily AS (
    SELECT
        date,
        onboarding_class,
        count(DISTINCT pay_wallet) AS n_ga_wallets_new
    FROM base
    GROUP BY date, onboarding_class
),

-- Dense calendar spine × onboarding_class so chart has continuous dates.
calendar AS (
    SELECT
        addDays(min_date, number) AS date
    FROM (
        SELECT min(date) AS min_date, today() AS max_date
        FROM daily
    )
    ARRAY JOIN range(0, toUInt64(dateDiff('day', min_date, max_date) + 1)) AS number
),

classes AS (
    SELECT 'onboarded_via_ga' AS onboarding_class
    UNION ALL
    SELECT 'imported' AS onboarding_class
),

spine AS (
    SELECT c.date, cl.onboarding_class
    FROM calendar c CROSS JOIN classes cl
)

SELECT
    s.date                                                       AS date,
    s.onboarding_class                                           AS onboarding_class,
    coalesce(d.n_ga_wallets_new, 0)                              AS n_ga_wallets_new,
    sum(coalesce(d.n_ga_wallets_new, 0))
        OVER (PARTITION BY s.onboarding_class
              ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS n_ga_wallets_cumulative
FROM spine s
LEFT JOIN daily d
    ON d.date = s.date
   AND d.onboarding_class = s.onboarding_class
ORDER BY s.date, s.onboarding_class