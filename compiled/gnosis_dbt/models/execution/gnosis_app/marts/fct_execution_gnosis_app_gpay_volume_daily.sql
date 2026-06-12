

-- Gnosis-App-scoped Gnosis Pay funding & spend volume, daily, split by
-- onboarding_class. Answers "how much do GA-controlled wallets load onto and
-- spend on the card" — the real figures the narrow int_execution_gnosis_app_gpay_topups
-- model (in-app-swap-to-card flow only) does NOT capture.
--
--   funded_volume_usd — cumulative inflows (Fiat Top Up + Crypto Deposit). Loaded volume,
--                       NOT current balance (balance = inflows - outflows; use
--                       int_execution_gpay_balances_daily for remaining funds).
--   spend_usd         — card payments (action='Payment').
--
-- Built from raw int_execution_gpay_activity (timestamp grain) so the post-link
-- gate `block_timestamp >= first_ga_owner_at` is exact — a date-grain rollup
-- would leak same-day pre-link activity for `imported` wallets.

WITH ga_wallets AS (
    SELECT
        lower(pay_wallet)  AS wallet_address,
        onboarding_class,
        first_ga_owner_at
    FROM `dbt`.`int_execution_gnosis_app_gpay_wallets`
    WHERE first_ga_owner_at IS NOT NULL
),

agg AS (
    SELECT
        toDate(a.block_timestamp)                                                            AS date,
        w.onboarding_class                                                                   AS onboarding_class,
        sumIf(toFloat64OrNull(toString(a.amount_usd)), a.action IN ('Fiat Top Up','Crypto Deposit')) AS funded_volume_usd,
        sumIf(toFloat64OrNull(toString(a.amount_usd)), a.action = 'Payment')                 AS spend_usd,
        countIf(a.action = 'Payment')                                                        AS spend_count,
        uniqExactIf(lower(a.wallet_address), a.action = 'Payment')                           AS spending_wallets
    FROM `dbt`.`int_execution_gpay_activity` a
    INNER JOIN ga_wallets w
        ON w.wallet_address = lower(a.wallet_address)
    WHERE a.action IN ('Fiat Top Up','Crypto Deposit','Payment')
      AND a.block_timestamp >= w.first_ga_owner_at
      AND a.block_timestamp < today()
    GROUP BY date, w.onboarding_class
),

-- Dense calendar spine × onboarding_class so the cumulative series stays continuous.
calendar AS (
    SELECT addDays(min_date, number) AS date
    FROM (
        SELECT min(date) AS min_date, today() AS max_date
        FROM agg
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
    s.date                                                            AS date,
    s.onboarding_class                                                AS onboarding_class,
    round(coalesce(a.funded_volume_usd, 0), 2)                        AS funded_volume_usd,
    round(coalesce(a.spend_usd, 0), 2)                                AS spend_usd,
    coalesce(a.spend_count, 0)                                        AS spend_count,
    coalesce(a.spending_wallets, 0)                                   AS spending_wallets,
    round(sum(coalesce(a.funded_volume_usd, 0))
        OVER (PARTITION BY s.onboarding_class
              ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)   AS funded_volume_cumulative_usd,
    round(sum(coalesce(a.spend_usd, 0))
        OVER (PARTITION BY s.onboarding_class
              ORDER BY s.date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)   AS spend_cumulative_usd
FROM spine s
LEFT JOIN agg a
    ON a.date = s.date
   AND a.onboarding_class = s.onboarding_class
ORDER BY s.date, s.onboarding_class