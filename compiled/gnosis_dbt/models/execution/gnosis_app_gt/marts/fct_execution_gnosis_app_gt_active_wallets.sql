

-- Active-wallet time-series (DAU / WAU / MAU) from real on-chain activity
-- (transaction_action.timestamp), scoped to app-engaged wallets. TWO definitions
-- per period:
--   active_wallets            = ANY on-chain action by an app-engaged wallet (broad)
--   active_wallets_app_tagged = a DELIBERATE app-feature action that day (swap /
--                               auto-topup / MetriFee·PayTopUp·AutoTopup) — the
--                               series comparable to the heuristic current-app DAU.
-- new_wallets(_app_tagged) = wallets whose FIRST (app-tagged) activity falls in the period.
WITH first_seen AS (
    SELECT address,
        min(activity_date)                        AS first_day,
        minIf(activity_date, is_app_tagged_day)    AS first_day_app
    FROM `dbt`.`stg_envio_ga__wallet_activity_daily`
    GROUP BY address
),
acts AS (
    SELECT w.address, w.activity_date, w.is_app_tagged_day, f.first_day, f.first_day_app
    FROM `dbt`.`stg_envio_ga__wallet_activity_daily` w
    INNER JOIN first_seen f USING (address)
)
SELECT
    'day'                                                                          AS period_type,
    activity_date                                                                  AS period_start,
    uniqExact(address)                                                             AS active_wallets,
    uniqExactIf(address, first_day = activity_date)                                AS new_wallets,
    uniqExactIf(address, is_app_tagged_day)                                        AS active_wallets_app_tagged,
    uniqExactIf(address, is_app_tagged_day AND first_day_app = activity_date)       AS new_wallets_app_tagged
FROM acts
GROUP BY period_start

UNION ALL

SELECT
    'week',
    toMonday(activity_date),
    uniqExact(address),
    uniqExactIf(address, toMonday(first_day) = toMonday(activity_date)),
    uniqExactIf(address, is_app_tagged_day),
    uniqExactIf(address, is_app_tagged_day AND toMonday(first_day_app) = toMonday(activity_date))
FROM acts
GROUP BY toMonday(activity_date)

UNION ALL

SELECT
    'month',
    toStartOfMonth(activity_date),
    uniqExact(address),
    uniqExactIf(address, toStartOfMonth(first_day) = toStartOfMonth(activity_date)),
    uniqExactIf(address, is_app_tagged_day),
    uniqExactIf(address, is_app_tagged_day AND toStartOfMonth(first_day_app) = toStartOfMonth(activity_date))
FROM acts
GROUP BY toStartOfMonth(activity_date)