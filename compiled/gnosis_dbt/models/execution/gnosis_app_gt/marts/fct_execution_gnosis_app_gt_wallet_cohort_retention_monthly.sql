

-- Wallet retention by ACQUISITION cohort, two bases:
--   any_action = cohort + activity from ANY on-chain action by an app-engaged wallet (broad)
--   app_tagged = cohort + activity from DELIBERATE app-feature actions (is_app_tagged_day) —
--                the series comparable to the heuristic current-app retention.
-- Cohort + numerator come from the SAME feed (self-consistent → month_index 0 = 1.0). Denominator
-- is the cohort size (active wallets), never the raw registry (identity-grain guard).
WITH wad AS (
    SELECT address, activity_date, is_app_tagged_day
    FROM `dbt`.`stg_envio_ga__wallet_activity_daily`
),
-- any_action basis
acq_a AS (SELECT address, toStartOfMonth(min(activity_date)) AS cohort_month FROM wad GROUP BY address),
act_a AS (SELECT DISTINCT address, toStartOfMonth(activity_date) AS active_month FROM wad),
siz_a AS (SELECT cohort_month, uniqExact(address) AS cohort_size FROM acq_a GROUP BY cohort_month),
-- app_tagged basis
acq_t AS (SELECT address, toStartOfMonth(min(activity_date)) AS cohort_month FROM wad WHERE is_app_tagged_day GROUP BY address),
act_t AS (SELECT DISTINCT address, toStartOfMonth(activity_date) AS active_month FROM wad WHERE is_app_tagged_day),
siz_t AS (SELECT cohort_month, uniqExact(address) AS cohort_size FROM acq_t GROUP BY cohort_month)
SELECT
    'any_action'                                              AS basis,
    a.cohort_month                                            AS cohort_month,
    dateDiff('month', a.cohort_month, x.active_month)         AS month_index,
    uniqExact(a.address)                                      AS retained_wallets,
    any(s.cohort_size)                                        AS cohort_size,
    round(uniqExact(a.address) / any(s.cohort_size), 4)       AS retention_pct
FROM acq_a a
INNER JOIN act_a x USING (address)
INNER JOIN siz_a s ON s.cohort_month = a.cohort_month
WHERE x.active_month >= a.cohort_month
GROUP BY a.cohort_month, month_index

UNION ALL

SELECT
    'app_tagged'                                             AS basis,
    a.cohort_month                                           AS cohort_month,
    dateDiff('month', a.cohort_month, x.active_month)        AS month_index,
    uniqExact(a.address)                                     AS retained_wallets,
    any(s.cohort_size)                                       AS cohort_size,
    round(uniqExact(a.address) / any(s.cohort_size), 4)      AS retention_pct
FROM acq_t a
INNER JOIN act_t x USING (address)
INNER JOIN siz_t s ON s.cohort_month = a.cohort_month
WHERE x.active_month >= a.cohort_month
GROUP BY a.cohort_month, month_index