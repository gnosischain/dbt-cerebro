

-- The composite Gnosis App activity feed EXTENDED with Gnosis Pay card-wallet transactions
-- (int_execution_gnosis_app_gpay_txns → the safe's GA owner). Parallel to
-- int_execution_gnosis_app_user_activity_daily; consumed ONLY by the "incl. Gnosis Pay" WAU
-- variant (fct_execution_gnosis_app_users_weekly_incl_gpay), so the current metric — and DAU /
-- MAU / WEAU / retention — stay unchanged for comparison.
--
-- IMPORTANT: read the base activity table ONCE (all activity_kinds) into `cur` and filter only
-- WITHIN the `combined` CTE result. Reading the base table in two CTEs with opposite
-- activity_kind filters (= 'onboard' vs != 'onboard') triggers a ClickHouse predicate-pushdown
-- bug that leaks every non-onboard row into the 'onboard' branch (onboard exploded to ~8.5×).

WITH cur AS (
    SELECT date, address, activity_kind, n_events, amount_usd
    FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
),

gpay AS (
    -- one row per (day, GA owner) for any user-initiated GP wallet transaction
    SELECT
        toDate(block_timestamp)                     AS date,
        ga_user                                     AS address,
        'gpay_txn'                                  AS activity_kind,
        count(*)                                    AS n_events,
        sum(toFloat64OrNull(toString(amount_usd)))  AS amount_usd
    FROM `dbt`.`int_execution_gnosis_app_gpay_txns`
    GROUP BY toDate(block_timestamp), ga_user
),

combined AS (
    SELECT date, address, activity_kind, n_events, amount_usd FROM cur
    UNION ALL
    SELECT date, address, activity_kind, n_events, amount_usd FROM gpay
),

onboarded_addrs AS (
    SELECT DISTINCT address FROM combined WHERE activity_kind = 'onboard'
),

extra_onboard AS (
    -- First-touch onboard for owners who are active (incl. via GP) but never got a Circles
    -- onboard row, so a first GP transaction can count as "New". Currently empty — every GP
    -- owner already onboarded via the app — but kept for correctness / future-proofing.
    SELECT
        min(date)                        AS date,
        address                          AS address,
        'onboard'                        AS activity_kind,
        1                                AS n_events,
        CAST(NULL AS Nullable(Float64))  AS amount_usd
    FROM combined
    WHERE activity_kind != 'onboard'
      AND address NOT IN (SELECT address FROM onboarded_addrs)
    GROUP BY address
)

SELECT date, address, activity_kind, n_events, amount_usd FROM combined
UNION ALL
SELECT date, address, activity_kind, n_events, amount_usd FROM extra_onboard