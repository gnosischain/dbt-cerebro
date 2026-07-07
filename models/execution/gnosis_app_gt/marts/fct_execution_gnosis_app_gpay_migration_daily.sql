{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    tags=['production', 'execution', 'gnosis_app_gt', 'gpay', 'migration', 'mart'],
    meta={'grain': 'date'}
) }}

-- June-2026 Gnosis Pay exploit -> Safe migration recovery time-series.
-- One row per calendar date over the observation window, backing the Migration
-- tab KPIs / line charts. Columns:
--   usd_old_safes        = latest-known USD still held in migrated OLD safes as of date
--   usd_new_safes        = latest-known USD held in migrated NEW (canonical) safes as of date
--   cum_reactivated_any  = cumulative distinct NEW safes with ANY activity on-or-before date (since cutover)
--   cum_reactivated_spend= cumulative distinct NEW safes with a card Payment on-or-before date (since cutover)
--   refunds_landed_usd   = exploit-recovery refund USD credited to NEW safes ON that date
--
-- The 66,001 OLD->NEW pairs come from int_execution_gpay_safe_canonical (1:1).
-- Refunds land 2026-06-05..09; they carry raw token units, priced here with the
-- same per-token USD/native ratio that int_execution_gpay_balances_daily uses,
-- so refund USD reconciles with the balance USD series.

{% set cutover_date = "'2026-06-04'" %}

WITH canon AS (
    SELECT
        lower(address)           AS old_addr,
        lower(canonical_address) AS new_addr
    FROM {{ ref('int_execution_gpay_safe_canonical') }}
),

-- ---- date spine: every day from cutover through the latest observed data ----
bounds AS (
    SELECT
        toDate({{ cutover_date }})                                        AS start_date,
        greatest(
            (SELECT max(date) FROM {{ ref('int_execution_gpay_balances_daily') }}),
            (SELECT max(date) FROM {{ ref('int_execution_gpay_activity') }})
        )                                                                 AS end_date
),
spine AS (
    SELECT (start_date + toIntervalDay(number)) AS date
    FROM bounds
    ARRAY JOIN range(toUInt32(dateDiff('day', start_date, end_date) + 1)) AS number
),

-- ---- daily USD balances in OLD vs NEW migrated safes ----
bal_daily AS (
    SELECT
        b.date                                                                       AS date,
        sumIf(b.balance_usd, lower(b.address) IN (SELECT new_addr FROM canon))        AS usd_new_safes,
        sumIf(b.balance_usd, lower(b.address) IN (SELECT old_addr FROM canon))        AS usd_old_safes
    FROM {{ ref('int_execution_gpay_balances_daily') }} b
    WHERE b.date >= toDate({{ cutover_date }})
      AND ( lower(b.address) IN (SELECT new_addr FROM canon)
         OR lower(b.address) IN (SELECT old_addr FROM canon) )
    GROUP BY b.date
),

-- ---- first (any / spend) activity date per reactivated NEW safe, since cutover ----
first_act AS (
    SELECT
        lower(a.wallet_address)                                          AS new_addr,
        min(a.date)                                                       AS first_any_date,
        minIf(a.date, a.action IN ('Payment'))                            AS first_spend_date
    FROM {{ ref('int_execution_gpay_activity') }} a
    WHERE a.date >= toDate({{ cutover_date }})
      AND lower(a.wallet_address) IN (SELECT new_addr FROM canon)
    GROUP BY lower(a.wallet_address)
),
react_daily AS (
    SELECT
        first_any_date                                                    AS date,
        toInt64(count())                                                  AS new_react_any
    FROM first_act
    GROUP BY first_any_date
),
spend_daily AS (
    SELECT
        first_spend_date                                                  AS date,
        toInt64(count())                                                  AS new_react_spend
    FROM first_act
    WHERE first_spend_date IS NOT NULL
    GROUP BY first_spend_date
),

-- ---- per-token USD price + native decimals, used to value refunds ----
token_price AS (
    SELECT
        symbol,
        median(balance_usd / nullIf(balance, 0))                          AS usd_per_native
    FROM {{ ref('int_execution_gpay_balances_daily') }}
    WHERE date BETWEEN toDate({{ cutover_date }}) AND (toDate({{ cutover_date }}) + toIntervalDay(7))
      AND balance > 0
      AND balance_usd IS NOT NULL
    GROUP BY symbol
),
refunds_daily AS (
    SELECT
        r.refund_date                                                     AS date,
        sum(
            (r.refund_amount_raw / pow(10, if(r.symbol IN ('USDC.e','USDC','USDT'), 6, 18)))
            * coalesce(p.usd_per_native, 0)
        )                                                                 AS refunds_landed_usd
    FROM {{ ref('int_execution_gpay_refunds') }} r
    LEFT JOIN token_price p ON p.symbol = r.symbol
    GROUP BY r.refund_date
),

-- ---- assemble per-date, forward-filling last-known balances over gap days ----
joined AS (
    SELECT
        s.date                                                            AS date,
        bd.usd_old_safes                                                  AS usd_old_raw,
        bd.usd_new_safes                                                  AS usd_new_raw,
        coalesce(ra.new_react_any, 0)                                     AS new_react_any,
        coalesce(sd.new_react_spend, 0)                                   AS new_react_spend,
        coalesce(rf.refunds_landed_usd, 0)                               AS refunds_landed_usd
    FROM spine s
    LEFT JOIN bal_daily    bd ON bd.date = s.date
    LEFT JOIN react_daily  ra ON ra.date = s.date
    LEFT JOIN spend_daily  sd ON sd.date = s.date
    LEFT JOIN refunds_daily rf ON rf.date = s.date
)

SELECT
    date,
    -- forward-fill last-observed balance across any date with no snapshot
    toFloat64(round(coalesce(
        usd_old_raw,
        anyLast(usd_old_raw) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ), 2))                                                                AS usd_old_safes,
    toFloat64(round(coalesce(
        usd_new_raw,
        anyLast(usd_new_raw) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ), 2))                                                                AS usd_new_safes,
    toInt64(sum(new_react_any)   OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS cum_reactivated_any,
    toInt64(sum(new_react_spend) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS cum_reactivated_spend,
    toFloat64(round(refunds_landed_usd, 2))                               AS refunds_landed_usd
FROM joined
ORDER BY date
