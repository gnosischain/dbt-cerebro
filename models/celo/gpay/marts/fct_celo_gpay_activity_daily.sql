{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date)',
    tags=['production','celo','gpay']
  )
}}

-- Daily payment activity + newly-funded/cumulative-funded card counts.
-- Mirrors Gnosis Chain's fct_execution_gpay_activity_daily, with the one
-- structural difference that on Celo a card = a Safe = the user, so the user
-- grain is safe_address (Gnosis Chain distinguishes a separate wallet_address
-- identity; MiniPay cards have no such split). "Funded" = has settled a
-- payment to the bridge, matching int_celo_gpay_wallets.is_activated.
WITH daily_activity AS (
    SELECT
        date,
        uniqExact(safe_address) AS active_users,
        sum(activity_count)     AS total_payments,
        sum(amount_usd)         AS total_volume_usd
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action = 'Payment'
      AND date < today()
    GROUP BY date
),

first_payment AS (
    SELECT
        safe_address,
        min(date) AS first_date
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action = 'Payment'
    GROUP BY safe_address
),

daily_funded AS (
    SELECT
        first_date AS date,
        count()    AS newly_funded
    FROM first_payment
    WHERE first_date < today()
    GROUP BY date
)

SELECT
    a.date,
    a.active_users,
    a.total_payments,
    round(toFloat64(a.total_volume_usd), 2)                 AS total_volume_usd,
    coalesce(f.newly_funded, 0)                             AS newly_funded,
    sum(coalesce(f.newly_funded, 0)) OVER (ORDER BY a.date) AS cumulative_funded
FROM daily_activity a
LEFT JOIN daily_funded f ON f.date = a.date
ORDER BY a.date
