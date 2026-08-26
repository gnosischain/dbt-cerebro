

-- Daily payment activity plus the two funnel counts, which are DIFFERENT METRICS and
-- were conflated under one name until 2026-08-05:
--   funded    = the card has ever RECEIVED money (any inbound action).
--   activated = the card has ever SPENT (settled a payment to a bridge). Equivalent to
--               int_celo_gpay_wallets.is_activated.
-- Funnel: 1817 issued (int_celo_gpay_safe_registry) > 1087 funded > 662 activated on
-- 2026-08-05. Both are useful; the bug was that `cumulative_funded` counted from first
-- PAYMENT, so the api_celo_gpay_funded_addresses_* series charted activation under the
-- funded label and read 40% low (654 against 1075). `newly_activated`/
-- `cumulative_activated` now carry that original series under its true name, and
-- funded means funded. Never reintroduce a single "funded" derived from Payment.
--
-- Mirrors Gnosis Chain's fct_execution_gpay_activity_daily, with the one structural
-- difference that on Celo a card = a Safe = the user, so the user grain is
-- safe_address (Gnosis Chain distinguishes a separate wallet_address identity;
-- MiniPay cards have no such split). NOTE the Gnosis twin still has the original
-- Payment-derived `funded` and so still understates funding — fixing it needs the same
-- split plus a dashboard change, and is deliberately not done here.
--
-- THE DATE SPINE IS THE UNION of payment-activity days and funnel-transition days, not
-- payment days alone. A card can be funded on a day when nobody spends: 9 such days
-- exist and they carried 28 cards, all of which a payment-only spine dropped from
-- cumulative_funded silently, since the LEFT JOIN simply had no row to attach them to.
-- Rows added by the union carry active_users/total_payments/total_volume_usd = 0, which
-- is the truth for those days. The weekly and monthly rollups lose nothing today
-- (0 cards off-spine at both grains) but use the same union so a funding-only week
-- cannot introduce the bug later.

WITH payment_activity AS (
    SELECT
        date,
        uniqExact(safe_address) AS active_users,
        sum(activity_count)     AS total_payments,
        sum(amount_usd)         AS total_volume_usd
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action = 'Payment'
      AND date < today()
    GROUP BY date
),

-- First SPEND per card -> activation.
first_payment AS (
    SELECT safe_address, min(date) AS first_date
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action = 'Payment'
    GROUP BY safe_address
),

-- First INFLOW per card -> funding. Inbound actions only; the classifier's outbound
-- actions are Payment/Withdrawal/Other. Cashback is currently compiled out of
-- int_celo_gpay_activity and Reversal has never occurred, so this is Top-up in
-- practice today — listing all three keeps it correct the day either starts firing.
first_inflow AS (
    SELECT safe_address, min(date) AS first_date
    FROM `dbt`.`int_celo_gpay_activity_daily`
    WHERE action IN ('Top-up', 'Reversal', 'Cashback')
    GROUP BY safe_address
),

daily_activated AS (
    SELECT first_date AS date, count() AS n
    FROM first_payment WHERE first_date < today() GROUP BY date
),

daily_funded AS (
    SELECT first_date AS date, count() AS n
    FROM first_inflow WHERE first_date < today() GROUP BY date
),

spine AS (
    SELECT date FROM payment_activity
    UNION DISTINCT SELECT date FROM daily_activated
    UNION DISTINCT SELECT date FROM daily_funded
)

SELECT
    s.date                                                   AS date,
    coalesce(p.active_users, 0)                              AS active_users,
    coalesce(p.total_payments, 0)                            AS total_payments,
    round(toFloat64(coalesce(p.total_volume_usd, 0)), 2)     AS total_volume_usd,
    coalesce(fd.n, 0)                                        AS newly_funded,
    sum(coalesce(fd.n, 0)) OVER (ORDER BY s.date)            AS cumulative_funded,
    coalesce(ac.n, 0)                                        AS newly_activated,
    sum(coalesce(ac.n, 0)) OVER (ORDER BY s.date)            AS cumulative_activated
FROM spine s
LEFT JOIN payment_activity p ON p.date = s.date
LEFT JOIN daily_funded     fd ON fd.date = s.date
LEFT JOIN daily_activated  ac ON ac.date = s.date
ORDER BY s.date