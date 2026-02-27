{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(month, segment)',
    tags=['production','execution','gpay']
  )
}}

-- Cashback received per wallet per month
WITH cashback_by_month AS (
    SELECT
        wallet_address,
        toStartOfMonth(date) AS month,
        sum(amount_usd)      AS cb_usd
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE action = 'Cashback'
    GROUP BY wallet_address, month
),

-- Payment + deposit/withdrawal activity per wallet per month
wallet_monthly AS (
    SELECT
        wallet_address,
        toStartOfMonth(date) AS month,
        sumIf(amount_usd, action = 'Payment')                                          AS payment_vol,
        sumIf(activity_count, action = 'Payment')                                      AS payment_cnt,
        sumIf(amount_usd, action IN ('Fiat Top Up', 'Crypto Deposit'))                 AS deposit_vol,
        sumIf(amount_usd, action IN ('Fiat Off-ramp', 'Crypto Withdrawal'))            AS withdrawal_vol
    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY wallet_address, month
    HAVING payment_cnt > 0   -- only wallets that made payments
),

-- For each (wallet, month), compute cumulative cashback received up to that month
with_cumulative_cashback AS (
    SELECT
        p.wallet_address,
        p.month,
        p.payment_vol,
        p.payment_cnt,
        p.deposit_vol,
        p.withdrawal_vol,
        coalesce(sum(c.cb_usd), 0) AS cumulative_cb_usd
    FROM wallet_monthly p
    LEFT JOIN cashback_by_month c
        ON  c.wallet_address = p.wallet_address
        AND c.month <= p.month
    GROUP BY
        p.wallet_address, p.month,
        p.payment_vol, p.payment_cnt,
        p.deposit_vol, p.withdrawal_vol
),

-- Assign cashback tier segment
classified AS (
    SELECT
        *,
        multiIf(
            cumulative_cb_usd = 0,  'non_recipient',
            cumulative_cb_usd < 10, 'cb_low',
            cumulative_cb_usd < 50, 'cb_medium',
                                    'cb_high'
        ) AS segment
    FROM with_cumulative_cashback
)

SELECT
    month,
    segment,

    count()                                                         AS users,
    round(toFloat64(sum(payment_vol)), 2)                           AS payment_volume_usd,
    sum(payment_cnt)                                                AS payment_count,
    round(toFloat64(sum(payment_vol)) / greatest(count(), 1), 2)    AS avg_volume_per_user,
    round(toFloat64(sum(payment_cnt)) / greatest(count(), 1), 2)    AS avg_tx_per_user,

    round(toFloat64(sum(deposit_vol)), 2)                           AS deposit_volume_usd,
    round(toFloat64(sum(withdrawal_vol)), 2)                        AS withdrawal_volume_usd,
    round(toFloat64(sum(deposit_vol) - sum(withdrawal_vol)), 2)     AS net_flow_usd,

    -- Share of total monthly volume & users (window functions)
    round(
        sum(payment_vol) / greatest(sum(sum(payment_vol)) OVER (PARTITION BY month), 1) * 100
    , 1)  AS pct_of_total_volume,
    round(
        count() / greatest(sum(count()) OVER (PARTITION BY month), 1) * 100
    , 1)  AS pct_of_total_users

FROM classified
GROUP BY month, segment
ORDER BY month, segment
