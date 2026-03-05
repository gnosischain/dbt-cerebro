{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(month)',
    tags=['production','execution','gpay']
  )
}}

WITH mau_volumes AS (
    SELECT
        toStartOfMonth(date) AS month,

        -- MAU by scope
        uniqExact(wallet_address)                                                         AS mau,
        uniqExactIf(wallet_address, action = 'Payment')                                   AS payment_mau,
        uniqExactIf(wallet_address, action IN ('Fiat Top Up', 'Crypto Deposit'))           AS deposit_mau,
        uniqExactIf(wallet_address, action IN ('Fiat Off-ramp', 'Crypto Withdrawal'))      AS withdrawal_mau,
        uniqExactIf(wallet_address, action = 'Cashback')                                   AS cashback_mau,

        -- Volume aggregates
        sumIf(amount_usd, action = 'Payment')                                              AS total_payment_volume_usd,
        sumIf(activity_count, action = 'Payment')                                          AS total_payment_count,
        sumIf(amount_usd, action IN ('Fiat Top Up', 'Crypto Deposit'))                     AS total_deposit_volume_usd,
        sumIf(amount_usd, action IN ('Fiat Off-ramp', 'Crypto Withdrawal'))                AS total_withdrawal_volume_usd,
        sumIf(amount_usd, action = 'Cashback')                                             AS cashback_total_usd,
        sumIf(amount,     action = 'Cashback')                                             AS cashback_total_gno,
        sumIf(amount_usd, action = 'Refund')                                               AS refund_total_usd,
        sumIf(amount_usd, action = 'Reversal')                                             AS reversal_total_usd

    FROM {{ ref('int_execution_gpay_activity_daily') }}
    WHERE toStartOfMonth(date) < toStartOfMonth(today())
    GROUP BY month
),

repeat_rate AS (
    SELECT
        month,
        countIf(payments >= 2) / greatest(count(), 1) AS repeat_purchase_rate
    FROM (
        SELECT
            toStartOfMonth(date) AS month,
            wallet_address,
            sum(activity_count)  AS payments
        FROM {{ ref('int_execution_gpay_activity_daily') }}
        WHERE action = 'Payment'
          AND toStartOfMonth(date) < toStartOfMonth(today())
        GROUP BY month, wallet_address
    )
    GROUP BY month
)

SELECT
    m.month,

    m.mau,
    m.payment_mau,
    m.deposit_mau,
    m.withdrawal_mau,
    m.cashback_mau,

    round(toFloat64(m.total_payment_volume_usd), 2)       AS total_payment_volume_usd,
    m.total_payment_count,
    round(toFloat64(m.total_deposit_volume_usd), 2)        AS total_deposit_volume_usd,
    round(toFloat64(m.total_withdrawal_volume_usd), 2)     AS total_withdrawal_volume_usd,
    round(toFloat64(
        m.total_deposit_volume_usd - m.total_withdrawal_volume_usd
    ), 2)                                                   AS net_flow_usd,

    round(toFloat64(m.cashback_total_usd), 2)              AS cashback_total_usd,
    round(toFloat64(m.cashback_total_gno), 6)              AS cashback_total_gno,
    round(toFloat64(m.refund_total_usd), 2)                AS refund_total_usd,
    round(toFloat64(m.reversal_total_usd), 2)              AS reversal_total_usd,

    -- Derived KPIs
    round(toFloat64(m.total_payment_volume_usd) / greatest(m.payment_mau, 1), 2) AS arpu,
    round(toFloat64(m.total_payment_count) / greatest(m.payment_mau, 1), 2)      AS avg_tx_per_user,
    round(toFloat64(r.repeat_purchase_rate) * 100, 1)                              AS repeat_purchase_rate

FROM mau_volumes m
LEFT JOIN repeat_rate r ON r.month = m.month
ORDER BY m.month
