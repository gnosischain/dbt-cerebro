

-- Monthly KPI aggregates. Mirrors fct_execution_gpay_kpi_monthly, adapted to
-- the Celo action taxonomy: Deposit == Top-up, Withdrawal == Withdrawal,
-- Payment == card spend, Reversal == processor refund. No Cashback columns
-- (GNO cashback is Gnosis Chain only). User grain = safe_address.
-- Net flow = deposits - withdrawals (excludes card spend, matching the
-- Gnosis Chain definition).
WITH mau_volumes AS (
    SELECT
        toStartOfMonth(date) AS month,

        uniqExact(safe_address)                                AS mau,
        uniqExactIf(safe_address, action = 'Payment')          AS payment_mau,
        uniqExactIf(safe_address, action = 'Top-up')           AS deposit_mau,
        uniqExactIf(safe_address, action = 'Withdrawal')       AS withdrawal_mau,

        sumIf(amount_usd, action = 'Payment')                  AS total_payment_volume_usd,
        sumIf(activity_count, action = 'Payment')              AS total_payment_count,
        sumIf(amount_usd, action = 'Top-up')                   AS total_deposit_volume_usd,
        sumIf(amount_usd, action = 'Withdrawal')               AS total_withdrawal_volume_usd,
        sumIf(amount_usd, action = 'Reversal')                 AS reversal_total_usd
    FROM `dbt`.`int_celo_gpay_activity_daily`
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
            safe_address,
            sum(activity_count)  AS payments
        FROM `dbt`.`int_celo_gpay_activity_daily`
        WHERE action = 'Payment'
          AND toStartOfMonth(date) < toStartOfMonth(today())
        GROUP BY month, safe_address
    )
    GROUP BY month
)

SELECT
    m.month,

    m.mau,
    m.payment_mau,
    m.deposit_mau,
    m.withdrawal_mau,

    round(toFloat64(m.total_payment_volume_usd), 2)    AS total_payment_volume_usd,
    m.total_payment_count,
    round(toFloat64(m.total_deposit_volume_usd), 2)    AS total_deposit_volume_usd,
    round(toFloat64(m.total_withdrawal_volume_usd), 2) AS total_withdrawal_volume_usd,
    round(toFloat64(
        m.total_deposit_volume_usd - m.total_withdrawal_volume_usd
    ), 2)                                              AS net_flow_usd,
    round(toFloat64(m.reversal_total_usd), 2)          AS reversal_total_usd,

    round(toFloat64(m.total_payment_volume_usd) / greatest(m.payment_mau, 1), 2) AS arpu,
    round(toFloat64(m.total_payment_count) / greatest(m.payment_mau, 1), 2)      AS avg_tx_per_user,
    round(toFloat64(r.repeat_purchase_rate) * 100, 1)                            AS repeat_purchase_rate

FROM mau_volumes m
LEFT JOIN repeat_rate r ON r.month = m.month
ORDER BY m.month