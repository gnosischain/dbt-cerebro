

SELECT
    wallet_address,

    min(date)                              AS first_activity_date,
    max(date)                              AS last_activity_date,
    dateDiff('day', min(date), max(date))  AS tenure_days,
    uniqExact(toStartOfMonth(date))        AS active_months,

    -- Payment metrics
    round(toFloat64(sumIf(amount_usd, action = 'Payment')), 2)        AS total_payment_volume_usd,
    sumIf(activity_count, action = 'Payment')                          AS total_payment_count,

    -- Deposit / Withdrawal / Net flow
    round(toFloat64(sumIf(amount_usd, action IN ('Fiat Top Up', 'Crypto Deposit'))), 2)      AS total_deposit_volume_usd,
    round(toFloat64(sumIf(amount_usd, action IN ('Fiat Off-ramp', 'Crypto Withdrawal'))), 2)  AS total_withdrawal_volume_usd,
    round(toFloat64(
        sumIf(amount_usd, action IN ('Fiat Top Up', 'Crypto Deposit'))
      - sumIf(amount_usd, action IN ('Fiat Off-ramp', 'Crypto Withdrawal'))
    ), 2) AS net_flow_usd,

    -- Cashback
    round(toFloat64(sumIf(amount_usd, action = 'Cashback')), 2)  AS total_cashback_usd,
    round(toFloat64(sumIf(amount, action = 'Cashback')), 6)      AS total_cashback_gno,

    -- Refunds
    round(toFloat64(sumIf(amount_usd, action = 'Refund')), 2)  AS total_refund_usd,
    sumIf(activity_count, action = 'Refund')                    AS total_refund_count,

    -- Derived
    round(toFloat64(
        sumIf(amount_usd, action = 'Payment')
      / greatest(uniqExact(toStartOfMonth(date)), 1)
    ), 2) AS avg_monthly_payment_volume_usd

FROM `dbt`.`int_execution_gpay_activity_daily`
GROUP BY wallet_address