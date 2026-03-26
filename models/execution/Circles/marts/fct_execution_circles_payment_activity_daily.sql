{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'payments']
    )
}}

SELECT
    toDate(block_timestamp) AS date,
    gateway,
    count() AS payment_count,
    uniqExact(payer) AS unique_payer_count,
    uniqExact(payee) AS unique_payee_count,
    sum(amount_raw) AS total_amount_raw
FROM {{ ref('int_execution_circles_payments') }}
GROUP BY 1, 2
