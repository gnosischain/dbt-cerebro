{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'payments']
    )
}}

WITH payment_spend AS (
    SELECT
        toStartOfMonth(toDate(block_timestamp)) AS month,
        sum(amount_raw) AS payment_amount_raw,
        toUInt256(0) AS offer_spent_raw
    FROM {{ ref('int_execution_circles_payments') }}
    GROUP BY 1
),
offer_spend AS (
    SELECT
        toStartOfMonth(date) AS month,
        toUInt256(0) AS payment_amount_raw,
        sum(total_spent_raw) AS offer_spent_raw
    FROM {{ ref('fct_execution_circles_token_offer_spend_daily') }}
    GROUP BY 1
),
unioned AS (
    SELECT * FROM payment_spend
    UNION ALL
    SELECT * FROM offer_spend
)

SELECT
    month,
    sum(payment_amount_raw) AS payment_amount_raw,
    sum(offer_spent_raw) AS offer_spent_raw,
    sum(payment_amount_raw) + sum(offer_spent_raw) AS total_amount_raw
FROM unioned
GROUP BY 1
