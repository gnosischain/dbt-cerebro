{{
    config(
        materialized='view',
        tags=['production', 'execution', 'gpay', 'api:gpay_user_top_wallets', 'granularity:snapshot', 'tier1']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_gpay_activity_daily') }}) AS as_of_date
FROM (
SELECT wallet_address
FROM {{ ref('fct_execution_gpay_user_lifetime_metrics') }}
WHERE total_payment_count > 0
ORDER BY
    total_payment_volume_usd DESC,
    tenure_days DESC
LIMIT 50
) AS sub
