{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(month)',
    tags=['production','execution','gnosis_app','gpay','topups','mart']
  )
}}

SELECT
    toStartOfMonth(block_timestamp)              AS month,
    count(*)                                     AS n_topups,
    countDistinct(ga_user)                       AS n_ga_users,
    countDistinct(gp_wallet)                     AS n_gp_wallets,
    sum(amount_usd)                              AS volume_usd
FROM {{ ref('int_execution_gnosis_app_gpay_topups') }}
WHERE toStartOfMonth(block_timestamp) < toStartOfMonth(today())
GROUP BY month
ORDER BY month
