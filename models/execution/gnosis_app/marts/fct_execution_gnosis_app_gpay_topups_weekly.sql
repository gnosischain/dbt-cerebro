{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(week)',
    tags=['production','execution','gnosis_app','gpay','topups','mart']
  )
}}

SELECT
    toStartOfWeek(block_timestamp, 1)            AS week,
    count(*)                                     AS n_topups,
    countDistinct(ga_user)                       AS n_ga_users,
    countDistinct(gp_wallet)                     AS n_gp_wallets,
    sum(amount_usd)                              AS volume_usd
FROM {{ ref('int_execution_gnosis_app_gpay_topups') }}
WHERE toStartOfWeek(block_timestamp, 1) < toStartOfWeek(today(), 1)
GROUP BY week
ORDER BY week
