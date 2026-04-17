{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, token_bought_symbol)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','gpay','topups','mart']
  )
}}

SELECT
    toDate(t.block_timestamp)                        AS date,
    coalesce(t.token_bought_symbol, wb.symbol)       AS token_bought_symbol,
    count(*)                                         AS n_topups,
    countDistinct(t.ga_user)                         AS n_ga_users,
    countDistinct(t.gp_wallet)                       AS n_gp_wallets,
    sum(t.amount_bought)                             AS volume_token_bought,
    sum(t.amount_usd)                                AS volume_usd
FROM {{ ref('int_execution_gnosis_app_gpay_topups') }} t
LEFT JOIN {{ ref('int_execution_circles_v2_wrapper_tokens') }} wb
    ON wb.wrapper_address = t.token_bought_address
GROUP BY date, token_bought_symbol
ORDER BY date, token_bought_symbol
