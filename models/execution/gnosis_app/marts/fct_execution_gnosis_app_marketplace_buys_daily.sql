{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, offer_name)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','marketplace','mart']
  )
}}

SELECT
    toDate(block_timestamp)                  AS date,
    offer_name                               AS offer_name,
    count(*)                                 AS n_buys,
    countDistinct(payer)                     AS n_payers,
    sum(amount)                              AS volume_token
FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }}
GROUP BY toDate(block_timestamp), offer_name
ORDER BY date, offer_name
