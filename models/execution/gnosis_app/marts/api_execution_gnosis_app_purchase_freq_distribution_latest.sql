{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','tier1','api:gnosis_app_purchase_freq_distribution','granularity:latest']
  )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}) AS as_of_date
FROM (
-- Distribution of repeat-purchase intensity over the last 30 days.
-- Buckets: 1 / 2 / 3 / 4-5 / 6-10 / 11+. Each row gives the count of
-- distinct users falling in that bucket.

WITH bucketed AS (
    SELECT
        multiIf(
            n_purchases =  1,              '1',
            n_purchases =  2,              '2',
            n_purchases =  3,              '3',
            n_purchases <= 5,              '4-5',
            n_purchases <= 10,             '6-10',
                                           '11+'
        ) AS bucket,
        multiIf(
            n_purchases =  1,              toUInt8(1),
            n_purchases =  2,              toUInt8(2),
            n_purchases =  3,              toUInt8(3),
            n_purchases <= 5,              toUInt8(4),
            n_purchases <= 10,             toUInt8(5),
                                           toUInt8(6)
        ) AS bucket_order
    FROM {{ ref('int_execution_gnosis_app_user_purchase_freq_30d') }}
)

SELECT
    bucket_order,
    bucket,
    count() AS n_users
FROM bucketed
GROUP BY bucket_order, bucket
ORDER BY bucket_order
) AS sub
