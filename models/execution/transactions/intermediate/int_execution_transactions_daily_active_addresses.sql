{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, address_hash)',
    partition_by='toStartOfMonth(date)',
    tags=['production','execution','transactions']
  )
}}

SELECT
    d.date,
    arrayJoin(bitmapToArray(d.ua_bitmap_state)) AS address_hash
FROM {{ ref('int_execution_transactions_by_project_daily') }} d
WHERE d.date > subtractDays(today(), 181)
