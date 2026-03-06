{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, wallet_address, action, symbol)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, wallet_address, action, symbol)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay','activity_daily']
  )
}}

SELECT
    date
    ,wallet_address
    ,action
    ,direction
    ,symbol
    ,SUM(value_raw) AS amount_raw
    ,SUM(amount) AS amount
    ,SUM(amount_usd) AS amount_usd
    ,COUNT() AS activity_count
FROM {{ ref('int_execution_gpay_activity') }} 
{{ apply_monthly_incremental_filter('date', 'date', false) }}
GROUP BY date, wallet_address, action, direction, symbol