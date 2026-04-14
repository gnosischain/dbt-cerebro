{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    engine='ReplacingMergeTree()',
    order_by='(date, gp_safe)',
    partition_by='toStartOfMonth(date)',
    unique_key='(date, gp_safe)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','gpay']
  )
}}

{# Description in schema.yml — see int_execution_gpay_spend_activity_daily #}

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet FROM {{ ref('int_execution_gpay_wallets') }}
),

events_filtered AS (
    SELECT
        toDate(s.block_timestamp) AS date,
        s.spend_account           AS gp_safe_raw,
        s.spend_asset,
        s.spend_receiver
    FROM {{ ref('int_execution_gpay_spender_events') }} s
    WHERE s.event_name = 'Spend'
      AND s.spend_account IS NOT NULL
      AND toDate(s.block_timestamp) < today()
      {{ apply_monthly_incremental_filter('s.block_timestamp', 'date', add_and=True) }}
)

SELECT
    e.date,
    e.gp_safe_raw                AS gp_safe,
    count()                      AS spend_count,
    uniqExact(e.spend_asset)     AS distinct_assets,
    uniqExact(e.spend_receiver)  AS distinct_receivers
FROM events_filtered e
INNER JOIN gpay_safes gs ON gs.pay_wallet = e.gp_safe_raw
GROUP BY e.date, e.gp_safe_raw
