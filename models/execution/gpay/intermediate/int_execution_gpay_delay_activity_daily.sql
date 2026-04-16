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

WITH gp_delay_modules AS (
    SELECT gp_safe, module_proxy_address AS delay_module_address
    FROM {{ ref('int_execution_gpay_safe_modules') }}
    WHERE contract_type = 'DelayModule'
),

events_filtered AS (
    SELECT
        toDate(d.block_timestamp)  AS date,
        d.delay_module_address     AS delay_module_address
    FROM {{ ref('int_execution_gpay_delay_events') }} d
    WHERE d.event_name = 'TransactionAdded'
      AND toDate(d.block_timestamp) < today()
      {{ apply_monthly_incremental_filter('d.block_timestamp', 'date', add_and=True) }}
)

SELECT
    e.date,
    m.gp_safe,
    count() AS tx_added_count
FROM events_filtered e
INNER JOIN gp_delay_modules m
    ON m.delay_module_address = e.delay_module_address
GROUP BY e.date, m.gp_safe
