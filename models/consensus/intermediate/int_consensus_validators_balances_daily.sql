{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_balances"]
    ) 
}}


SELECT
    toStartOfDay(slot_timestamp) AS date
    ,SUM(balance/POWER(10,9)) AS balance
    ,SUM(effective_balance/POWER(10,9)) AS effective_balance
FROM {{ ref('stg_consensus__validators') }}
WHERE 
    slot_timestamp < today()
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
GROUP BY date