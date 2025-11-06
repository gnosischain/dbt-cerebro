{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date,withdrawal_credentials)',
        unique_key='(date,withdrawal_credentials)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "credentials"]
    ) 
}}


SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,withdrawal_credentials
    ,COUNT(*) AS cnt
FROM {{ ref('stg_consensus__validators') }}
WHERE
    slot_timestamp < today()
    AND status LIKE 'active_%'
{{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and=true) }}
GROUP BY 1, 2
