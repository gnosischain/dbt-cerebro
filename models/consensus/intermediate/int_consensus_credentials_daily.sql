{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date,credentials_type)',
        unique_key='(date,credentials_type)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "credentials"]
    ) 
}}




SELECT 
    toStartOfDay(slot_timestamp) AS date
    ,leftUTF8(withdrawal_credentials, 4) AS credentials_type
    ,COUNT(*) AS cnt
FROM {{ ref('stg_consensus__validators') }}
{{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and=false) }}
GROUP BY 1, 2
