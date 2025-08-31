{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, inclusion_delay)',
        unique_key='(date, inclusion_delay)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "attestations"]
    )
}}



SELECT
    toStartOfDay(slot_timestamp) AS date
    ,slot - attestation_slot AS inclusion_delay
    ,COUNT(*) AS cnt
FROM {{ ref('stg_consensus__attestations') }}
WHERE
    slot_timestamp < today()
    {{ apply_monthly_incremental_filter('slot_timestamp', 'date', 'true') }}
GROUP BY 1, 2
