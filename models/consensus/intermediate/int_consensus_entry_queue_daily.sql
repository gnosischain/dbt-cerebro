{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date)',
        unique_key='(date)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "entry_queue"]
    )
}}

WITH

queue_activation AS (
    SELECT
        validator_index
        ,date
        ,epoch_eligibility
        ,epoch_activation
        ,(epoch_activation - epoch_eligibility) * 16 * 5 /(60 * 60 * 24) AS activation_days
    FROM (
        SELECT 
            validator_index
            ,toStartOfDay(argMin(slot_timestamp,slot)) AS date
            ,argMin(activation_eligibility_epoch,slot) AS epoch_eligibility
            ,argMin(activation_epoch,slot) AS epoch_activation
        FROM {{ ref('stg_consensus__validators') }}
        WHERE 
            activation_epoch < 18446744073709551615
            {{ apply_monthly_incremental_filter(source_field='slot_timestamp',destination_field='date',add_and='true') }}
        GROUP BY 1
    )
)

SELECT
    date
    ,validator_count
    ,q_activation[1] AS q05
    ,q_activation[2] AS q10
    ,q_activation[3] AS q25
    ,q_activation[4] AS q50
    ,q_activation[5] AS q75
    ,q_activation[6] AS q90
    ,q_activation[7] AS q95
    ,mean
FROM (
    SELECT
        date,
        count() AS validator_count
        ,quantilesTDigest(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(activation_days) AS q_activation
        ,avg(activation_days) AS  mean
    FROM queue_activation
    GROUP BY date
)
