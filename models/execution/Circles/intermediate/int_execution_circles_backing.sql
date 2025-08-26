{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(date)',
        unique_key              = '(date)',
        partition_by            = 'toStartOfMonth(date)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
    )
}}


SELECT
    toStartOfDay(block_timestamp) AS date
    ,COUNT(*) AS cnt
FROM {{ ref('contracts_circles_v2_CirclesBackingFactory_events') }}
WHERE 
    event_name = 'CirclesBackingCompleted'
    {{ apply_monthly_incremental_filter(source_field='block_timestamp',destination_field='date',add_and=true) }}
GROUP BY 1