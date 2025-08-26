{{ 
    config(
        materialized            = 'incremental',
        incremental_strategy    = 'delete+insert',
        engine                  = 'ReplacingMergeTree()',
        order_by                = '(date, avatar_type)',
        unique_key              = '(date, avatar_type)',
        partition_by            = 'toStartOfMonth(date)',
        settings                = { 
                                    'allow_nullable_key': 1 
                                },
    )
}}



SELECT
    toStartOfDay(block_timestamp) AS date
    ,CASE  
        WHEN event_name = 'RegisterHuman' THEN 'Human' 
        WHEN event_name = 'RegisterGroup' THEN 'Group' 
        WHEN event_name = 'RegisterOrganization' THEN 'Org'
        ELSE 'Unknown' 
    END AS avatar_type
    ,COUNT(*) AS cnt
FROM {{ ref('contracts_circles_v2_Hub_events') }}
WHERE 
    event_name IN ('RegisterHuman','RegisterGroup','RegisterOrganization')
    {{ apply_monthly_incremental_filter(source_field='block_timestamp',destination_field='date',add_and=true) }}
GROUP BY 1,2
