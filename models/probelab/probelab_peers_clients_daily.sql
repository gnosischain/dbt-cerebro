{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client)',
        unique_key='(date, client)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    )
}}


WITH


probelab_agent AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,any_value(toInt32(floor(__total))) AS value
    FROM 
        {{ source('crawlers_data','probelab_agent_semvers_avg_1d') }} 
    {{ apply_monthly_incremental_filter('max_crawl_created_at', 'date') }}
    GROUP BY
        1, 2
)

SELECT
    *
FROM probelab_agent 
WHERE date < today()


        
