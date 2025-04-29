{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client, cloud)',
        unique_key='(date, client, cloud)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    )
}}


WITH


probelab_agent_cloud AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,cloud_provider AS cloud
        ,toInt32(floor(__count)) AS value
    FROM 
        {{ source('crawlers_data','probelab_cloud_provider_avg_1d') }} 
    {{ apply_monthly_incremental_filter('max_crawl_created_at','date') }}
)

SELECT
    *
FROM probelab_agent_cloud 
WHERE date < today()


        
