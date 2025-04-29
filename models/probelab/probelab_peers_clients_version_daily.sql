{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client, version)',
        unique_key='(date, client, version)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    )
}}


WITH


probelab_agent_version AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,agent_version_semver_str AS version
        ,toInt32(floor(__count)) AS value
    FROM 
        {{ source('crawlers_data','probelab_agent_semvers_avg_1d') }} 
    {{ apply_monthly_incremental_filter('max_crawl_created_at', 'date') }}
)

SELECT
    *
FROM probelab_agent_version 
WHERE date < today()


        
