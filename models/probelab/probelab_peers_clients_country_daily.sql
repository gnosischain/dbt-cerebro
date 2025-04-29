{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client, country)',
        unique_key='(date, client, country)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    )
}}


WITH


probelab_agent_country AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,country
        ,toInt32(floor(__count)) AS value
    FROM 
        {{ source('crawlers_data','probelab_countries_avg_1d') }} 
    {{ apply_monthly_incremental_filter('max_crawl_created_at','date') }}
)

SELECT
    *
FROM probelab_agent_country 
WHERE date < today()


        
