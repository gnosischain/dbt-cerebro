{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client, quic)',
        unique_key='(date, client, quic)',
        partition_by='toStartOfMonth(date)',
        settings={
            'allow_nullable_key': 1
        }
    )
}}


WITH


probelab_agent_quic AS (
    SELECT 
        toStartOfDay(max_crawl_created_at) AS date
        ,agent_version_type AS client
        ,quic_support AS quic
        ,__count AS value
    FROM 
        {{ source('crawlers_data','probelab_quic_support_over_7d') }} 
    {{ apply_monthly_incremental_filter('max_crawl_created_at', 'date') }}
)

SELECT
    *
FROM probelab_agent_quic 
WHERE date < today()


        
