{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(date, client, version)',
        unique_key='(date, client, version)',
        partition_by='toStartOfMonth(date)',
        tags=['production','execution','blocks']
    ) 
}}

WITH

clients_version AS (
    SELECT
        toStartOfDay(block_timestamp) AS date
        ,multiIf(
             lower(decoded_extra_data[1]) = 'choose' 
            OR lower(decoded_extra_data[1]) = 'mysticryuujin'  
            OR lower(decoded_extra_data[1]) = 'sanae.io'
            OR decoded_extra_data[1] = ''  , 
            'Unknown',
            decoded_extra_data[1]
        )   AS client
        ,IF(length(decoded_extra_data)>1, 
            IF(decoded_extra_data[2]='Ethereum',decoded_extra_data[3],decoded_extra_data[2]), 
            ''
        ) AS version
        ,COUNT(*) AS cnt
    FROM {{ ref('stg_execution__blocks') }}
    {{ apply_monthly_incremental_filter('block_timestamp', 'date') }}
    GROUP BY 1, 2, 3
)

SELECT
    *
FROM clients_version



