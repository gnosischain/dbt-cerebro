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

{% set blocks_pre_filter %}
    block_timestamp > '1970-01-01'
    {{ apply_monthly_incremental_filter('block_timestamp', 'date', add_and=True) }}
{% endset %}

WITH

deduped_blocks AS (
    SELECT
        block_timestamp,
        {{ decode_hex_tokens('extra_data') }} AS decoded_extra_data
    FROM (
        {{ dedup_source(
            source_ref=source('execution', 'blocks'),
            partition_by='block_number',
            columns='block_timestamp, extra_data',
            pre_filter=blocks_pre_filter
        ) }}
    )
),

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
    FROM deduped_blocks
    GROUP BY 1, 2, 3
)

SELECT
    *
FROM clients_version



