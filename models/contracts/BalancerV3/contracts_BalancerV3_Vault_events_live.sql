{{
    config(
        materialized='view',
        pre_hook=[
            "SET allow_experimental_json_type = 1"
        ],
        tags=['dev', 'live', 'contracts', 'balancerv3', 'events']
    )
}}

{%- set src = source('execution_live', 'logs') -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT max(block_timestamp) FROM " ~ src ~ ") - INTERVAL 4 HOUR)" -%}

{{
    decode_logs(
        source_table       = filtered_src,
        contract_address   = '0xba1333333333a1ba1108e8412f11850a5c319ba9',
        output_json_type   = true,
        incremental_column = none
    )
}}
