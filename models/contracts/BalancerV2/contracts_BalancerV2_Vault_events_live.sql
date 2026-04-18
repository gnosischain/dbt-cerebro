{{
    config(
        materialized='view',
        tags=['dev', 'live', 'contracts', 'balancerv2', 'events'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{%- set src = source('execution_live', 'logs') -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT max(block_timestamp) FROM " ~ src ~ ") - INTERVAL 4 HOUR)" -%}

{{
    decode_logs(
        source_table       = filtered_src,
        contract_address   = '0xBA12222222228d8Ba445958a75a0704d566BF2C8',
        output_json_type   = true,
        incremental_column = none
    )
}}
