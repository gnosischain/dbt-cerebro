{{
    config(
        materialized='view',
        pre_hook=[
            "SET allow_experimental_json_type = 1"
        ],
        tags=['dev', 'live', 'contracts', 'swapr', 'events']
    )
}}

{%- set src = source('execution_live', 'logs') -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT max(block_timestamp) FROM " ~ src ~ ") - INTERVAL 4 HOUR)" -%}

{{
    decode_logs(
        source_table         = filtered_src,
        contract_address_ref = ref('contracts_whitelist'),
        contract_type_filter = 'SwaprPool',
        output_json_type     = true,
        incremental_column   = none
    )
}}
