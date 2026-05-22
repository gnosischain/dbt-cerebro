{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_number, transaction_index, log_index)',
        ttl='block_timestamp + INTERVAL 2 HOUR',
        settings={'allow_nullable_key': 1},
        tags=['live', 'contracts', 'balancerv3', 'events'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{%- set src = source('execution_live', 'logs') -%}
{%- if is_incremental() -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT addMinutes(max(block_timestamp), -5) FROM " ~ this ~ "))" -%}
{%- else -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT max(block_timestamp) FROM " ~ src ~ ") - INTERVAL 30 MINUTE)" -%}
{%- endif -%}

{{
    decode_logs(
        source_table       = filtered_src,
        contract_address   = '0xba1333333333a1ba1108e8412f11850a5c319ba9',
        output_json_type   = true,
        incremental_column = none
    )
}}
