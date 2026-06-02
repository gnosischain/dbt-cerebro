{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_number, transaction_index, log_index)',
        ttl='block_timestamp + INTERVAL 2 HOUR',
        settings={'allow_nullable_key': 1},
        tags=['live', 'contracts', 'balancerv2', 'events', 'microbatch'],
        pre_hook=["SET allow_experimental_json_type = 1"],
        post_hook=["SET allow_experimental_json_type = 0"]
    )
}}
{%- set src = source('execution_live', 'logs') -%}
{%- if is_incremental() -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT if(max(block_timestamp) > toDateTime(0), addMinutes(max(block_timestamp), -5), now() - INTERVAL 30 MINUTE) FROM " ~ this ~ "))" -%}
{%- else -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT max(block_timestamp) FROM " ~ src ~ ") - INTERVAL 30 MINUTE)" -%}
{%- endif -%}

{{
    decode_logs(
        source_table       = filtered_src,
        contract_address   = '0xBA12222222228d8Ba445958a75a0704d566BF2C8',
        output_json_type   = true,
        incremental_column = none
    )
}}
