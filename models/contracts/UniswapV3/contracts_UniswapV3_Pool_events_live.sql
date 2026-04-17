{{
    config(
        materialized='view',
        pre_hook=[
            "SET allow_experimental_json_type = 1"
        ],
        tags=['dev', 'live', 'contracts', 'uniswapv3', 'events']
    )
}}

{#
    Plain view (NOT a materialized_view). The cryo-live indexer populates
    `execution_live.logs` via bulk part attaches that do not fire MV triggers,
    so an MV here silently misses large chunks of data. A view re-evaluates
    at query time and always reflects current source state.

    The source is pre-filtered to the last 4h (relative to the source HWM,
    not wall-clock) so the decode_logs scan covers ~4h instead of the full
    48h TTL. This filter lands INSIDE the macro's FROM clause — before the
    dedup window function and ABI join — which is the only position where
    ClickHouse can use it for partition/granule skipping.
#}

{%- set src = source('execution_live', 'logs') -%}
{%- set filtered_src = "(SELECT *, insert_version FROM " ~ src ~ " WHERE block_timestamp >= (SELECT max(block_timestamp) FROM " ~ src ~ ") - INTERVAL 4 HOUR)" -%}

{{
    decode_logs(
        source_table         = filtered_src,
        contract_address_ref = ref('contracts_whitelist'),
        contract_type_filter = 'UniswapV3Pool',
        output_json_type     = true,
        incremental_column   = none
    )
}}
