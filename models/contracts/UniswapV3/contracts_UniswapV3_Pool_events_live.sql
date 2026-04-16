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
    at query time and always reflects current source state. The `decode_logs`
    macro filters by contract address so the scan stays bounded by the
    48h-TTL source.
#}

{{
    decode_logs(
        source_table         = source('execution_live', 'logs'),
        contract_address_ref = ref('contracts_whitelist'),
        contract_type_filter = 'UniswapV3Pool',
        output_json_type     = true,
        incremental_column   = none
    )
}}
