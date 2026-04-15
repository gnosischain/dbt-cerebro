{{
    config(
        materialized='view',
        pre_hook=[
            "SET allow_experimental_json_type = 1"
        ],
        tags=['live', 'contracts', 'swapr', 'events']
    )
}}

{#
    Plain view (NOT a materialized_view). See contracts_UniswapV3_Pool_events_live
    for rationale: the cryo-live indexer bulk-attaches parts which skip the
    MV insert trigger, causing silent data gaps.
#}

{{
    decode_logs(
        source_table         = source('execution_live', 'logs'),
        contract_address_ref = ref('contracts_whitelist'),
        contract_type_filter = 'SwaprPool',
        output_json_type     = true,
        incremental_column   = none
    )
}}
