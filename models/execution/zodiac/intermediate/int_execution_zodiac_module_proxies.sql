{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(proxy_address)',
    partition_by='toStartOfMonth(block_timestamp)',
    unique_key='(proxy_address)',
    settings={ 'allow_nullable_key': 1 },
    pre_hook=["SET allow_experimental_json_type = 1"],
    tags=['production','execution','zodiac']
  )
}}

{# Description in schema.yml — see int_execution_zodiac_module_proxies #}

WITH decoded AS (
    SELECT * FROM (
        {{ decode_logs(
            source_table       = source('execution','logs'),
            contract_address   = '0x000000000000addb49795b0f9ba5bc298cdda236',
            output_json_type   = true,
            incremental_column = 'block_timestamp',
            start_blocktime    = '2021-01-01'
        ) }}
    )
)

SELECT
    lower(decoded_params['proxy'])      AS proxy_address,
    lower(decoded_params['masterCopy']) AS master_copy,
    block_timestamp,
    block_number,
    concat('0x', transaction_hash)      AS tx_hash,
    log_index
FROM decoded
WHERE event_name = 'ModuleProxyCreation'
