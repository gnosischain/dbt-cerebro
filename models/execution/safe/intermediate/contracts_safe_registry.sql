{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','safe','registry']
  )
}}

SELECT
    safe_address                      AS address,
    'SafeProxy'                       AS contract_type,
    creation_singleton                AS abi_source_address,
    toUInt8(1)                        AS is_dynamic,
    block_timestamp                   AS start_blocktime,
    'traces:safe_setup_delegatecall'  AS discovery_source
FROM {{ ref('int_execution_safes') }}
