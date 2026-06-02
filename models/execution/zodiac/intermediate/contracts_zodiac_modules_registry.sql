{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','zodiac','registry']
  )
}}

WITH proxies AS (
    SELECT
        proxy_address,
        master_copy,
        block_timestamp
    FROM {{ ref('int_execution_zodiac_module_proxies') }}
    WHERE master_copy IN (
        '0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5',
        '0xd54895b1121a2ee3f37b502f507631fa1331bed6',
        '0xd62129bf40cd1694b3d9d9847367783a1a4d5cb4',
        '0x9646fdad06d3e24444381f44362a3b0eb343d337'
    )
)

SELECT
    proxy_address                                          AS address,
    multiIf(
        master_copy = '0x9646fdad06d3e24444381f44362a3b0eb343d337', 'RolesModule',
        master_copy IN (
            '0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5',
            '0xd54895b1121a2ee3f37b502f507631fa1331bed6',
            '0xd62129bf40cd1694b3d9d9847367783a1a4d5cb4'
        ),                                                   'DelayModule',
        'Unknown'
    )                                                      AS contract_type,
    master_copy                                            AS abi_source_address,
    toUInt8(1)                                             AS is_dynamic,
    block_timestamp                                        AS start_blocktime,
    'zodiac_module_proxy_factory'                          AS discovery_source
FROM proxies
