



SELECT
    safe_address                      AS address,
    'SafeProxy'                       AS contract_type,
    creation_singleton                AS abi_source_address,
    toUInt8(1)                        AS is_dynamic,
    block_timestamp                   AS start_blocktime,
    'traces:safe_setup_delegatecall'  AS discovery_source
FROM `dbt`.`int_execution_safes`