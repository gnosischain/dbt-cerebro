{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by=['contract_address'],
        unique_key='contract_address'
    )
}}

-- Storage for contract ABIs
SELECT 
    '' AS contract_address,
    '' AS abi_json,
    '' AS source,
    toDateTime('1970-01-01 00:00:00') AS updated_at
WHERE 0=1